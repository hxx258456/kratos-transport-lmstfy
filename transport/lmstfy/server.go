package lmstfy

import (
	"context"
	"fmt"
	kratosTransport "github.com/go-kratos/kratos/v2/transport"
	"github.com/hxx258456/kratos-transport-lmstfy/broker/lmstfy"
	"github.com/tx7do/kratos-transport/broker"
	"github.com/tx7do/kratos-transport/keepalive"
	"github.com/tx7do/kratos-transport/transport"
	"net/url"
	"sync"
	"sync/atomic"
)

var (
	_ kratosTransport.Server     = (*Server)(nil)
	_ kratosTransport.Endpointer = (*Server)(nil)
)

type Server struct {
	broker.Broker
	brokerOpts []broker.Option

	subscribers    broker.SubscriberMap
	subscriberOpts transport.SubscribeOptionMap

	sync.RWMutex
	started atomic.Bool

	baseCtx context.Context
	err     error

	keepAlive       *keepalive.Service
	enableKeepAlive bool
}

func (s *Server) Start(ctx context.Context) error {
	if s.err != nil {
		return s.err
	}
	if s.started.Load() {
		return nil
	}
	s.err = s.Init(s.brokerOpts...)
	if s.err != nil {
		return s.err
	}
	s.err = s.Connect()
	if s.err != nil {
		return s.err
	}
	if s.enableKeepAlive {
		go func() {
			_ = s.keepAlive.Start()
		}()
	}

	LogInfof("lmstfy server started")
	s.err = s.doRegisterSubscriberMap()
	if s.err != nil {
		return s.err
	}
	s.baseCtx = ctx
	s.started.Store(true)
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	s.started.Store(false)
	return s.Disconnect()
}

func (s *Server) Endpoint() (*url.URL, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.keepAlive.Endpoint()
}

func NewServer(opts ...ServerOption) *Server {
	srv := &Server{
		baseCtx:         context.Background(),
		subscribers:     make(broker.SubscriberMap),
		subscriberOpts:  make(transport.SubscribeOptionMap),
		brokerOpts:      []broker.Option{},
		started:         atomic.Bool{},
		keepAlive:       keepalive.NewKeepAliveService(),
		enableKeepAlive: true,
	}

	srv.init(opts...)
	srv.Broker = lmstfy.NewBroker(srv.brokerOpts...)
	return srv
}

func (s *Server) init(opts ...ServerOption) {
	for _, o := range opts {
		o(s)
	}
}

func (s *Server) Name() string {
	return string(KindLmstfy)
}

func (s *Server) RegisterSubscriber(topic string, handler broker.Handler, binder broker.Binder, opts ...broker.SubscribeOption) error {
	s.Lock()
	defer s.Unlock()

	if s.started.Load() {
		return s.doRegisterSubscriber(topic, handler, binder, opts...)
	} else {
		s.subscriberOpts[topic] = &transport.SubscribeOption{Handler: handler, Binder: binder, SubscribeOptions: opts}
	}
	return nil
}

func RegisterSubscriber[T any](srv *Server, topic string, handler func(context.Context, string, broker.Headers, *T) error, opts ...broker.SubscribeOption) error {
	return srv.RegisterSubscriber(
		topic,
		func(ctx context.Context, event broker.Event) error {
			switch t := event.Message().Body.(type) {
			case *T:
				if err := handler(ctx, event.Topic(), event.Message().Headers, t); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unsupported type: %T", t)
			}
			return nil
		},
		func() broker.Any {
			var t T
			return &t
		},
		opts...,
	)
}

func (s *Server) doRegisterSubscriber(topic string, handler broker.Handler, binder broker.Binder, opts ...broker.SubscribeOption) error {
	sub, err := s.Subscribe(topic, handler, binder, opts...)
	if err != nil {
		return err
	}

	s.subscribers[topic] = sub

	return nil
}

func (s *Server) doRegisterSubscriberMap() error {
	for topic, opt := range s.subscriberOpts {
		_ = s.doRegisterSubscriber(topic, opt.Handler, opt.Binder, opt.SubscribeOptions...)
	}
	s.subscriberOpts = make(transport.SubscribeOptionMap)
	return nil
}
