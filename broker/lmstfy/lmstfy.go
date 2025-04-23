package lmstfy

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/bitleak/lmstfy/client"
	"github.com/tx7do/kratos-transport/broker"
)

const (
	defaultAddr = "127.0.0.1:7777"
)

type lmstfyBroker struct {
	sync.RWMutex
	host      string
	port      int
	connected bool
	token     string
	namespace string
	options   broker.Options

	client *client.LmstfyClient

	subscribers *broker.SubscriberSyncMap
}

func (b *lmstfyBroker) Address() string {
	return fmt.Sprintf("%s:%d", b.host, b.port)
}

func (b *lmstfyBroker) Request(_ context.Context, _ string, _ broker.Any, _ ...broker.RequestOption) (broker.Any, error) {
	return nil, errors.New("lmstfy broker does not support request")
}

func NewBroker(opts ...broker.Option) broker.Broker {
	options := broker.NewOptionsAndApply(opts...)

	return &lmstfyBroker{
		options:     options,
		subscribers: broker.NewSubscriberSyncMap(),
	}
}

func (b *lmstfyBroker) Name() string {
	return "lmstfy"
}

func (b *lmstfyBroker) Init(opts ...broker.Option) error {
	for _, o := range opts {
		o(&b.options)
	}
	var host string
	var port int
	var err error
	if len(b.options.Addrs) == 0 || b.options.Addrs[0] == "" {
		host = strings.Split(defaultAddr, ":")[0]
		port, err = strconv.Atoi(strings.Split(defaultAddr, ":")[1])
		if err != nil {
			return err
		}
	} else {
		host = strings.Split(b.options.Addrs[0], ":")[0]
		port, err = strconv.Atoi(strings.Split(b.options.Addrs[0], ":")[1])
		if err != nil {
			return err
		}
	}

	b.token = getToken(b.options.Context)
	b.namespace = getNamespace(b.options.Context)
	b.host = host
	b.port = port
	return nil
}

func (b *lmstfyBroker) Options() broker.Options {
	return b.options
}

func (b *lmstfyBroker) Connect() error {
	b.RLock()
	if b.connected {
		b.RUnlock()
		return nil
	}
	b.RUnlock()
	cli := client.NewLmstfyClient(b.host, b.port, b.namespace, b.token)
	b.Lock()
	b.client = cli
	b.connected = true
	b.Unlock()

	return nil
}

func (b *lmstfyBroker) Disconnect() error {
	b.Lock()
	defer b.Unlock()

	b.subscribers.Clear()
	b.client = nil
	b.connected = false

	return nil
}

func (b *lmstfyBroker) Publish(ctx context.Context, topic string, msg broker.Any, opts ...broker.PublishOption) error {
	buf, err := broker.Marshal(b.options.Codec, msg)
	if err != nil {
		return err
	}
	if b.client == nil {
		return errors.New("lmstfy client is not connected")
	}
	options := broker.PublishOptions{Context: ctx}
	for _, o := range opts {
		o(&options)
	}

	delay := getDelay(options.Context)
	ttl := getTTL(options.Context)
	tries := getTries(options.Context)
	_, err = b.client.Publish(topic, buf, ttl, tries, delay)
	return err
}

func (b *lmstfyBroker) Subscribe(topic string, handler broker.Handler, binder broker.Binder, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	options := broker.NewSubscribeOptions()

	for _, o := range opts {
		o(&options)
	}
	ttr := getTTR(options.Context)
	timeout := getTimeout(options.Context)
	if ttr <= 0 {
		return nil, errors.New("ttr must be greater than 0")
	}

	if timeout <= 0 {
		return nil, errors.New("timeout must be greater than 0")
	}

	cons := &subscriber{
		topic:   topic,
		client:  b.client,
		handler: handler,
		b:       b,
		options: options,
		done:    make(chan bool),
		binder:  binder,
		ttr:     ttr,
		timeout: timeout,
	}
	b.Lock()
	b.subscribers.Add(topic, cons)
	b.Unlock()

	go cons.Start()

	return cons, nil
}
