package lmstfy

import (
	"github.com/hxx258456/kratos-transport-lmstfy/broker/lmstfy"
	"github.com/tx7do/kratos-transport/broker"
)

type ServerOption func(o *Server)

// WithBrokerOptions broker配置
func WithBrokerOptions(opts ...broker.Option) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, opts...)
	}
}

// WithAddress lmstfy服务地址
func WithAddress(addr string) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, broker.WithAddress(addr))
	}
}

// WithEnableKeepAlive enable keep alive
func WithEnableKeepAlive(enable bool) ServerOption {
	return func(s *Server) {
		s.enableKeepAlive = enable
	}
}

// WithCodec 编解码器
func WithCodec(c string) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, broker.WithCodec(c))
	}
}

func WithNamespace(ns string) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, lmstfy.WithNamespace(ns))
	}
}

func WithToken(token string) ServerOption {
	return func(s *Server) {
		s.brokerOpts = append(s.brokerOpts, lmstfy.WithToken(token))
	}
}
