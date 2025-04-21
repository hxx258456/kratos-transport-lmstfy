package lmstfy

import (
	"context"
	"github.com/tx7do/kratos-transport/broker"
)

type namespaceKey struct{}
type tokenKey struct{}
type delayKey struct{}
type ttlKey struct{}
type triesKey struct{}

func WithNamespace(namespace string) broker.Option {
	return broker.OptionContextWithValue(namespaceKey{}, namespace)
}

func WithToken(token string) broker.Option {
	return broker.OptionContextWithValue(tokenKey{}, token)
}

func WithDelay(delay uint32) broker.PublishOption {
	return broker.PublishContextWithValue(delayKey{}, delay)
}

func WithTTL(ttl uint32) broker.PublishOption {
	return broker.PublishContextWithValue(ttlKey{}, ttl)
}

func WithTries(tries uint16) broker.PublishOption {
	return broker.PublishContextWithValue(triesKey{}, tries)
}

func getNamespace(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if val, ok := ctx.Value(namespaceKey{}).(string); ok {
		return val
	}
	return ""
}

func getToken(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if val, ok := ctx.Value(tokenKey{}).(string); ok {
		return val
	}
	return ""
}

func getDelay(ctx context.Context) uint32 {
	if ctx == nil {
		return 0
	}
	if val, ok := ctx.Value(delayKey{}).(uint32); ok {
		return val
	}
	return 0
}

func getTTL(ctx context.Context) uint32 {
	if ctx == nil {
		return 0
	}
	if val, ok := ctx.Value(ttlKey{}).(uint32); ok {
		return val
	}
	return 0
}

func getTries(ctx context.Context) uint16 {
	if ctx == nil {
		return 0
	}
	if val, ok := ctx.Value(triesKey{}).(uint16); ok {
		return val
	}
	return 0
}

/*
*
subscriber options context
*/
type ttrKey struct{}
type timeoutKey struct{}

func WithTTR(ttr uint32) broker.SubscribeOption {
	return broker.SubscribeContextWithValue(ttrKey{}, ttr)
}

func WithTimeout(timeout uint32) broker.SubscribeOption {
	return broker.SubscribeContextWithValue(timeoutKey{}, timeout)
}

func getTTR(ctx context.Context) uint32 {
	if ctx == nil {
		return 0
	}
	if val, ok := ctx.Value(ttrKey{}).(uint32); ok {
		return val
	}
	return 0
}

func getTimeout(ctx context.Context) uint32 {
	if ctx == nil {
		return 0
	}
	if val, ok := ctx.Value(timeoutKey{}).(uint32); ok {
		return val
	}
	return 0
}
