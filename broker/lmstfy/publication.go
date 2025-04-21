package lmstfy

import (
	"github.com/bitleak/lmstfy/client"
	"github.com/tx7do/kratos-transport/broker"
)

var _ broker.Event = (*publication)(nil)

type publication struct {
	topic     string
	msg       *broker.Message
	lmstfyMsg *client.Job
	client    *client.LmstfyClient
	err       error
	options   broker.PublishOptions
}

func (p publication) Topic() string {
	return p.topic
}

func (p publication) Message() *broker.Message {
	return p.msg
}

func (p publication) RawMessage() interface{} {
	return p.lmstfyMsg
}

func (p publication) Ack() error {
	return p.client.Ack(p.topic, p.lmstfyMsg.ID)
}

func (p publication) Error() error {
	return p.err
}
