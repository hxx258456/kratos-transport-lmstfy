package lmstfy

import (
	"github.com/bitleak/lmstfy/client"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-transport/broker"
	"time"
)

type subscriber struct {
	topic   string
	client  *client.LmstfyClient
	handler broker.Handler
	options broker.SubscribeOptions
	done    chan bool
	b       *lmstfyBroker
	binder  broker.Binder
	ttr     uint32
	timeout uint32
}

func (c *subscriber) Topic() string {
	return c.topic
}

func (c *subscriber) Options() broker.SubscribeOptions {
	return c.options
}

func (c *subscriber) Unsubscribe(removeFromManager bool) error {
	c.Stop()
	return nil
}

func (c *subscriber) Start() {
	for {
		select {
		case <-c.done:
			return
		default:
			job, err := c.client.Consume(c.topic, c.ttr, c.timeout)
			if err != nil {
				log.Error(err)
				time.Sleep(time.Second * 5)
				continue
			}
			if job == nil {
				continue
			}
			var msg broker.Message
			var p *publication
			if c.binder != nil {
				msg.Body = c.binder()
			} else {
				msg.Body = job.Data
			}

			if job == nil {
				continue
			}

			if err := broker.Unmarshal(c.b.options.Codec, job.Data, &msg.Body); err != nil {
				continue
			}

			p = &publication{
				client:    c.client,
				lmstfyMsg: job,
				msg:       &msg,
				topic:     c.topic,
				err:       nil,
			}
			err = c.handler(c.options.Context, p)
			if err == nil {
				if c.options.AutoAck {
					_ = c.client.Ack(c.topic, job.ID)
				}
			}
		}
	}
}

func (c *subscriber) Stop() {
	c.done <- true
}
