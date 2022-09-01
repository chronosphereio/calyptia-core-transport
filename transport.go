package core_transport

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/go-kit/log"
	nats "github.com/nats-io/nats.go"
)

type Subscription interface {
	Unsubscribe()
	Receive() chan []byte
}

type NatsSubscription struct {
	channel chan *nats.Msg
	conn    *nats.Subscription
	ctx     context.Context
}

func (ns *NatsSubscription) Unsubscribe() {
	ns.conn.Unsubscribe()
}

func (ns *NatsSubscription) Receive() chan []byte {
	ch := make(chan []byte)
	go func(ns *NatsSubscription, ch chan []byte) {
		for {
			select {
			case msg := <-ns.channel:
				ch <- msg.Data
			case <-ns.ctx.Done():
				return
			}
		}
	}(ns, ch)
	return ch
}

type Connection interface {
	Publish(channel string, data []byte) error
}

type NatsConnection struct {
	conn *nats.Conn
	ctx  context.Context
}

func (c *NatsConnection) Publish(channel string, data []byte) error {
	return c.conn.Publish(channel, data)
}

func Listen(ctx context.Context, logger log.Logger, uri *url.URL, clientID string) (Subscription, error) {
	hosturl := fmt.Sprintf("%s://%s:%s", uri.Scheme, uri.Hostname(), uri.Port())
	l := log.With(
		logger,
		"system", "transport",
		"nats", uri.String(),
	)

	path := uri.Path
	if path[0] == '/' {
		path = path[1:]
	}

	nc, err := nats.Connect(hosturl)
	if err != nil {
		_ = l.Log("nats", "connect", "error", err)
		return nil, err
	}

	js, err := nc.JetStream(nats.Context(ctx))
	if err != nil {
		_ = l.Log("nats", "jetstream", "error", err)
		return nil, err
	}

	_ = l.Log("nats", "addstream", "name", path, "level", "debug")
	_, err = js.AddStream(&nats.StreamConfig{
		Name:      path,
		Subjects:  []string{path},
		Retention: nats.LimitsPolicy,
		NoAck:     true,
		MaxAge:    time.Second * 10,
		Storage:   nats.MemoryStorage,
	}, nats.Context(ctx))
	if err != nil {
		_ = l.Log("nats", "addstream", "error", err)
		return nil, err
	}
	_ = l.Log("nats", "addstream", "name", path, "level", "debug")

	_, err = js.AddConsumer(path, &nats.ConsumerConfig{
		Durable:       uri.Fragment,
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckAllPolicy,
	}, nats.Context(ctx))
	if err != nil {
		_ = l.Log("nats", "addconsumer", "error", err)
		return nil, err
	}
	_ = l.Log("nats", "addconsumer", "name", uri.Fragment, "level", "debug")

	ch := make(chan *nats.Msg)
	sub, err := js.ChanSubscribe(path, ch, nats.Context(ctx))
	if err != nil {
		_ = l.Log(
			"nats", "subscribe",
			"uri", uri.Path,
			"error", err)
		return nil, err
	}
	return &NatsSubscription{
		ctx:     ctx,
		conn:    sub,
		channel: ch,
	}, nil
}

func Connect(ctx context.Context, logger log.Logger, uri *url.URL) (Connection, error) {
	hosturl := fmt.Sprintf("%s://%s:%s", uri.Scheme, uri.Hostname(), uri.Port())
	l := log.With(
		logger,
		"system", "transport",
		"nats", uri.String(),
	)

	nc, err := nats.Connect(hosturl)
	if err != nil {
		_ = l.Log("nats", "connect", "error", err)
		return nil, err
	}

	return &NatsConnection{
		conn: nc,
		ctx:  ctx,
	}, nil
}
