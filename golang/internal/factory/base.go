package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type baseMiddleware struct {
	conn        *amqp.Connection
	ch          *amqp.Channel
	consumerTag string
}

func (b *baseMiddleware) Close() error {
	if b.ch != nil {
		err := b.ch.Close()
		if err != nil && err != amqp.ErrClosed {
			return m.ErrMessageMiddlewareClose
		}
		b.ch = nil
	}

	if b.conn != nil {
		err := b.conn.Close()
		if err != nil && err != amqp.ErrClosed {
			return m.ErrMessageMiddlewareClose
		}
		b.conn = nil
	}

	return nil
}

func (b *baseMiddleware) StopConsuming() error {
	if b.ch == nil {
		b.Close()
		return m.ErrMessageMiddlewareDisconnected
	}

	if b.consumerTag != "" {
		err := b.ch.Cancel(b.consumerTag, false)
		if err != nil {
			return m.ErrMessageMiddlewareDisconnected
		}
	}
	return nil
}
