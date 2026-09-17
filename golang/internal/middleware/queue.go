package middleware

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	conn        *amqp.Connection
	ch          *amqp.Channel
	queueName   string
	consumerTag string
}

func NewQueueMiddleware(conn *amqp.Connection, ch *amqp.Channel, queueName string) *QueueMiddleware {
	return &QueueMiddleware{
		conn:      conn,
		ch:        ch,
		queueName: queueName,
	}
}

func (e *QueueMiddleware) Close() error {
	if e.ch != nil {
		err := e.ch.Close()
		if err != nil && err != amqp.ErrClosed {
			return ErrMessageMiddlewareClose
		}
	}

	if e.conn != nil {
		err := e.conn.Close()
		if err != nil && err != amqp.ErrClosed {
			return ErrMessageMiddlewareClose
		}
	}

	return nil
}

func (e *QueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if e.ch == nil {
		e.Close()
		return ErrMessageMiddlewareDisconnected
	}

	e.consumerTag = fmt.Sprintf("consumer-%s-%d", e.queueName, time.Now().UnixNano())

	msgs, err := e.ch.Consume(
		e.queueName,   // queue
		e.consumerTag, // consumer
		false,         // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	if err != nil {
		e.Close()
		return ErrMessageMiddlewareMessage
	}

	go func() {
		for d := range msgs {
			callbackFunc(Message{Body: string(d.Body)}, func() { d.Ack(false) }, func() { d.Nack(false, true) })
		}
	}()
	return nil
}

func (e *QueueMiddleware) StopConsuming() error {
	if e.ch == nil {
		e.Close()
		return ErrMessageMiddlewareDisconnected
	}

	if e.consumerTag != "" {
		return e.ch.Cancel(e.consumerTag, false)
	}
	return nil
}

func (e *QueueMiddleware) Send(msg Message) error {
	if e.ch == nil {
		e.Close()
		return ErrMessageMiddlewareDisconnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := e.ch.PublishWithContext(ctx,
		"",          // exchange
		e.queueName, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
	if err != nil {
		e.Close()
		return ErrMessageMiddlewareMessage
	}
	return nil
}
