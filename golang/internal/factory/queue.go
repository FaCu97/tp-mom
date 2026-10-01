package factory

import (
	"context"
	"fmt"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	baseMiddleware
	queueName string
}

func NewQueueMiddleware(conn *amqp.Connection, ch *amqp.Channel, queueName string) *QueueMiddleware {
	return &QueueMiddleware{
		baseMiddleware: baseMiddleware{
			conn: conn,
			ch:   ch,
		},
		queueName: queueName,
	}
}

func (e *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	if e.ch == nil {
		e.Close()
		return m.ErrMessageMiddlewareDisconnected
	}

	e.consumerTag = fmt.Sprintf("consumer-%s-%d", e.queueName, time.Now().UnixNano())

	err := e.ch.Qos(
		10,    // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		e.Close()
		return m.ErrMessageMiddlewareMessage
	}

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
		return m.ErrMessageMiddlewareMessage
	}

	go func() {
		for d := range msgs {
			callbackFunc(m.Message{Body: string(d.Body)}, func() { d.Ack(false) }, func() { d.Nack(false, true) })
		}
	}()
	return nil
}

func (e *QueueMiddleware) Send(msg m.Message) error {
	if e.ch == nil {
		e.Close()
		return m.ErrMessageMiddlewareDisconnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := e.ch.PublishWithContext(ctx,
		"",          // exchange
		e.queueName, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(msg.Body),
		})
	if err != nil {
		e.Close()
		return m.ErrMessageMiddlewareMessage
	}
	return nil
}
