package middleware

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	conn         *amqp.Connection
	ch           *amqp.Channel
	exchangeName string
	routingKeys  []string
	ConsumerTag  string
	queueName    string
}

func NewExchangeMiddleware(conn *amqp.Connection, ch *amqp.Channel, exchangeName string, keys []string) *ExchangeMiddleware {
	return &ExchangeMiddleware{
		conn:         conn,
		ch:           ch,
		exchangeName: exchangeName,
		routingKeys:  keys,
		ConsumerTag:  "",
		queueName:    "",
	}
}

func (e *ExchangeMiddleware) Close() error {
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

func (e *ExchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if e.ch == nil {
		e.Close()
		return ErrMessageMiddlewareDisconnected
	}

	// Declarar y bindear la cola solo la primera vez que se consume
	if e.queueName == "" {
		queue, err := e.ch.QueueDeclare(
			"",    // name
			false, // durability
			false, // delete when unused
			true,  // exclusive
			false, // no-wait
			nil,   // arguments
		)

		if err != nil {
			e.Close()
			return ErrMessageMiddlewareMessage
		}
		e.queueName = queue.Name

		for _, key := range e.routingKeys {
			err = e.ch.QueueBind(
				e.queueName,    // queue name
				key,            // routing key
				e.exchangeName, // exchange
				false,
				nil)
			if err != nil {
				e.Close()
				return ErrMessageMiddlewareMessage
			}
		}
	}

	e.ConsumerTag = fmt.Sprintf("consumer-%s-%d", e.queueName, time.Now().UnixNano())

	msgs, err := e.ch.Consume(
		e.queueName,   // queue
		e.ConsumerTag, // consumer
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

func (e *ExchangeMiddleware) StopConsuming() error {
	if e.ch == nil {
		e.Close()
		return ErrMessageMiddlewareDisconnected
	}

	if e.ConsumerTag != "" {
		return e.ch.Cancel(e.ConsumerTag, false)
	}
	return nil
}

func (e *ExchangeMiddleware) Send(msg Message) error {
	if e.ch == nil {
		e.Close()
		return ErrMessageMiddlewareDisconnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, key := range e.routingKeys {
		err := e.ch.PublishWithContext(ctx,
			e.exchangeName, // exchange
			key,            // routing key
			false,          // mandatory
			false,          // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg.Body),
			})
		if err != nil {
			e.Close()
			return ErrMessageMiddlewareMessage
		}
	}
	return nil
}
