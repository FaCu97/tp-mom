package middleware

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrMessageMiddlewareMessage      = errors.New("message middleware: message error")
	ErrMessageMiddlewareDisconnected = errors.New("message middleware: disconnected")
	ErrMessageMiddlewareClose        = errors.New("message middleware: close error")
)

type Message struct {
	Body string
}

type ConnSettings struct {
	Hostname string
	Port     int
}

type Middleware interface {

	//Comienza a escuchar a la cola/exchange e invoca a callbackFunc tras
	//cada mensaje de datos o de control con el cuerpo del mensaje.
	//callbackFunc tiene como parámetro:
	// msg - El struct tal y como lo recibe el método Send.
	// ack - Una función que hace ACK del mensaje recibido.
	// nack - Una función que hace NACK del mensaje recibido.
	//Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
	//Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareMessage.
	StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error

	//Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
	//no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
	//Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
	StopConsuming() error

	//Envía un mensaje a la cola o a los tópicos con el que se inicializó el exchange.
	//Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
	//Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareMessage.
	Send(msg Message) error

	//Se desconecta de la cola o exchange al que estaba conectado.
	//Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareClose.
	Close() error
}

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
