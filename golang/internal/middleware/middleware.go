package middleware

import (
	"context"
	"errors"
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

func (q *QueueMiddleware) Close() error {
	if q.ch != nil {
		err := q.ch.Close()
		if err != nil && err != amqp.ErrClosed {
			return ErrMessageMiddlewareClose
		}
	}

	if q.conn != nil {
		err := q.conn.Close()
		if err != nil && err != amqp.ErrClosed {
			return ErrMessageMiddlewareClose
		}
	}

	return nil
}

func (q *QueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if q.ch == nil {
		q.Close()
		return ErrMessageMiddlewareDisconnected
	}

	q.consumerTag = "consumer-" + q.queueName

	msgs, err := q.ch.Consume(
		q.queueName,   // queue
		q.consumerTag, // consumer
		false,         // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	if err != nil {
		q.Close()
		return ErrMessageMiddlewareMessage
	}

	go func() {
		for d := range msgs {
			callbackFunc(Message{Body: string(d.Body)}, func() { d.Ack(false) }, func() { d.Nack(false, true) })
		}
	}()
	return nil
}

func (q *QueueMiddleware) StopConsuming() error {
	if q.ch == nil {
		q.Close()
		return ErrMessageMiddlewareDisconnected
	}

	if q.consumerTag != "" {
		return q.ch.Cancel(q.consumerTag, false)
	}
	return nil
}

func (q *QueueMiddleware) Send(msg Message) error {
	if q.ch == nil {
		q.Close()
		return ErrMessageMiddlewareDisconnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := q.ch.PublishWithContext(ctx,
		"",          // exchange
		q.queueName, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
	if err != nil {
		q.Close()
		return ErrMessageMiddlewareMessage
	}
	return nil

}
