package messaging

import (
	"log"

	"github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	Conn *amqp091.Connection
	Ch   *amqp091.Channel
}

func NewRabbitMQ(url string) (*RabbitMQ, error) {
	conn, err := amqp091.Dial(url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &RabbitMQ{conn, ch}, nil
}

func (r *RabbitMQ) Close() {
	if err := r.Ch.Close(); err != nil {
		log.Printf("couldn't close channel: %s", err)
	}

	if err := r.Conn.Close(); err != nil {
		log.Printf("couldn't close connection: %s", err)
	}
}
