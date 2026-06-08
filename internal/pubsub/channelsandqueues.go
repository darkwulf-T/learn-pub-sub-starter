package pubsub

import (
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	Durable   SimpleQueueType = "durable"
	Transient SimpleQueueType = "transient"
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	newChannel, err := conn.Channel()
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}

	kind := "direct"
	if exchange == routing.ExchangePerilTopic {
		kind = "topic"
	}

	err = newChannel.ExchangeDeclare(exchange, kind, true, false, false, false, nil)
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}

	queue, err := newChannel.QueueDeclare(queueName, queueType == Durable, queueType == Transient, queueType == Transient, false, amqp.Table{routing.Delete_Key: routing.ExchangePerilDelete})
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}
	err = newChannel.QueueBind(queue.Name, key, exchange, false, nil)
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}
	return newChannel, queue, nil
}
