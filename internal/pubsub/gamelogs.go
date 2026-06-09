package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var data bytes.Buffer
	encoder := gob.NewEncoder(&data)

	err := encoder.Encode(val)
	if err != nil {
		return err
	}
	serializedData := data.Bytes()

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body:        serializedData,
	})
	if err != nil {
		return err
	}
	return nil
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) Acktype,
) error {
	err := subscribe(conn, exchange, queueName, key, queueType, handler, gobUnmarshaller)
	return err
}
