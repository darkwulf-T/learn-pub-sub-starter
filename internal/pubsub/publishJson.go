package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonData, err := json.Marshal(val)
	if err != nil {
		return err
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        jsonData,
	})
	if err != nil {
		return err
	}
	return nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T),
) error {
	aChannel, aQueue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	dChan, err := aChannel.Consume(aQueue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for message := range dChan {
			var mess T
			err = json.Unmarshal(message.Body, &mess)
			if err != nil {
				fmt.Println(err)
				continue
			}
			handler(mess)
			err = message.Ack(false)
			if err != nil {
				fmt.Println(err)
				continue
			}
		}
	}()

	return nil
}
