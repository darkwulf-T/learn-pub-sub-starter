package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Acktype string

const (
	Ack         Acktype = "ack"
	NackRequeue Acktype = "NackRequeue"
	NackDiscard Acktype = "NackDiscard"
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
	handler func(T) Acktype,
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
			aType := handler(mess)
			switch aType {
			case Ack:
				err = message.Ack(false)
				fmt.Println("Ack")
				fmt.Print("> ")
			case NackRequeue:
				err = message.Nack(false, true)
				fmt.Println("Requeue")
				fmt.Print("> ")
			default:
				err = message.Nack(false, false)
				fmt.Println("Discard")
				fmt.Print("> ")
			}

			if err != nil {
				fmt.Println(err)
				continue
			}
		}
	}()

	return nil
}
