package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func jsonUnmarhsaller[T any](message []byte) (val T, err error) {
	var mess T
	err = json.Unmarshal(message, &mess)
	if err != nil {
		return val, err
	}
	return mess, nil
}

func gobUnmarshaller[T any](message []byte) (val T, err error) {
	buf := bytes.NewBuffer(message)
	decoder := gob.NewDecoder(buf)

	var mess T

	err = decoder.Decode(&mess)
	if err != nil {
		return val, err
	}
	return mess, nil
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
	unmarshaller func([]byte) (T, error),
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
			mess, err := unmarshaller(message.Body)
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
