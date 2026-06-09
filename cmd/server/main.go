package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conectionString := "amqp://guest:guest@localhost:5672/"
	con, err := amqp.Dial(conectionString)
	if err != nil {
		log.Fatalf("error creating a connection: %v", err)
	}
	defer con.Close()
	fmt.Println("successful connected to service")

	serverChannel, err := con.Channel()
	if err != nil {
		log.Fatalf("error creating server channel: %v", err)
	}
	defer serverChannel.Close()

	err = pubsub.SubscribeGob(con, routing.ExchangePerilTopic, routing.GameLogSlug, fmt.Sprintf("%s.*", routing.GameLogSlug), pubsub.Durable, handlerLogs())
	if err != nil {
		log.Fatalf("error declaring or binding the queue: %v", err)
	}

	gamelogic.PrintServerHelp()

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "pause":
			fmt.Println("sending pause message")
			err = pubsub.PublishJSON(serverChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				log.Fatalf("error publishing the message: %v", err)
			}
		case "resume":
			fmt.Println("sending resume message")
			err = pubsub.PublishJSON(serverChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				log.Fatalf("error publishing the message: %v", err)
			}
		case "quit":
			fmt.Println("exiting...")
			return
		default:
			fmt.Println("Unknown command. Please try a valid one.")
		}

	}
}
