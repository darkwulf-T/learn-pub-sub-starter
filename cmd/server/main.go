package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

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
	err = pubsub.PublishJSON(serverChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
	if err != nil {
		log.Fatalf("error publishing the message: %v", err)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("\nshutting down connection")
}
