package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

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

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
	}
	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)

	_, _, err = pubsub.DeclareAndBind(con, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.Transient)
	if err != nil {
		log.Fatalf("error declaring or binding the queue: %v", err)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("\nshutting down connection")
}
