package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

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

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("\nshutting down connection")
}
