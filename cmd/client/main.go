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

	newChannel, err := con.Channel()
	if err != nil {
		log.Fatalf("error opening channel: %v", err)
	}
	defer newChannel.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
	}
	queueNamePause := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	queueNameMove := fmt.Sprintf("army_moves.%s", username)

	gameState := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(con, routing.ExchangePerilDirect, queueNamePause, routing.PauseKey, pubsub.Transient, handlerPause(gameState))
	if err != nil {
		log.Fatalf("error setting up subscriber: %v", err)
	}

	err = pubsub.SubscribeJSON(con, routing.ExchangePerilTopic, queueNameMove, "army_moves.*", pubsub.Transient, handlerMove(gameState, newChannel))
	if err != nil {
		log.Fatalf("error setting up subscriber: %v", err)
	}

	err = pubsub.SubscribeJSON(con, routing.ExchangePerilTopic, "war", fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix), pubsub.Durable, handlerWar(gameState))
	if err != nil {
		log.Fatalf("error setting up subscriber: %v", err)
	}

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "spawn":
			err = gameState.CommandSpawn(input)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "move":
			move, err := gameState.CommandMove(input)
			if err != nil {
				fmt.Println(err)
				continue
			}
			err = pubsub.PublishJSON(newChannel, routing.ExchangePerilTopic, queueNameMove, move)
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Printf("Player %v successfully moved unit(s) to location %v\n", move.Player.Username, move.ToLocation)
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Printf("Error: invalid command\n")
		}
	}
}
