package main

import (
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func publishGameLog(gs gamelogic.GameState, ch *amqp.Channel, msg string) error {
	username := gs.Player.Username
	routingKey := fmt.Sprintf("%s.%s", routing.GameLogSlug, username)

	log := routing.GameLog{
		CurrentTime: time.Now(),
		Message:     msg,
		Username:    username,
	}

	err := pubsub.PublishGob(ch, routing.ExchangePerilTopic, routingKey, log)
	if err != nil {
		return err
	}
	return nil
}
