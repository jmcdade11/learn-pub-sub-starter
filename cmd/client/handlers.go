package main

import (
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(s routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(s)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, publishCh *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(m gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		move := gs.HandleMove(m)
		switch move {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(publishCh, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix+"."+gs.GetUsername(), gamelogic.RecognitionOfWar{Attacker: m.Player, Defender: gs.GetPlayerSnap()})
			if err != nil {
				fmt.Println("Error publishing war message:", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		default:
			return pubsub.NackDiscard
		}
	}
}

func handlerWar(gs *gamelogic.GameState, publishCh *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(r gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		warLog := ""
		outcome, winner, loser := gs.HandleWar(r)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			warLog = fmt.Sprintf("%s won a war against %s", winner, loser)
			err := pubsub.PublishGob(publishCh, routing.ExchangePerilTopic, routing.GameLogSlug+"."+r.Attacker.Username, routing.GameLog{
				CurrentTime: time.Now().UTC(),
				Message:     warLog,
				Username:    gs.GetUsername(),
			})
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			warLog = fmt.Sprintf("%s won a war against %s", winner, loser)
			err := pubsub.PublishGob(publishCh, routing.ExchangePerilTopic, routing.GameLogSlug+"."+r.Attacker.Username, routing.GameLog{
				CurrentTime: time.Now().UTC(),
				Message:     warLog,
				Username:    gs.GetUsername(),
			})
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			warLog = fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			err := pubsub.PublishGob(publishCh, routing.ExchangePerilTopic, routing.GameLogSlug+"."+r.Attacker.Username, routing.GameLog{
				CurrentTime: time.Now().UTC(),
				Message:     warLog,
				Username:    gs.GetUsername(),
			})
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			fmt.Println("Error handling war outcome")
			return pubsub.NackDiscard
		}
	}
}
