package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	SimpleQueueTypeTransient int = iota
	SimpleQueueTypeDurable
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	dat, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("could not marshal val to JSON: %v", err)
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        dat,
	})

	if err != nil {
		return fmt.Errorf("error while publishing with context: %v", err)
	}

	return nil
}

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, simpleQueueType int, handler func(T) AckType) error {
	unmarshaller := func(data []byte) (T, error) {
		var target T
		err := json.Unmarshal(data, &target)
		return target, err
	}

	err := subscribe(conn, exchange, queueName, key, simpleQueueType, handler, unmarshaller)
	if err != nil {
		return fmt.Errorf("error on subscribe: %v", err)
	}

	return nil
}

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, simpleQueueType int) (*amqp.Channel, amqp.Queue, error) {
	rabbitChannel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("error creating RabbitMQ channel: %v", err)
	}

	isDurable := simpleQueueType == SimpleQueueTypeDurable
	autoDelete := simpleQueueType == SimpleQueueTypeTransient
	exclusive := simpleQueueType == SimpleQueueTypeTransient

	queue, err := rabbitChannel.QueueDeclare(queueName, isDurable, autoDelete, exclusive, false, amqp.Table{"x-dead-letter-exchange": "peril_dlx"})
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("error creating RabbitMQ queue: %v", err)
	}

	err = rabbitChannel.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("error binding channel to queue: %v", err)
	}

	return rabbitChannel, queue, nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(val)
	if err != nil {
		return fmt.Errorf("could not marshal val to gob: %v", err)
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body:        buf.Bytes(),
	})

	if err != nil {
		return fmt.Errorf("error while publishing with context: %v", err)
	}

	return nil
}

func SubscribeGob[T any](conn *amqp.Connection, exchange, queueName, key string, simpleQueueType int, handler func(T) AckType) error {
	unmarshaller := func(data []byte) (T, error) {
		var result T
		buf := bytes.NewBuffer(data)
		dec := gob.NewDecoder(buf)
		err := dec.Decode(&result)
		return result, err
	}

	err := subscribe(conn, exchange, queueName, key, simpleQueueType, handler, unmarshaller)
	if err != nil {
		return fmt.Errorf("error on subscribe: %v", err)
	}

	return nil
}

func subscribe[T any](conn *amqp.Connection, exchange, queueName, key string, simpleQueueType int, handler func(T) AckType, unmarshaller func([]byte) (T, error)) error {
	channel, _, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return fmt.Errorf("error subscribing: %v", err)
	}

	err = channel.Qos(10, 0, true)
	if err != nil {
		return fmt.Errorf("error on QOS: %v", err)
	}
	deliveryChan, err := channel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("error on consume: %v", err)
	}

	go func() {
		defer channel.Close()
		for message := range deliveryChan {
			target, err := unmarshaller(message.Body)
			if err != nil {
				fmt.Printf("error during message unmarshal: %v\n", err)
				continue
			}
			log.Println("Running handler")
			ackResult := handler(target)
			switch ackResult {
			case Ack:
				message.Ack(false)
				log.Println("Ack")
			case NackRequeue:
				message.Nack(false, true)
				log.Println("Nack Requeue")
			case NackDiscard:
				message.Nack(false, false)
				log.Println("Nack Discard")
			default:
				fmt.Printf("invalid ack result from handler: %v\n", err)
				continue
			}
		}
	}()

	return nil
}
