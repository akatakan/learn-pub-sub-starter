package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishGOB[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var network bytes.Buffer
	encoder := gob.NewEncoder(&network)
	err := encoder.Encode(val)
	if err != nil {
		return err
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body:        network.Bytes(),
	})
	if err != nil {
		return err
	}
	return nil
}
