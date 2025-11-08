package pubsub

import (
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	chann, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	newChan, err := chann.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	for msg := range newChan {
		var message T
		json.Unmarshal(msg.Body, &message)
		handler(message)
		go msg.Ack(false)
	}
	return nil
}
