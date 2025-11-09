package pubsub

import (
	"bytes"
	"encoding/gob"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeGOB[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	err := subscribe(conn, exchange, queueName, key, simpleQueueType, handler, func(data []byte) (T, error) {
		buffer := bytes.NewBuffer(data)
		var target T
		decoder := gob.NewDecoder(buffer)
		err := decoder.Decode(&target)
		if err != nil {
			return target, err
		}
		return target, nil
	})
	if err != nil {
		return err
	}
	return nil
}
