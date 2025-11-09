package pubsub

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
	unmarshaller func([]byte) (T, error),
) error {
	chann, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	chann.Qos(10, 0, false)
	msgs, err := chann.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		defer chann.Close()
		for msg := range msgs {
			target, err := unmarshaller(msg.Body)
			if err != nil {
				fmt.Printf("could not unmarshal message: %v\n", err)
				continue
			}
			pckType := handler(target)
			switch pckType {
			case Ack:
				msg.Ack(false)
				fmt.Println("Ack package send")
			case NackRequeue:
				msg.Nack(false, true)
				fmt.Println("Nack package requeue")
			case NackDiscard:
				msg.Nack(false, false)
				fmt.Println("Nack package discard")
			default:
				return
			}
		}
	}()
	return nil
}
