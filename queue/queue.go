package queue

import (
	"fmt"
	"github.com/streadway/amqp"
)

type Queue struct {
	bucket  string
	topic   string
	channel *amqp.Channel
}

func NewQueue(bucket, topic string, channel *amqp.Channel) *Queue {
	return &Queue{
		bucket:  bucket,
		topic:   topic,
		channel: channel,
	}
}

func (q *Queue) Name() string {
	return fmt.Sprintf("%s:%s", q.bucket, q.topic)
}

func (q *Queue) nameDlx() string {
	return fmt.Sprintf("%s:%s_dlx", q.bucket, q.topic)
}

func (q *Queue) bucketDlx() string {
	return fmt.Sprintf("%s-dlx", q.bucket)
}

func (q *Queue) arguments() map[string]interface{} {
	return map[string]interface{}{
		"x-expires":                 24 * 60 * 60 * 1000, // 1 day
		"x-dead-letter-exchange":    "bus-dlx",
		"x-dead-letter-routing-key": q.Name(),
	}
}

func (q *Queue) CreateQueue() error {
	if _, err := q.channel.QueueDeclare(q.Name(), true, true, false, false, q.arguments()); err != nil {
		return fmt.Errorf("unable to create queue '%s': %w", q.Name(), err)
	}
	if err := q.channel.QueueBind(q.Name(), q.topic, q.bucket, false, amqp.Table{}); err != nil {
		return fmt.Errorf("unable to bind queue '%s': %w", q.Name(), err)
	}

	if _, err := q.channel.QueueDeclare(q.nameDlx(), true, true, false, false, q.arguments()); err != nil {
		return fmt.Errorf("unable to create dlx queue '%s': %w", q.nameDlx(), err)
	}
	if err := q.channel.QueueBind(q.nameDlx(), q.Name(), q.bucketDlx(), false, amqp.Table{}); err != nil {
		return fmt.Errorf("unable to bind dlx queue '%s': %w", q.nameDlx(), err)
	}

	return nil
}
