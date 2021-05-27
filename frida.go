package frida

import (
	"context"
	"fmt"
	"github.com/kayx-org/frida/queue"
	"github.com/kayx-org/frida/topology"
	"github.com/streadway/amqp"
	"os"
	"time"
)

type logger interface {
	Errorf(format string, args ...interface{})
	Infof(format string, args ...interface{})
}

type Register interface {
	RegisterHandler(topic Topic, handler Handler) error
}

type topologyCreator interface {
	CreateTopology(ctx context.Context) error
}

type Handler func(*Message) error

type Frida struct {
	consuming       bool
	topologyCreator topologyCreator
	handlers        map[Topic]Handler
	connection      *amqp.Connection
	bucket          string
	logger          logger
}

func New(logger logger) (*Frida, error) {
	conn, err := amqp.Dial(getEnv("RABBITMQ_HOST", "amqp://guest:guest@localhost:5672/"))
	if err != nil {
		return nil, fmt.Errorf("unable to connect: %w", err)
	}

	bucket := getEnv("SERVICE_NAME", "service")
	return &Frida{
		consuming:       true,
		connection:      conn,
		topologyCreator: topology.NewTopologyCreator(conn, bucket),
		handlers:        make(map[Topic]Handler),
		bucket:          bucket,
		logger:          logger,
	}, nil
}

func (f *Frida) RegisterHandler(topic Topic, handler Handler) error {
	if _, ok := f.handlers[topic]; ok {
		return fmt.Errorf("topic '%s' already registered", topic)
	}

	f.handlers[topic] = handler
	return nil
}

func (f *Frida) Init() error {
	return f.topologyCreator.CreateTopology(context.Background())
}

func (f *Frida) Run(ctx context.Context) error {
	return f.run(ctx)
}

func (f *Frida) PublishMessage(message *Message) error {
	channel, err := f.connection.Channel()
	if err != nil {
		return fmt.Errorf("unable to create channel: %w", err)
	}

	timestamp := time.Now()
	if message.Timestamp() != nil {
		timestamp = *message.timestamp
	}

	if err := message.parseBody(); err != nil {
		return nil
	}

	if err := channel.Publish("bus", message.Topic().String(), false, false, amqp.Publishing{
		Headers:      nil,
		ContentType:  "applications/json",
		Timestamp:    timestamp,
		Body:         message.Body(),
		DeliveryMode: amqp.Persistent,
	}); err != nil {
		return fmt.Errorf("unable to publish message: %s", err)
	}

	return nil
}

func (f *Frida) run(ctx context.Context) error {
	for topic, handler := range f.handlers {
		if err := f.runHandler(ctx, topic, handler); err != nil {
			return err
		}
	}

	return nil
}

func (f *Frida) runHandler(_ context.Context, topic Topic, handler Handler) error {
	channel, err := f.connection.Channel()
	if err != nil {
		return fmt.Errorf("unable to create channel: %w", err)
	}

	topicQueue := queue.NewQueue(f.bucket, topic.String(), channel)
	if err := topicQueue.CreateQueue(); err != nil {
		return err
	}

	msg, err := channel.Consume(topicQueue.Name(), "", false, false, false, false, nil)
	if err := topicQueue.CreateQueue(); err != nil {
		return fmt.Errorf("unable to start consuming: %w", err)
	}

	go f.consume(msg, channel, handler, topicQueue.Name())

	return nil
}

func (f *Frida) consume(msg <-chan amqp.Delivery, channel *amqp.Channel, handler Handler, topicName string) {
	defer func() {
		// If one of the channels stop consuming we will close all of the rest
		f.consuming = false
		_ = channel.Close()
	}()

	f.logger.Infof("start consuming for topic '%s'", topicName)
	for m := range msg {
		if !f.consuming {
			return
		}

		translatedMsg := newMessage(m.Body, Topic(m.RoutingKey))
		translatedMsg.timestamp = &m.Timestamp
		if err := handler(translatedMsg); err != nil {
			if err := m.Nack(false, false); err != nil {
				f.logger.Errorf("unable to nack topic '%s': %s", m.RoutingKey, err)
			}
		} else {
			if err := m.Ack(false); err != nil {
				f.logger.Errorf("unable to ack topic '%s': %s", m.RoutingKey, err)
			}
		}
	}
}

func (f *Frida) Stop(_ context.Context) error {
	f.consuming = false
	return f.connection.Close()
}

func (f *Frida) IsRunning() bool {
	return f.consuming && !f.connection.IsClosed()
}

func getEnv(key string, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}

	return defaultVal
}
