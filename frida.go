package frida

import (
	"context"
	"fmt"
	"github.com/kayx-org/freja"
	"github.com/kayx-org/freja/healthcheck"
	"github.com/kayx-org/frida/queue"
	"github.com/kayx-org/frida/topology"
	"github.com/streadway/amqp"
	"os"
)

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
	logger          freja.Logger
}

func New(logger freja.Logger) (*Frida, error) {
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

	if err := channel.Publish("bus", message.topic.String(), false, false, amqp.Publishing{
		Headers:     nil,
		ContentType: "applications/json",
		Timestamp:   message.timestamp,
		Body:        message.body,
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

	go func() {
		defer func() {
			// If one of the channels stop consuming we will close all of the rest
			f.consuming = false
			_ = channel.Close()
		}()
		for m := range msg {
			if !f.consuming {
				return
			}

			translatedMsg := NewMessage(m.Body, Topic(m.RoutingKey))
			translatedMsg.timestamp = m.Timestamp
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
	}()

	return nil
}

func (f *Frida) Stop(_ context.Context) error {
	return f.connection.Close()
}

func (f *Frida) Name() string {
	return "frida"
}

func (f *Frida) Status() healthcheck.ServiceStatus {
	if f.consuming && !f.connection.IsClosed() {
		return healthcheck.UP
	}

	return healthcheck.DOWN
}

func getEnv(key string, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}

	return defaultVal
}
