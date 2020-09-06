package topology

import (
	"context"
	"fmt"
	"github.com/streadway/amqp"
)

type TopologyCreator struct {
	connection *amqp.Connection
	bucket     string
	bucketDlx  string
}

func NewTopologyCreator(connection *amqp.Connection, bucket string) *TopologyCreator {
	return &TopologyCreator{connection: connection, bucket: bucket, bucketDlx: fmt.Sprintf("%s-dlx", bucket)}
}

func (t *TopologyCreator) CreateTopology(ctx context.Context) error {
	channel, err := t.connection.Channel()
	if err != nil {
		return fmt.Errorf("unable to create channel: %w", err)
	}
	if err := t.createExchange(ctx, channel); err != nil {
		return err
	}

	return t.createExchangeDlx(ctx, channel)
}

func (t *TopologyCreator) createExchange(_ context.Context, channel *amqp.Channel) error {
	if err := channel.ExchangeDeclare("bus", "fanout", true, true, false, false, amqp.Table{}); err != nil {
		return fmt.Errorf("unable to create the bus exchange: %w", err)
	}
	if err := channel.ExchangeDeclare(t.bucket, "topic", true, true, false, false, amqp.Table{}); err != nil {
		return fmt.Errorf("unable to create the topic exchange: %w", err)
	}

	if err := channel.ExchangeBind(t.bucket, "", "bus", false, amqp.Table{}); err != nil {
		return fmt.Errorf("unable to create the exchange binding: %w", err)
	}

	return nil
}

func (t *TopologyCreator) createExchangeDlx(_ context.Context, channel *amqp.Channel) error {
	if err := channel.ExchangeDeclare("bus-dlx", "fanout", true, true, false, false, amqp.Table{}); err != nil {
		return fmt.Errorf("unable to create the bus exchange: %w", err)
	}
	if err := channel.ExchangeDeclare(t.bucketDlx, "topic", true, true, false, false, amqp.Table{}); err != nil {
		return fmt.Errorf("unable to create the topic exchange: %w", err)
	}

	if err := channel.ExchangeBind(t.bucketDlx, "", "bus-dlx", false, amqp.Table{}); err != nil {
		return fmt.Errorf("unable to create the exchange binding: %w", err)
	}

	return nil
}
