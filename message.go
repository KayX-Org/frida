package frida

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type Topic string

func (t Topic) String() string {
	return string(t)
}

type Message struct {
	event     interface{}
	topic     Topic
	timestamp *time.Time
	ctx       *context.Context
}

func NewMessage(event interface{}, topic Topic) *Message {
	return &Message{
		event: event,
		topic: topic,
	}
}

func (m *Message) GetContext() context.Context {
	if m.ctx != nil {
		return *m.ctx
	}

	return context.Background()
}

func (m *Message) WithContext(ctx context.Context) *Message {
	m.ctx = &ctx
	return m
}

func (m *Message) WithTimestamp(timestamp time.Time) *Message {
	m.timestamp = &timestamp
	return m
}

func (m *Message) GetBody() ([]byte, error) {
	if res, err := json.Marshal(m.event); err != nil {
		return nil, fmt.Errorf("unable to unmarshall message '%s': %w", m.topic, err)
	} else {
		return res, nil
	}
}

func (m *Message) GetTopic() string {
	return m.topic.String()
}

func (m *Message) GetTimestamp() *time.Time {
	return m.timestamp
}
