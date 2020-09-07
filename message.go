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
	body      []byte
	event     Event
	topic     Topic
	timestamp *time.Time
	ctx       *context.Context
}

func newMessage(body []byte, topic Topic) *Message {
	return &Message{
		body:  body,
		topic: topic,
	}
}

func NewMessage(event Event) *Message {
	return &Message{
		event: event,
		topic: event.Topic(),
	}
}

func (m *Message) Context() context.Context {
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

func (m *Message) parseBody() error {
	if m.event == nil {
		return nil
	}

	body, err := json.Marshal(m.event)
	if err != nil {
		return fmt.Errorf("unable to unmarshall event '%s': %w", m.topic, err)
	}
	m.body = body

	return nil
}

func (m *Message) Body() []byte {
	return m.body
}

func (m *Message) Topic() Topic {
	return m.topic
}

func (m *Message) Timestamp() *time.Time {
	return m.timestamp
}
