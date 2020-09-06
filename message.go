package frida

import (
	"context"
	"time"
)

type Topic string

func (t Topic) String() string {
	return string(t)
}

type Message struct {
	body      []byte
	topic     Topic
	timestamp time.Time
	ctx       *context.Context
}

func NewMessage(body []byte, topic Topic) *Message {
	return &Message{
		body:      body,
		topic:     topic,
		timestamp: time.Now(),
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

func (m *Message) GetBody() []byte {
	return m.body
}

func (m *Message) GetTopic() string {
	return m.topic.String()
}

func (m *Message) GetTimestamp() time.Time {
	return m.timestamp
}
