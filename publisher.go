package frida

type Event interface {
	Topic() Topic
}

type Publisher interface {
	PublishMessage(message *Message) error
}

type DummyPublisher struct {
	messages []*Message
}

func NewDummyPublisher() *DummyPublisher {
	return &DummyPublisher{messages: make([]*Message, 0)}
}

func (p *DummyPublisher) PublishMessage(message *Message) error {
	p.messages = append(p.messages, message)
	return nil
}

func (p *DummyPublisher) GetPublishedMessages() []*Message {
	return p.messages
}
