package frida

type Publisher interface {
	PublishMessage(message *Message) error
}
