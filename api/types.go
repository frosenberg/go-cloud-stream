package api

// A Transport is the underlying mechanism for sending and receiving messages
// between individual modules. Examples could be Redis, Kafka, RabbitMQ, etc.
type Transport struct {
	InputBinding  string
	OutputBinding string
}

// The TransportInterface allows connecting and disconnecting to a particular
// Transport and managing its connectivity.
type TransportInterface interface {
	Connect() (error)
	Disconnect()
}

// An InputChannel returns a Go channel to receive a Message from the channel.
type InputChannel interface {
	Receive() <-chan Message
}

// An OutputChannel allows to send a Message to the channel.
type OutputChannel interface {
	Send(m *Message) (error)
}

// An InputOutputChannel allow to receive and send message.
type InputOutputChannel interface {
	InputChannel
	OutputChannel
}

// Type for implementing a Source module
type Source func(OutputChannel)

// Type for implementing a Sink module
type Sink func(InputChannel)

// Type for implementing a Processor module
type Processor func(InputOutputChannel)
