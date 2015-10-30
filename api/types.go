package api

type Transport struct {
	InputBinding string
	OutputBinding string
	// todo other properties
}

type TransportInterface interface {
	Connect() (err error)
	Disconnect() (err error)
}

type InputChannel interface {
	Receive() <-chan Message
}

type OutputChannel interface {
	Send(m *Message) (err error)
}

type InputOutputChannel interface {
	InputChannel
	OutputChannel
}

type Source func(OutputChannel)
type Sink func(InputChannel)
type Processor func(InputOutputChannel)
