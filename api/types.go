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
	Receive(callback OnMessageFunction) (err error)
	ReceiveChan() <-chan Message
}

type OutputChannel interface {
	Send(m *Message) (err error)
}

type InputOutputChannel interface {
	InputChannel
	OutputChannel
}

// TODO Remove - it was for testing a callback but we may remove that as this is not go like at all ...
type OnMessageFunction func(m *Message) // (err error)

type Source func(OutputChannel)
type Sink func(InputChannel)
type Processor func(InputOutputChannel)
