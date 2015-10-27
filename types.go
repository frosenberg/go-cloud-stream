package main

type TransportProperties struct {
	name string;
	inputName string
	outputName string
	// todo other properties
}

type Transport interface {
	Connect()
	Disconnect()
}

type InputChannel interface {
	Receive(payload []byte)
}

type OutputChannel interface {
	Send(payload []byte)
}

type InputOutputChannel interface {
	InputChannel
	OutputChannel
}

type Source func(OutputChannel)
type Sink func(InputChannel)
type Processor func(InputOutputChannel)
