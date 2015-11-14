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

	// Connects to the underlying transport mechanism. In case of connectivity
	// issue it will retry a number of time and then fail.
	Connect() (error)

	// Executes a source "module" that is given as a "function pointer". This method
	// connects to an outgoing stream of messages at the underlying transport
	// to process and output the generated messages by the source modules
	// (e.g., a http-source which receives HTTP messages and output them as a stream
	// on the underlying transport).
	RunSource(Source)

	// Executes a sink "module" that is given as a "function pointer". This method
	// connects to an incoming stream of messages at the underlying transport
	// and performs some action on them (e.g., store them in a relation database
	// in case of a jdbc-sink).
	RunSink(Sink)

	// Executes a processor modules that is given as a "function pointer". This method
	// connects to an incoming and outgoing stream of messages at the underlying transport.
	// A 'Process' module which processes input messages, performs some logic on the message
	// and pushed them to the output (e.g., filter out message, perform analytics on messages).
	RunProcessor(Processor)

	// Disconnects from the underlying transport.
	Disconnect()
}

// Type for implementing a Source module
type Source func(output chan<- *Message)

// Type for implementing a Sink module
type Sink func(input <-chan *Message)

// Type for implementing a Processor module
type Processor func(input <-chan *Message, output chan<- *Message)
