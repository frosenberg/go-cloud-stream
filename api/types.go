package api

// Define the possible semantic of an input and output channel.
type BindingSemantic string

// The two possible binding semantics
const (
	TopicSemantic BindingSemantic = "topic"
	QueueSemantic BindingSemantic = "queue"
)

// A Transport is the underlying mechanism for sending and receiving messages
// between individual modules. Examples could be Redis, Kafka, RabbitMQ, etc.
type Transport struct {
	InputBinding   string
	InputSemantic  BindingSemantic
	OutputBinding  string
	OutputSemantic BindingSemantic
}

// The TransportInterface allows connecting and disconnecting to a particular
// Transport and managing its connectivity.
type TransportInterface interface {

	// Connects to the underlying transport mechanism. In case of connectivity
	// issue it will retry a number of time and then fail.
	Connect() error

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

// Type for implementing a Source module. A source module performs some task
// and outputs the results as a Message to downstream modules.
type Source func(output chan<- *Message)

// Type for implementing a Sink module. A sink modules is a module that receives
// messages from upstream modules and processes them.
type Sink func(input <-chan *Message)

// Type for implementing a Processor module. A processor modules receives messages
// from upstream modules, processes them, and outputs the result to the downstream
// modules.
type Processor func(input <-chan *Message, output chan<- *Message)
