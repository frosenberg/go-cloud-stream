package main

import (
	"fmt"
	"time"
)

//
// Executors for Source/Sink/Processor
//

func executeSource(fs Source) {
	// TODO factor our Redis to be drive by config flags

	properties := new(RedisTransportProperties)

	properties.address = *redisAddress
	properties.name = "RedisTransport"
	properties.outputName = fmt.Sprint("queue." + *outputQueue)

	properties.Connect()

	fs(properties)

	properties.Disconnect();
}

func executeSink(fs Sink) {
	// TODO implement
}

func executeProcessor(fp Processor) {

	// TODO implement
}

// code that the module coder would write is below

func timeSource(ch OutputChannel) {

	fmt.Println("timeSource: ");

		t := time.Tick(1 * time.Second)
		for now := range t {
			tock := fmt.Sprint(now)
			ch.Send([]byte(tock))
		}

}

// TODO write a sink
func logSink(ch InputChannel) {
	fmt.Println("logSink")



}

// TODO write a processor
func regexProcessor(ch InputOutputChannel) {
	fmt.Println("regexProcessor");
}

func main() {

	executeSource(timeSource);

//	executeSink(logSink);
//	executeProcessor(regexProcessor);
}

