package main

import (
	"fmt"
	"time"
	"github.com/frosenberg/go-cloud-stream/api"
	"github.com/frosenberg/go-cloud-stream/stream"
)


// code that the module coder would write is below

func timeSource(ch api.OutputChannel) {

	fmt.Println("timeSource: ");

	t := time.Tick(5 * time.Second)
	for now := range t {
		ch.Send(api.NewTextMessage(fmt.Sprint(now)))
	}

}

// TODO write a sink
func logSink(ch api.InputChannel) {
	fmt.Println("logSink")

	out := ch.ReceiveChan()

	for {
		select {
		case s := <-out:
			fmt.Println(s)
		case <-time.After(100 * time.Second):
			fmt.Println("You're too slow.")
			return
		}
	}

}

// TODO write a processor
func regexProcessor(ch api.InputOutputChannel) {
	fmt.Println("regexProcessor")
}


func main() {
	stream.Init() // TODO provide capabilities to provide your own CLI args.
//	stream.RunSink(logSink)
	stream.RunSource(timeSource)
	stream.Cleanup()
}

