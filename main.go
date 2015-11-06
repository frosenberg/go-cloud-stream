package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/frosenberg/go-cloud-stream/api"
	"github.com/frosenberg/go-cloud-stream/stream"
	"sync"
)

func dummySource(ch api.OutputChannel) {
	// do nothing
	log.Debug("Dummy source")
}

func dummySink(ch api.InputChannel) {
	// do nothing
	log.Debug("Dummy sink")

}

func dummyProcessor(ch api.InputOutputChannel) {
	// do nothing
	log.Debug("Dummy processor")
}

func main() {
	// nothing to do really as we use it as a library
	stream.Init()

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		stream.RunSource(dummySource)
		wg.Done()
	}()

	go func() {
		stream.RunSink(dummySink)
		wg.Done()
	}()

	go func() {
		stream.RunProcessor(dummyProcessor)
		wg.Done()
	}()

	wg.Wait()
	stream.Cleanup()
}
