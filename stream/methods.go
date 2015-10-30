package stream

import (
	"strings"
	"fmt"
	"os/signal"
	"syscall"
	"os"
	"gopkg.in/alecthomas/kingpin.v2"
	log "github.com/Sirupsen/logrus"
	"github.com/frosenberg/go-cloud-stream/transport/redis"
	"github.com/frosenberg/go-cloud-stream/api"

)

// all CLI variables
var (
	debug = kingpin.Flag("v", "Enable debug logging.").Default("false").Bool()
	redisAddress = kingpin.Flag("spring.redis.host", "Address to the Redis server.").Default(":6379").String()
	inputBinding = kingpin.Flag("spring.cloud.stream.bindings.input", "Input Binding queue or topic.").Default("input").String()
	outputBinding = kingpin.Flag("spring.cloud.stream.bindings.output", "Output Binding queue or topic.").Default("output").String()
	ServerPort = kingpin.Flag("server.port", "HTTP Server port.").Default("8080").String()

// TODO add deployment properties for partitioning

	// TODO add kafka variables

	transport = interface{}(nil)
)

// TODO not really needed as this point
type CloudStreamModule interface {
	Init()
	RunSource(s api.Source)
	RunSink(s api.Sink)
	RunProcessor(p api.Processor)
	Cleanup()
}

// Lazy inits a transport
func getTransport() (api.TransportInterface) {

	// TODO init based on CLI setting
	if transport == nil {
		redisTransport := redis.NewRedisTransport()

		// TODO this needs to be done based on CLI args
		redisTransport.Address = *redisAddress

		if strings.HasPrefix(*inputBinding, "topic:") {
			redisTransport.InputBinding = fmt.Sprint("topic.", *inputBinding)
		} else {
			redisTransport.InputBinding = fmt.Sprint("queue.", *inputBinding)
		}

		if strings.HasPrefix(*outputBinding, "topic:") {
			redisTransport.OutputBinding = fmt.Sprint("topic.", *outputBinding)
		} else {
			redisTransport.OutputBinding = fmt.Sprint("queue.", *outputBinding)
		}

		log.Debugln("redisTransport.inputName: ", redisTransport.InputBinding)
		log.Debugln("redisTransport.outputName: ", redisTransport.OutputBinding)

		log.Debugln("CLI Arguments:")
		log.Debugln("\tAddress: " , redisTransport.Address)
		log.Debugln("\tMax: ", redisTransport.MaxConnections)
		log.Debugln("\tOutput: ", redisTransport.OutputBinding)

		transport = redisTransport
	}
	return transport.(api.TransportInterface)
}

// helper to cast the transport to an InputChannel
func getInputChannel() (api.InputChannel) {
	return transport.(api.InputChannel)
}

// helper to cast the transport to an OutputChannel
func getOutputChannel() (api.OutputChannel) {
	return transport.(api.OutputChannel)
}

// helper to cast the transport to an InputOutputChannel
func getInOutChannel() (api.InputOutputChannel) {
	return transport.(api.InputOutputChannel)
}


func Init() {
	// CLI init
	kingpin.Version("0.0.1")
	kingpin.Parse()
	if *debug {
		log.SetLevel(log.DebugLevel)
	}

	// Catch SIGINT and SIGTERM
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	go func() {
		<-c
		Cleanup()
		os.Exit(1)
	}()
}

//
// Executors for Source/Sink/Processor
//
func RunSource(fs api.Source) {
	getTransport().Connect()
	fs(getOutputChannel())
	defer getTransport().Disconnect()
}

func RunSink(fs api.Sink) {
	getTransport().Connect()
	fs(getInputChannel())
	defer getTransport().Disconnect()
}

func RunProcessor(fp api.Processor) {

	// TODO implement
}

func Cleanup() {
	log.Debugln("Cleaning up")
	if transport != nil {
		getTransport().Disconnect()
	}
}
