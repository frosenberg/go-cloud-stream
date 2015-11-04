package stream

import (
	log "github.com/Sirupsen/logrus"
	"github.com/frosenberg/go-cloud-stream/api"
	"github.com/frosenberg/go-cloud-stream/transport/redis"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"os/signal"
	"syscall"
	"fmt"
)

// all CLI variables
var (
	debug         = kingpin.Flag("verbose", "Enable debug logging.").Short(byte('v')).Default("false").Bool()
	redisAddress  = kingpin.Flag("spring.redis.host", "Address for the Redis server.").Default(":6379").OverrideDefaultFromEnvar("SPRING_REDIS_HOST").String()
	inputBinding  = kingpin.Flag("spring.cloud.stream.bindings.input.destination", "Input Binding queue or topic.").Short(byte('i')).Default("input").String()
	outputBinding = kingpin.Flag("spring.cloud.stream.bindings.output.destination", "Output Binding queue or topic.").Short(byte('o')).Default("output").String()
	ServerPort    = kingpin.Flag("server.port", "HTTP Server port.").Default("8080").OverrideDefaultFromEnvar("SERVER_PORT").Short(byte('p')).String()

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

// Lazy initialize a transport
func getTransport() api.TransportInterface {

	// TODO init based on CLI setting
	// TODO figure out CLI settings for this
	if transport == nil {
		redisTransport := redis.NewRedisTransport(*redisAddress, *inputBinding, *outputBinding)

		log.Debugln("redisTransport.inputName: ", redisTransport.InputBinding)
		log.Debugln("redisTransport.outputName: ", redisTransport.OutputBinding)

		log.Debugln("CLI Arguments:")
		log.Debugln("\tAddress: ", redisTransport.Address)
		log.Debugln("\tMax: ", redisTransport.MaxConnections)
		log.Debugln("\tOutput: ", redisTransport.OutputBinding)

		transport = redisTransport
	}
	return transport.(api.TransportInterface)
}

// helper to cast the transport to an InputChannel
func getInputChannel() api.InputChannel {
	return transport.(api.InputChannel)
}

// helper to cast the transport to an OutputChannel
func getOutputChannel() api.OutputChannel {
	return transport.(api.OutputChannel)
}

// helper to cast the transport to an InputOutputChannel
func getInputOutputChannel() api.InputOutputChannel {
	return transport.(api.InputOutputChannel)
}

func Init() {
	// CLI init
	kingpin.Version("0.0.1")
	kingpin.CommandLine.HelpFlag.Short(byte('h'))
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
	err := getTransport().Connect()
	panicOnError(err)
	fs(getOutputChannel())
	defer getTransport().Disconnect()
}

func RunSink(fs api.Sink) {
	err := getTransport().Connect()
	panicOnError(err)
	fs(getInputChannel())
	defer getTransport().Disconnect()
}

func RunProcessor(fp api.Processor) {
	err := getTransport().Connect()
	panicOnError(err)
	fp(getInputOutputChannel())
	defer getTransport().Disconnect()
}


func panicOnError(err error) {
	if err != nil {
		msg := fmt.Sprintf("Error while connecting to transport: %s", err.Error)
		log.Fatalln(msg)
		panic(msg)
	}
}

func Cleanup() {
	log.Debugln("Cleaning up")
	if transport != nil {
		getTransport().Disconnect()
	}
}
