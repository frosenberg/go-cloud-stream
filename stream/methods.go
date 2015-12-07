package stream

import (
	log "github.com/Sirupsen/logrus"
	"github.com/frosenberg/go-cloud-stream/api"
	"github.com/frosenberg/go-cloud-stream/transport/kafka"
	"github.com/frosenberg/go-cloud-stream/transport/redis"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

// all CLI variables
var (
	debug         = kingpin.Flag("verbose", "Enable debug logging.").Short(byte('v')).Default("false").Bool()
	inputBinding  = kingpin.Flag("spring.cloud.stream.bindings.input.destination", "Input Binding queue or topic.").Short(byte('i')).Default("input").String()
	outputBinding = kingpin.Flag("spring.cloud.stream.bindings.output.destination", "Output Binding queue or topic.").Short(byte('o')).Default("output").String()
	// not use currently beyond this point - planned to be used for health check
	ServerPort = kingpin.Flag("server.port", "HTTP Server port.").Default("8080").OverrideDefaultFromEnvar("SERVER_PORT").Short(byte('p')).String()

	// Redis specific properties
	redisAddress        = kingpin.Flag("spring.redis.host", "Address for the Redis server.").Default(":6379").OverrideDefaultFromEnvar("SPRING_REDIS_HOST").String()
	redisSentinelNodes  = kingpin.Flag("spring.redis.sentinel.nodes", "Address for the Redis sentinel server.").OverrideDefaultFromEnvar("SPRING_REDIS_SENTINEL_NODES").String()
	redisSentinelMaster = kingpin.Flag("spring.redis.sentinel.master", "Address for the Redis master node.").Default("mymasters").OverrideDefaultFromEnvar("SPRING_REDIS_SENTINEL_MASTER").String()

	// Kafka specific properties
	zkNodes      = kingpin.Flag("spring.cloud.stream.binder.kafka.zkNodes", "Addresses of the Zookeeper nodes.").Default("localhost:2181").OverrideDefaultFromEnvar("SPRING_CLOUD_STREAM_BINDER_KAFKA_ZKNODES").String()
	kafkaBrokers = kingpin.Flag("spring.cloud.stream.binder.kafka.brokers", "Addresses of the Kafka brokers.").Default("localhost:9092").OverrideDefaultFromEnvar("SPRING_CLOUD_STREAM_BINDER_KAFKA_BROKERS").String()

	// TODO add deployment properties for partitioning

	transport = interface{}(nil)
)

// Lazy initialize a transport
func getTransport() api.TransportInterface {
	log.Debugln("CLI Arguments:")
	log.Debugln("\tredisAddress: ", *redisAddress)
	log.Debugln("\tredisSentinelNodes: ", *redisSentinelNodes)
	log.Debugln("\tredisSentinelMaster: ", *redisSentinelMaster)
	log.Debugln("\tkafkaBrokers: ", *kafkaBrokers)
	log.Debugln("\tzkNodes: ", *zkNodes)
	log.Debugln("\tinputBinding: ", *inputBinding)
	log.Debugln("\toutputBinding: ", *outputBinding)

	if transport == nil {

		if *kafkaBrokers != "" {
			transport = kafka.NewKafkaTransport(strings.Split(*kafkaBrokers, ","), strings.Split(*zkNodes, ","), *inputBinding, *outputBinding)
		} else if *redisSentinelNodes != "" {
			transport = redis.NewRedisTransport(*redisSentinelNodes, *redisSentinelMaster, *inputBinding, *outputBinding)
		} else if *redisAddress != "" {
			transport = redis.NewRedisTransport(*redisAddress, *redisSentinelMaster, *inputBinding, *outputBinding)
		}
	}
	return transport.(api.TransportInterface)
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
	transport := getTransport()
	transport.Connect()
	defer transport.Disconnect()

	transport.RunSource(fs)
}

func RunSink(fs api.Sink) {
	transport := getTransport()
	transport.Connect()
	defer transport.Disconnect()

	transport.RunSink(fs)
}

func RunProcessor(fp api.Processor) {
	transport := getTransport()
	transport.Connect()
	defer transport.Disconnect()

	transport.RunProcessor(fp)
}

func Cleanup() {
	log.Debugln("Cleaning up")
	if transport != nil {
		getTransport().Disconnect()
	}
}
