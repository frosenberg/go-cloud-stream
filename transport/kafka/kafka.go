package kafka

import (
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/frosenberg/go-cloud-stream/api"
	"github.com/frosenberg/go-cloud-stream/transport"
	"github.com/pborman/uuid"
	kafkaClient "github.com/stealthly/go_kafka_client"
	"sort"
	"time"
)

//
// Basic Kafka transport information
//
type KafkaTransport struct {
	api.Transport
	Brokers                   []string
	ZookeeperHosts            []string
	TopicReadinessWaitTimeSec int

	client         sarama.Client
	producer       sarama.AsyncProducer
	consumer       sarama.Consumer
	consumerConfig kafkaClient.ConsumerConfig
}

// Creates a new KafkaTransport instance with sensible default values.
func NewKafkaTransport(brokers []string, zkHosts []string, inputBinding string, outputBinding string) *KafkaTransport {
	if len(brokers) == 0 {
		brokers = make([]string, 1)
		brokers[0] = "localhost:9092"
		log.Debugf("No Kafka brokers set. Using localhost:9092")
	}
	if len(zkHosts) == 0 {
		zkHosts = make([]string, 1)
		zkHosts[0] = "localhost:2181"
		log.Debugf("No Zookeeper hosts set. Using localhost:2181")
	}
	if inputBinding == "" {
		inputBinding = "input"
	}
	if outputBinding == "" {
		outputBinding = "output"
	}

	transport := &KafkaTransport{
		Transport: api.Transport{
			InputBinding:   transport.EscapeTopicName(transport.StripPrefix(inputBinding)),
			InputSemantic:  transport.GetBindingSemantic(inputBinding),
			OutputBinding:  transport.EscapeTopicName(transport.StripPrefix(outputBinding)),
			OutputSemantic: transport.GetBindingSemantic(outputBinding),
		},
		Brokers:                   brokers,
		ZookeeperHosts:            zkHosts,
		TopicReadinessWaitTimeSec: 120,
	}

	return transport
}

func (t *KafkaTransport) Connect() error {

	config := sarama.NewConfig()
	config.Producer.Compression = sarama.CompressionSnappy

	client, err := sarama.NewClient(t.Brokers, config)
	if err != nil {
		return err
	}
	t.client = client

	producer, err := sarama.NewAsyncProducerFromClient(t.client)
	if err != nil {
		return err
	}
	t.producer = producer

	// Consumer configuration
	zkConfig := kafkaClient.NewZookeeperConfig()
	zkConfig.ZookeeperConnect = t.ZookeeperHosts

	consumerConfig := kafkaClient.DefaultConsumerConfig()
	consumerConfig.Coordinator = kafkaClient.NewZookeeperCoordinator(zkConfig)
	consumerConfig.RebalanceMaxRetries = 10
	consumerConfig.NumWorkers = 1
	consumerConfig.NumConsumerFetchers = 1
	consumerConfig.AutoOffsetReset = kafkaClient.LargestOffset
	t.consumerConfig = *consumerConfig

	return nil
}

func (t *KafkaTransport) Disconnect() {
	if t.client != nil {
		warnClosing(t.client.Close())
	}
	if t.producer != nil {
		warnClosing(t.producer.Close())
	}
}

func (t *KafkaTransport) RunSource(sourceFunc api.Source) {
	log.Debugf("RunSource called ...")

	channel := make(chan *api.Message)

	if t.OutputSemantic == api.TopicSemantic {
		go t.publish(channel)
	} else {
		go t.push(channel)
	}

	sourceFunc(channel)
}

func (t *KafkaTransport) RunSink(sinkFunc api.Sink) {

	channel := make(chan *api.Message)

	if t.InputSemantic == api.TopicSemantic {
		go t.subscribe(channel) // topic processing
	} else {
		go t.pop(channel) // queue processing
	}

	sinkFunc(channel)
}

func (t *KafkaTransport) RunProcessor(processorFunc api.Processor) {

	inputCh := make(chan *api.Message)
	outputCh := make(chan *api.Message)

	if t.InputSemantic == api.TopicSemantic { // topic processing

		go t.subscribe(inputCh)
		go t.publish(outputCh)

	} else { // queue processing

		go t.push(outputCh)
		go t.pop(inputCh)
	}

	processorFunc(inputCh, outputCh)
}

func (t *KafkaTransport) push(channel chan *api.Message) {

	// TODO add support for creating a topic (port kafka.AdminUtils)

	for m := range channel {
		t.producer.Input() <- &sarama.ProducerMessage{
			Topic: t.OutputBinding,
			//			Key: sarama.ByteEncoder(""+host),
			Value: sarama.ByteEncoder(m.Content), // m.ToRawByteArray(),
		}
	}
}

func (t *KafkaTransport) pop(channel chan *api.Message) {

	// use same group for all consumers -> queue semantics

	t.consumerConfig.Groupid = "groupdId-" + t.InputBinding
	t.consumerConfig.AutoOffsetReset = kafkaClient.SmallestOffset

	startNewConsumer(t.consumerConfig, t.InputBinding, channel)
}

func (t *KafkaTransport) publish(channel chan *api.Message) {
	t.push(channel) // sending semantics is the same for now
}

func (t *KafkaTransport) subscribe(channel chan *api.Message) {

	// usage of a different consumer group each time achieves pub-sub
	// but multiple instances of this binding will each get all messages
	// PubSub consumers reset at the latest time, which allows them to receive only messages sent after
	// they've been bound
	t.consumerConfig.Groupid = uuid.NewRandom().String()
	t.consumerConfig.AutoOffsetReset = kafkaClient.LargestOffset

	startNewConsumer(t.consumerConfig, t.InputBinding, channel)
}

func startNewConsumer(config kafkaClient.ConsumerConfig, topic string, channel chan *api.Message) *kafkaClient.Consumer {

	// wait explicitly for topic to be created by the producer
	waitForTopicToBeReady(config, topic)

	config.Strategy = handleMessage(config.Consumerid, channel)
	config.WorkerFailureCallback = failedCallback
	config.WorkerFailedAttemptCallback = failedAttemptCallback
	//config.NumConsumerFetchers = 2
	consumer := kafkaClient.NewConsumer(&config)
	topics := map[string]int{topic: config.NumConsumerFetchers}

	go func() {
		consumer.StartStatic(topics)
	}()
	return consumer
}

func handleMessage(consumerId string, channel chan *api.Message) func(*kafkaClient.Worker, *kafkaClient.Message, kafkaClient.TaskId) kafkaClient.WorkerResult {
	return func(_ *kafkaClient.Worker, msg *kafkaClient.Message, id kafkaClient.TaskId) kafkaClient.WorkerResult {
		channel <- api.NewTextMessage(msg.Value)
		return kafkaClient.NewSuccessfulResult(id)
	}
}

func failedCallback(wm *kafkaClient.WorkerManager) kafkaClient.FailedDecision {
	log.Debugf("failedCallback")
	return kafkaClient.DoNotCommitOffsetAndStop
}

func failedAttemptCallback(task *kafkaClient.Task, result kafkaClient.WorkerResult) kafkaClient.FailedDecision {
	log.Debugf("failedAttemptCallback callback")
	return kafkaClient.CommitOffsetAndContinue
}

// Queries Zookeeper and waits for a topic to be ready and create so that the
// consumer can consume it.
func waitForTopicToBeReady(config kafkaClient.ConsumerConfig, topic string) {
	log.Debugf("Waiting for topic to be ready: %s", topic)

	result := make(chan bool)
	config.Coordinator.Connect()

	go func() {
		allTopics := []string(nil)
		inputId := -1

		// TODO remove check for get all topics - just ask for paritions seems to be better
		for {
			allTopics, _ = config.Coordinator.GetAllTopics()
			sort.Strings(allTopics)
			inputId = sort.SearchStrings(allTopics, topic)
			if inputId < len(allTopics) {

				// seems to be a good indicator for when the optic is available. Just
				// checking for the node is not enough
				_, err := config.Coordinator.GetPartitionsForTopics([]string{topic})

				if err == nil {
					result <- true
					break
				}
			}
			time.Sleep(250 * time.Millisecond)
		}
	}()

	select {
	case <-result:
		log.Debugf("Topic '%s' ready in Zookeeper!", topic)
	case <-time.After(time.Second * 120):
		log.Fatalf("Timeout waiting for topic '%s' to be ready.", topic)
	}
	close(result)
}

// Helper to log a warning in case of an error.
func warnClosing(err error) {
	if err != nil {
		log.Warnf("Error while closing client: %s", err.Error())
	}
}
