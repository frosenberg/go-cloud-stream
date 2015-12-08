package kafka

import (
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/frosenberg/go-cloud-stream/api"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	senderChan            chan string
	receiverChan          chan string
	receiverChan2         chan string
	bridgeChan            chan string
	pubsubReceivingChan   chan bool
	pubsubStopSendingChan chan bool
	maxMessages                       = 50
)

func init() {
	log.SetLevel(log.DebugLevel)
}

func TestNewKafkaTransportEmpty0(t *testing.T) {
	transport := NewTransport(nil, nil, "", "")

	if len(transport.Brokers) == 0 ||
		transport.Brokers[0] != "localhost:9092" {
		t.Fatalf("Error: Broker is not localhost:9092")
	}
	if len(transport.ZookeeperHosts) == 0 ||
		transport.ZookeeperHosts[0] != "localhost:2181" {
		t.Fatalf("Error: ZK is not localhost:2181")
	}
	if transport.InputBinding != "input" {
		t.Fatal("Expected input binding to be 'input'")
	}
	if transport.InputSemantic != api.QueueSemantic {
		t.Fatal("Expected input semantic to be 'QueueSemantic'")
	}
	if transport.OutputBinding != "output" {
		t.Fatal("Expected output binding to be 'output'")
	}
	if transport.OutputSemantic != api.QueueSemantic {
		t.Fatal("Expected output semantic to be 'QueueSemantic'")
	}
}

func TestNewKafkaTransport(t *testing.T) {
	transport := NewTransport([]string{"localhost:9092", "localhost:9093"}, []string{"localhost:2181", "localhost:2182"}, "topic:foo", "queue:bar")

	if len(transport.Brokers) != 2 &&
		transport.Brokers[0] != "localhost:9092" &&
		transport.Brokers[1] != "localhost:9093" {
		t.Fatal("Error: Broker address incorrect")
	}
	if len(transport.ZookeeperHosts) != 2 &&
		transport.ZookeeperHosts[0] != "localhost:2181" &&
		transport.ZookeeperHosts[1] != "localhost:2182" {
		t.Fatal("Error: ZK address incorrect")
	}
	if transport.InputBinding != "foo" {
		t.Fatalf("Expected input binding to be 'foo'. Got: %s", transport.InputBinding)
	}
	if transport.InputSemantic != api.TopicSemantic {
		t.Fatal("Expected input semantic to be 'TopicSemantic'")
	}
	if transport.OutputBinding != "bar" {
		t.Fatalf("Expected output binding to be 'bar'. Got: %s", transport.OutputBinding)
	}
	if transport.OutputSemantic != api.QueueSemantic {
		t.Fatal("Expected output semantic to be 'QueueSemantic'")
	}
}

// TODO hangs for some reason on dns resolution
//func TestNonExistingConnect(t *testing.T) {
//	broker := "doesnotexist:10323"
//	transport := NewKafkaTransport([]string{broker}, "", "")
//	err := transport.Connect()
//
//	if err == nil {
//		t.Fatalf("Expected an error during connection to: %s", broker)
//	}
//}

func TestConnecting(t *testing.T) {
	broker := "localhost:9092"
	transport := NewTransport([]string{broker}, getZookeeperHosts(), "", "")

	if err := transport.Connect(); err != nil {
		t.Fatalf("Expected connection to '%s' to succeeed", broker)
	}
	transport.Disconnect()
}

func TestSendReceiveQueueSemantics(t *testing.T) {

	initChannels()

	// use same queue for both so we can send and receive
	queueName := fmt.Sprintf("queue:input0-%d", time.Now().UnixNano())

	// init and connect to transport
	transport := NewTransport(getKafkaBrokers(), getZookeeperHosts(), queueName, queueName)
	transport.Connect()
	defer transport.Disconnect()

	// we start two receivers but only one should get the data (because of queue semantics)

	//	receiver 1
	go func() {
		transport.RunSink(countingSink1)
	}()

	//	receiver 2
	go func() {
		transport.RunSink(countingSink2)
	}()

	// sender
	go func() {
		transport.RunSource(countingSource)
	}()

	// wait x seconds for sending to finish
	select {
	case res := <-senderChan:
		log.Debugf("Done sending: %s", res)
	case <-time.After(time.Second * 60):
		t.Fatalf("Sending did not finish in time")
	}

	// wait x seconds for sending to finish (it will timeout if 100 messages have not been received
	receiver1 := ""
	receiver2 := ""
	select {
	case res := <-receiverChan:
		log.Debugf("Done receiving: %s", res)
	case res := <-receiverChan2:
		log.Debugf("Done receiving: %s", res)
	case <-time.After(time.Second * 90):
		t.Fatalf("Sending and receiving did not finish in time")
	}

	log.Debugf("receiver1: %s", receiver1)
	log.Debugf("receiver2: %s", receiver2)

	if receiver1 != "" && receiver2 != "" {
		t.Fatalf("We expect only one sink to receive the data")
	}

	closeChannels()
}

func TestSendReceiveQueueSemantics2(t *testing.T) {

	initChannels()

	inputQueue0 := fmt.Sprintf("queue:input0-%d", time.Now().UnixNano())
	outputQueue0 := fmt.Sprintf("queue:output0-%d", time.Now().UnixNano())
	t1 := NewTransport(getKafkaBrokers(), getZookeeperHosts(), inputQueue0, outputQueue0)
	t1.Connect()
	defer t1.Disconnect()

	inputQueue1 := outputQueue0
	outputQueue1 := fmt.Sprintf("queue:output1-%d", time.Now().UnixNano())
	t2 := NewTransport(getKafkaBrokers(), getZookeeperHosts(), inputQueue1, outputQueue1)
	t2.Connect()
	defer t2.Disconnect()

	inputQueue2 := outputQueue1
	outputQueue2 := fmt.Sprintf("queue:output2-%d", time.Now().UnixNano())
	t3 := NewTransport(getKafkaBrokers(), getZookeeperHosts(), inputQueue2, outputQueue2)
	t3.Connect()
	defer t3.Disconnect()

	go func() {
		t1.RunSource(countingSource)
	}()

	// wait x seconds for sending to finish
	select {
	case res := <-senderChan:
		log.Debugln(res)
	case <-time.After(time.Second * 90):
		t.Fatalf("Sending did not finish in time")
	}

	go func() {
		t2.RunProcessor(bridgeFunc)
	}()

	// wait x seconds for bridge to finish
	select {
	case res := <-bridgeChan:
		log.Debugln(res)
	case <-time.After(time.Second * 90):
		t.Fatalf("Bridiging did not finish in time")
	}

	go func() {
		t3.RunSink(countingSink1)
	}()

	// wait x seconds for sending to finish (it will timeout if maxMessages messages have not been received)
	select {
	case res := <-receiverChan:
		log.Debugln(res)
	case <-time.After(time.Second * 90):
		t.Fatalf("Receiving did not finish in time")
	}

	closeChannels()
}

func TestSendReceiveTopicSemantics(t *testing.T) {

	initChannels()

	// use same queue for both so we can send and receive
	queueName := fmt.Sprintf("topic:input0-%d", time.Now().UnixNano())

	// init and connect to transport
	transport := NewTransport(getKafkaBrokers(), getZookeeperHosts(), queueName, queueName)
	transport.Connect()
	defer transport.Disconnect()

	// sender
	go func() {
		transport.RunSource(pubsubSource)
	}()

	// receiver
	go func() {
		transport.RunSink(pubsubSink1)
	}()

	// receiver
	go func() {
		transport.RunSink(pubsubSink2)
	}()

	// wait for first warmup message
	select {
	case msg := <-pubsubReceivingChan:
		if !msg {
			log.Fatal("Warmup did not receive last message correctly!")
		} else {
			log.Debugf("Finished warmup: %t", msg)
		}
	case <-time.After(time.Second * 90):
		log.Fatalf("Warmup did not finish in time")
	}

	// wait for second warmup message
	select {
	case msg := <-pubsubReceivingChan:
		if !msg {
			log.Fatal("Warmup did not receive last message correctly!")
		} else {
			log.Debugf("Finished warmup: %t", msg)
		}
	case <-time.After(time.Second * 90):
		log.Fatalf("Warmup did not finish in time")
	}

	//	receiver
	go func() {
		transport.RunSink(countingSink1)
	}()

	//	receiver
	go func() {
		transport.RunSink(countingSink2)
	}()

	// wait x seconds for sending to finish
	select {
	case res := <-senderChan:
		log.Debugf("Done sending: %s", res)
	case <-time.After(time.Second * 60):
		t.Fatalf("Sending did not finish in time")
	}

	// wait x seconds for sending to finish (it will timeout if 100 messages have not been received
	select {
	case res := <-receiverChan:
		log.Debugf("Done receiving: %s", res)
	case <-time.After(time.Second * 60):
		t.Fatalf("Sending and receiving did not finish in time")
	}

	// wait x seconds for sending to timeout
	select {
	case res := <-receiverChan2:
		log.Debugf("Done receiving: %s", res)
	case <-time.After(time.Second * 60):
		t.Fatalf("Sending and receiving did not finish in time")
	}

	closeChannels()
}

func TestSendReceiveTopicSemantics2(t *testing.T) {

	initChannels()

	inputTopic0 := fmt.Sprintf("topic:input0-%d", time.Now().UnixNano())
	outputTopic0 := fmt.Sprintf("topic:output0-%d", time.Now().UnixNano())
	t1 := NewTransport(getKafkaBrokers(), getZookeeperHosts(), inputTopic0, outputTopic0)
	t1.Connect()
	defer t1.Disconnect()

	inputTopic1 := outputTopic0
	outputTopic1 := fmt.Sprintf("topic:output1-%d", time.Now().UnixNano())
	t2 := NewTransport(getKafkaBrokers(), getZookeeperHosts(), inputTopic1, outputTopic1)
	t2.Connect()
	defer t2.Disconnect()

	inputTopic2 := outputTopic1
	outputTopic2 := fmt.Sprintf("topic:output2-%d", time.Now().UnixNano())
	t3 := NewTransport(getKafkaBrokers(), getZookeeperHosts(), inputTopic2, outputTopic2)
	t3.Connect()
	defer t3.Disconnect()

	// sender
	go func() {
		t1.RunSource(pubsubSource)
	}()

	// bridge
	go func() {
		t2.RunProcessor(bridgeFunc)
	}()

	// receiver
	go func() {
		t3.RunSink(pubsubSink1)
	}()

	// receiver
	go func() {
		t3.RunSink(pubsubSink2)
	}()

	// wait for first "punch-trough" message
	select {
	case msg := <-pubsubReceivingChan:
		if !msg {
			log.Fatal("Warmup did not receive last message correctly!")
		} else {
			log.Debugf("Finished warmup: %t", msg)
		}
	case <-time.After(time.Second * 90):
		log.Fatalf("Warmup did not finish in time")
	}

	// wait for second "punch-trough" message
	select {
	case msg := <-pubsubReceivingChan:
		if !msg {
			log.Fatal("Warmup did not receive last message correctly!")
		} else {
			log.Debugf("Finished warmup: %t", msg)
		}
	case <-time.After(time.Second * 90):
		log.Fatalf("Warmup did not finish in time")
	}

	//	receiver
	go func() {
		t2.RunSink(countingSink1)
	}()

	//	receiver
	go func() {
		t3.RunSink(countingSink2)
	}()

	// wait x seconds for bridge to finish
	select {
	case res := <-bridgeChan:
		log.Debugln(res)
	case <-time.After(time.Second * 60):
		t.Fatalf("Bridiging did not finish in time")
	}

	// wait x seconds for sending to finish
	select {
	case res := <-senderChan:
		log.Debugf("Done sending: %s", res)
	case <-time.After(time.Second * 60):
		t.Fatalf("Sending did not finish in time")
	}

	// wait x seconds for sending to finish (it will timeout if 100 messages have not been received
	select {
	case res := <-receiverChan:
		log.Debugf("Done receiving: %s", res)
	case <-time.After(time.Second * 60):
		t.Fatalf("Sending and receiving did not finish in time")
	}

	// wait x seconds for sending to timeout
	select {
	case res := <-receiverChan2:
		log.Debugf("Done receiving: %s", res)
	case <-time.After(time.Second * 60):
		t.Fatalf("Sending and receiving did not finish in time")
	}

	// TODO figure out why which channel interferes with closing
	closeChannels()
}

//
// ------ Source, Sink and Processor Implementations
//

type logEntry struct {
	ID           int     `json:"id"`
	Host         string  `json:"host"`
	ResponseTime float64 `json:"response_time"`

	encoded      []byte
	err          error
}

func pubsubSource(output chan<- *api.Message) {
	log.Debugln("warmupSource started")
	i := 0
	stop1 := false
	stop2 := false

	go func() {
		for !stop1 || !stop2 {
			msg := fmt.Sprintf("msg: %d", i)
			log.Debugf("Sending pubsub message: %s", msg)
			output <- api.NewTextMessage([]byte(msg))
			i++
			time.Sleep(1 * time.Second)
		}
	}()

	select {
	case msg := <-pubsubStopSendingChan:
		log.Debugf("pubsubStopSendingChan #1: %t", msg)
		stop1 = true
	}
	select {
	case msg := <-pubsubStopSendingChan:
		log.Debugf("pubsubStopSendingChan #2: %t", msg)
		stop2 = true
	}

	// now we have both consumers online, send 100 messages
	countingSource(output)
}

func pubsubSink1(input <-chan *api.Message) {
	log.Debugln("pubsubSink1 started")

	for {
		msg := <-input
		log.Debugf("[pubsubSink1] Received first pubsub message: %s", msg.String())
		pubsubReceivingChan <- true
		pubsubStopSendingChan <- true
		break
	}

	// warmup don't. send real payload
	countingSink1(input)
}

func pubsubSink2(input <-chan *api.Message) {
	log.Debugln("pubsubSink2 started")
	for {
		msg := <-input
		log.Debugln("[pubsubSink2] Received first pubsub message: %s", msg.String())
		pubsubReceivingChan <- true
		pubsubStopSendingChan <- true
		break
	}

	countingSink2(input)
}

func countingSource(output chan<- *api.Message) {
	log.Debugln("countingSource started")

	for i := 0; i < maxMessages; i++ {
		entry := &logEntry{
			ID:           i,
			Host:         fmt.Sprintf("myhostname-%d", i%10),
			ResponseTime: rand.Float64(),
		}
		json, _ := json.Marshal(entry)

		log.Debugf("[countingSource] Sending message: %s", json)

		// TODO add a JSON message
		output <- api.NewTextMessage([]byte(json))
	}
	senderChan <- "countingSource"
}

func countingSink1(input <-chan *api.Message) {
	log.Debugln("countingSink started")

	i := 0
	var le logEntry
	for i < maxMessages {
		msg := <-input

		if err := json.Unmarshal(msg.Content, &le); err != nil {
			log.Debugf("[countingSink] Error decoding message to json. No json payload. Ignoring.")
		} else {
			log.Debugln("[countingSink] Received message: ", msg)
			i++
		}
	}
	receiverChan <- "countingSink"
}

func countingSink2(input <-chan *api.Message) {
	log.Debugln("countingSink2 started")

	i := 0
	var le logEntry
	for i < maxMessages {
		msg := <-input

		if err := json.Unmarshal(msg.Content, &le); err != nil {
			log.Debugf("[countingSink2] Error decoding message to json. No json payload. Ignoring.")
		} else {
			log.Debugln("[countingSink2] Received message: ", msg)
			i++
		}
	}
	receiverChan2 <- "countingSink2"
}

func bridgeFunc(input <-chan *api.Message, output chan<- *api.Message) {
	log.Debugln("bridgeFunc started")

	i := 0
	var le logEntry
	for i < maxMessages {
		msg := <-input

		if err := json.Unmarshal(msg.Content, &le); err != nil {
			log.Debugf("[bridgeFunc] Error decoding message to json. No json payload. Ignoring.")
		} else {
			log.Debugln("[bridgeFunc] Received message: ", msg)
			i++
		}
		output <- msg
	}
	bridgeChan <- "bridgeChan"
}

func getKafkaBrokers() []string {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers != "" {
		return strings.Split(brokers, ",")
	}
	log.Debug("ENV variable KAFKA_BROKERS not set. Using localhost:9092 as kafka broker host.")
	return nil
}

func getZookeeperHosts() []string {
	zkHosts := os.Getenv("ZK_HOSTS")
	if zkHosts != "" {
		return strings.Split(zkHosts, ",")
	}
	log.Debug("ENV variable ZK_HOSTS not set. Using localhost:2181 as Zookeeper host.")
	return nil
}

func initChannels() {
	senderChan = make(chan string)
	receiverChan = make(chan string)
	receiverChan2 = make(chan string)
	bridgeChan = make(chan string)
	pubsubReceivingChan = make(chan bool)
	pubsubStopSendingChan = make(chan bool)
}

func closeChannels() {
	close(senderChan)
	close(receiverChan)
	close(receiverChan2)
	close(bridgeChan)
	close(pubsubReceivingChan)
	close(pubsubStopSendingChan)
}
