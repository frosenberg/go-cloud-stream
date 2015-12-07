package redis

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/frosenberg/go-cloud-stream/api"
	"github.com/mediocregopher/radix.v2/redis"
	"os"
	"testing"
	"time"
)

var (
	senderChan   = make(chan string, 1)
	receiverChan = make(chan string, 1)
	bridgeChan   = make(chan string, 1)
	maxMessages  = 150
)

func init() {
	log.SetLevel(log.DebugLevel)
	client := pingRedis()
	defer cleanupRedis(client)
}

func TestNewRedisTransportEmpty0(t *testing.T) {
	r := NewRedisTransport(":6379", "", "", "")

	if r.Address != "localhost:6379" {
		t.Fatalf("Unexpected redis address: %s", r.Address)
	}
}

func TestNewRedisTransportEmpty1(t *testing.T) {
	r := NewRedisTransport("localhost", "", "", "")

	if r.Address != "localhost:6379" {
		t.Fatalf("Unexpected redis address: %s", r.Address)
	}
}

func TestNewRedisTransportEmpty2(t *testing.T) {
	r := NewRedisTransport("", "mymaster", "", "")

	if r.Address != "localhost:6379" {
		t.Fatalf("Unexpected redis address: %s", r.Address)
	}
	if r.SentinelMaster != "mymaster" {
		t.Fatalf("Unexpected sentinel master: %s ", r.SentinelMaster)
	}
	if r.InputBinding != "queue.input" {
		t.Fatalf("Unexpected input binding: %s", r.InputBinding)
	}
	if r.OutputBinding != "queue.output" {
		t.Fatalf("Unexpected output binding: %s", r.OutputBinding)
	}
}

func TestNewRedisTransportQueue(t *testing.T) {
	r := NewRedisTransport("", "", "queue:foo", "queue:bar")

	if r.InputBinding != "queue.foo" {
		t.Fatalf("Unexpected input binding: %s", r.InputBinding)
	}
	if r.OutputBinding != "queue.bar" {
		t.Fatalf("Unexpected output binding: %s", r.OutputBinding)
	}
}

func TestNewRedisTransportTopic(t *testing.T) {
	r := NewRedisTransport("", "", "topic:foo", "topic:bar")

	if r.InputBinding != "topic.foo" {
		t.Fatalf("Unexpected input binding: %s", r.InputBinding)
	}
	if r.OutputBinding != "topic.bar" {
		t.Fatalf("Unexpected output binding: %s", r.OutputBinding)
	}
}

func TestConnectToNotExistingRedis(t *testing.T) {
	redis := NewRedisTransport("doesnotexist:6379", "", "input", "ouput")
	err := redis.Connect()
	if err == nil { // expect an error
		log.Debugln("Error: ", err)
		t.Fatal("Expected connection to fail but it passed.")
	}
}

func TestConnectToExistingSingleRedis(t *testing.T) {
	redis := NewRedisTransport(getRedisHost(), "", "input", "ouput")
	err := redis.Connect()
	if err != nil {
		t.Fatal("Expected connection not established.")
	}
	redis.Disconnect()
}

func TestConnectToExistingSentinelRedis(t *testing.T) {
	redis := NewRedisTransport(getRedisSentinelHost(), getRedisMaster(), "input", "ouput")
	err := redis.Connect()
	if err != nil {
		t.Fatal("Expected connection not established.")
	}
	redis.Disconnect()
}

func TestDisconnectWithoutConnecting(t *testing.T) {
	redis := NewRedisTransport(getRedisHost(), "", "input", "ouput")
	redis.Disconnect() // should not fail
}

func TestSendingAndReceiveWithQueueSingleHost(t *testing.T) {
	queueName := fmt.Sprintf("queue:test-%d", time.Now().UnixNano())
	redis := NewRedisTransport(getRedisHost(), "", queueName, queueName)

	redis.Connect()
	log.Debugf("TestSendingAndReceiveWithQueueSingleHost.queueName: %s", queueName)

	sendAndReceiveWithQueueInternal(t, redis)
	defer redis.Disconnect()
}

func TestSendingAndReceiveWithQueueSentinel(t *testing.T) {
	queueName := fmt.Sprintf("queue:test-%d", time.Now().UnixNano())
	redis := NewRedisTransport(getRedisSentinelHost(), getRedisMaster(), queueName, queueName)

	redis.Connect()
	log.Debugf("TestSendingAndReceiveWithQueueSingleHost.queueName: %s", queueName)

	sendAndReceiveWithQueueInternal(t, redis)
	defer redis.Disconnect()
}

func TestProcessorWithQueueSingleHost(t *testing.T) {

	inputQueue0 := fmt.Sprintf("queue:input0-%d", time.Now().UnixNano())
	outputQueue0 := fmt.Sprintf("queue:output0-%d", time.Now().UnixNano())
	r1 := NewRedisTransport(getRedisHost(), "", inputQueue0, outputQueue0)
	r1.Connect()
	defer r1.Disconnect()

	inputQueue1 := outputQueue0
	outputQueue1 := fmt.Sprintf("queue:output1-%d", time.Now().UnixNano())
	r2 := NewRedisTransport(getRedisHost(), "", inputQueue1, outputQueue1)
	r2.Connect()
	defer r2.Disconnect()

	inputQueue2 := outputQueue1
	outputQueue2 := fmt.Sprintf("queue:output2-%d", time.Now().UnixNano())
	r3 := NewRedisTransport(getRedisHost(), "", inputQueue2, outputQueue2)
	r3.Connect()
	defer r3.Disconnect()

	processorWithQueueInternal(t, r1, r2, r3)

}

func TestProcessorWithQueueSentinelHost(t *testing.T) {

	inputQueue0 := fmt.Sprintf("queue:input0-%d", time.Now().UnixNano())
	outputQueue0 := fmt.Sprintf("queue:output0-%d", time.Now().UnixNano())
	r1 := NewRedisTransport(getRedisSentinelHost(), getRedisMaster(), inputQueue0, outputQueue0)
	r1.Connect()
	defer r1.Disconnect()

	inputQueue1 := outputQueue0
	outputQueue1 := fmt.Sprintf("queue:output1-%d", time.Now().UnixNano())
	r2 := NewRedisTransport(getRedisSentinelHost(), getRedisMaster(), inputQueue1, outputQueue1)
	r2.Connect()
	defer r2.Disconnect()

	inputQueue2 := outputQueue1
	outputQueue2 := fmt.Sprintf("queue:output2-%d", time.Now().UnixNano())
	r3 := NewRedisTransport(getRedisSentinelHost(), getRedisMaster(), inputQueue2, outputQueue2)
	r3.Connect()
	defer r3.Disconnect()

	processorWithQueueInternal(t, r1, r2, r3)

}

func TestProcessorWithTopicOnSingleHost(t *testing.T) {

	inputTopic0 := fmt.Sprintf("topic:input0-%d", time.Now().UnixNano())
	outputTopic0 := fmt.Sprintf("topic:output0-%d", time.Now().UnixNano())
	r1 := NewRedisTransport(getRedisHost(), "", inputTopic0, outputTopic0)
	r1.Connect()
	defer r1.Disconnect()

	inputTopic1 := outputTopic0
	outputTopic1 := fmt.Sprintf("topic:output1-%d", time.Now().UnixNano())
	r2 := NewRedisTransport(getRedisHost(), "", inputTopic1, outputTopic1)
	r2.Connect()
	defer r2.Disconnect()

	inputTopic2 := outputTopic1
	outputTopic2 := fmt.Sprintf("topic:output2-%d", time.Now().UnixNano())
	r3 := NewRedisTransport(getRedisHost(), "", inputTopic2, outputTopic2)
	r3.Connect()
	defer r3.Disconnect()

	processorWithTopicInternal(t, r1, r2, r3)
}

func TestProcessorWithTopicOnSentinelHost(t *testing.T) {

	inputTopic0 := fmt.Sprintf("topic:input0-%d", time.Now().UnixNano())
	outputTopic0 := fmt.Sprintf("topic:output0-%d", time.Now().UnixNano())
	r1 := NewRedisTransport(getRedisSentinelHost(), getRedisMaster(), inputTopic0, outputTopic0)
	r1.Connect()
	defer r1.Disconnect()

	inputTopic1 := outputTopic0
	outputTopic1 := fmt.Sprintf("topic:output1-%d", time.Now().UnixNano())
	r2 := NewRedisTransport(getRedisSentinelHost(), getRedisMaster(), inputTopic1, outputTopic1)
	r2.Connect()
	defer r2.Disconnect()

	inputTopic2 := outputTopic1
	outputTopic2 := fmt.Sprintf("topic:output2-%d", time.Now().UnixNano())
	r3 := NewRedisTransport(getRedisSentinelHost(), getRedisMaster(), inputTopic2, outputTopic2)
	r3.Connect()
	defer r3.Disconnect()

	processorWithTopicInternal(t, r1, r2, r3)
}

func countingSource(output chan<- *api.Message) {
	log.Debugln("countingSource started")

	for i := 0; i < maxMessages; i++ {
		log.Debugf("Sending message: %d", i)
		output <- api.NewTextMessage([]byte(fmt.Sprintf("message: %d", i)))
	}
	senderChan <- "countingSource"
}

func countingSink(input <-chan *api.Message) {
	log.Debugln("countingSink started")

	for i := 0; i < maxMessages; i++ {
		msg := <-input
		log.Debugln("Received message: ", msg)
	}
	receiverChan <- "countingSink"
}

func bridgeFunc(input <-chan *api.Message, output chan<- *api.Message) {
	log.Debugln("bridgeFunc started")

	for i := 0; i < maxMessages; i++ {
		msg := <-input
		log.Debugf("Bridge received %s", msg)
		output <- msg
	}
	bridgeChan <- "bridgeChan"
}

func processorWithQueueInternal(t *testing.T, r1 *RedisTransport, r2 *RedisTransport, r3 *RedisTransport) {

	go func() {
		r1.RunSource(countingSource)
	}()

	// wait 2 seconds for sending to finish
	select {
	case res := <-senderChan:
		log.Debugln(res)
	case <-time.After(time.Second * 2):
		t.Fatal("Sending did not finish in time")
	}

	go func() {
		r2.RunProcessor(bridgeFunc)
	}()

	// wait 2 seconds for bridge to finish
	select {
	case res := <-bridgeChan:
		log.Debugln(res)
	case <-time.After(time.Second * 2):
		t.Fatal("Bridiging did not finish in time")
	}

	go func() {
		r3.RunSink(countingSink)
	}()

	// wait 2 seconds for sending to finish (it will timeout if maxMessages messages have not been received)
	select {
	case res := <-receiverChan:
		log.Debugln(res)
	case <-time.After(time.Second * 2):
		t.Fatal("Receiving did not finish in time")
	}
}

func processorWithTopicInternal(t *testing.T, r1 *RedisTransport, r2 *RedisTransport, r3 *RedisTransport) {

	go func() {
		r2.RunProcessor(bridgeFunc)
	}()

	go func() {
		r3.RunSink(countingSink)
	}()

	go func() {
		time.Sleep(2 * time.Second)
		r1.RunSource(countingSource)
	}()

	// wait 5 seconds for sending to finish
	select {
	case res := <-senderChan:
		log.Debugln(res)
	case <-time.After(time.Second * 5):
		t.Fatal("Sending did not finish in time")
	}

	// wait 5 seconds for bridge to finish
	select {
	case res := <-bridgeChan:
		log.Debugln(res)
	case <-time.After(time.Second * 5):
		t.Fatal("Bridiging did not finish in time")
	}

	// wait 5 seconds for sending to finish (it will timeout if maxMessages messages have not been received)
	select {
	case res := <-receiverChan:
		log.Debugln(res)
	case <-time.After(time.Second * 5):
		t.Fatalf("Receiving did not finish in time")
	}
}

func sendAndReceiveWithQueueInternal(t *testing.T, redis *RedisTransport) {

	// sender
	go func() {
		redis.RunSource(countingSource)
	}()

	// wait 2 seconds for sending to finish
	select {
	case res := <-senderChan:
		log.Debugln(res)
	case <-time.After(time.Second * 2):
		t.Fatalf("Sending did not finish in time")
	}

	//	receiver
	go func() {
		redis.RunSink(countingSink)
	}()

	// wait 2 seconds for sending to finish (it will timeout if 100 messages have not been received
	select {
	case res := <-receiverChan:
		log.Debugln(res)
	case <-time.After(time.Second * 2):
		t.Fatalf("Sending and receiving did not finish in time")
	}

}

func getRedisHost() string {
	redisHost, set := os.LookupEnv("TEST_REDIS_HOST")
	if !set {
		redisHost = "localhost:6379"
	}
	return redisHost
}

func getRedisSentinelHost() string {
	redisHost, set := os.LookupEnv("TEST_REDIS_SENTINEL_HOST")
	if !set {
		redisHost = "localhost:26379"
	}
	return redisHost
}

func getRedisMaster() string {
	master, set := os.LookupEnv("TEST_REDIS_MASTER")
	if !set {
		master = "mymaster"
	}
	return master
}

// Redis is needed to run this test
func pingRedis() *redis.Client {

	redisClient, err := redis.Dial("tcp", getRedisHost())
	if err != nil {
		log.Warnln("Cannot find Redis (standalone) at 'localhost:6379'.")
		log.Infoln("Trying to connect to Redis sentinel ...")

		redisClient2, err2 := redis.Dial("tcp", getRedisSentinelHost())
		if err2 != nil {
			log.Infoln("Please start a local Redis or Redis sentinel.")
			log.Infoln("Please specify TEST_REDIS_HOST or TEST_REDIS_SENTINEL_HOST (and optionally TEST_REDIS_MASTER)")
			panic("Cannot find Redis server.")
		}
		log.Infof("Successfully connected to Redis Sentinel '%s'", redisClient2.Addr)
		return redisClient2
	}
	log.Infof("Successfully connected to Redis '%s'", redisClient.Addr)
	return redisClient
}

func cleanupRedis(client *redis.Client) {
	if client != nil {
		defer client.Close()
	}
}
