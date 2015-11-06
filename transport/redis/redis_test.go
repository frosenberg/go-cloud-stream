package redis

import (
	log "github.com/Sirupsen/logrus"
	"github.com/frosenberg/go-cloud-stream/api"
	"github.com/mediocregopher/radix.v2/redis"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

func init() {
	log.SetLevel(log.DebugLevel)
	client := pingRedis()
	defer cleanupRedis(client)
}

func TestNewRedisTransportEmpty0(t *testing.T) {
	r := NewRedisTransport(":6379", "", "", "")

	if r.Address != "localhost:6379" {
		t.Fatalf("Unexpected redis address: ", r.Address)
	}
}

func TestNewRedisTransportEmpty1(t *testing.T) {
	r := NewRedisTransport("localhost", "", "", "")

	if r.Address != "localhost:6379" {
		t.Fatalf("Unexpected redis address: ", r.Address)
	}
}

func TestNewRedisTransportEmpty2(t *testing.T) {
	r := NewRedisTransport("", "mymaster", "", "")

	if r.Address != "localhost:6379" {
		t.Fatalf("Unexpected redis address: ", r.Address)
	}
	if r.SentinelMaster != "mymaster" {
		t.Fatalf("Unexpected sentinel master: ", r.SentinelMaster)
	}
	if r.InputBinding != "queue.input" {
		t.Fatalf("Unexpected input binding: ", r.InputBinding)
	}
	if r.OutputBinding != "queue.output" {
		t.Fatalf("Unexpected output binding: ", r.OutputBinding)
	}
}

func TestNewRedisTransportQueue(t *testing.T) {
	r := NewRedisTransport("", "", "queue:foo", "queue:bar")

	if r.InputBinding != "queue.foo" {
		t.Fatalf("Unexpected input binding: ", r.InputBinding)
	}
	if r.OutputBinding != "queue.bar" {
		t.Fatalf("Unexpected output binding: ", r.OutputBinding)
	}
}

func TestNewRedisTransportTopic(t *testing.T) {
	r := NewRedisTransport("", "", "topic:foo", "topic:bar")

	if r.InputBinding != "topic.foo" {
		t.Fatalf("Unexpected input binding: ", r.InputBinding)
	}
	if r.OutputBinding != "topic.bar" {
		t.Fatalf("Unexpected output binding: ", r.OutputBinding)
	}
}

func TestConnectToNotExistingRedis(t *testing.T) {
	redis := NewRedisTransport("doesnotexist:6379", "", "input", "ouput")
	err := redis.Connect()
	if err == nil { // expect an error
		log.Debugln("Error: ", err)
		t.Fatalf("Expected connection to fail but it passed.")
	}
}

func TestConnectToExistingSingleRedis(t *testing.T) {
	redis := NewRedisTransport(getRedisHost(), "", "input", "ouput")
	err := redis.Connect()
	if err != nil {
		t.Fatalf("Expected connection not established.")
	}
	redis.Disconnect()
}

func TestConnectToExistingSentinelRedis(t *testing.T) {
	redis := NewRedisTransport(getRedisSentinelHost(), getRedisMaster(), "input", "ouput")
	err := redis.Connect()
	if err != nil {
		t.Fatalf("Expected connection not established.")
	}
	redis.Disconnect()
}

func TestDisconnectWithoutConnecting(t *testing.T) {
	redis := NewRedisTransport(getRedisHost(), "", "input", "ouput")
	redis.Disconnect() // should not fail
}

func TestSendingAndReceiveWithQueueSingleHost(t *testing.T) {
	redis := NewRedisTransport(getRedisHost(), "", "queue:test-123", "queue:test-123")
	sendAndReceiveWithQueueInternal(t, redis)
}

func TestSendingAndReceiveWithQueueSentinel(t *testing.T) {
	redis := NewRedisTransport(getRedisSentinelHost(), getRedisMaster(), "queue:test-123", "queue:test-123")
	sendAndReceiveWithQueueInternal(t, redis)
}

func sendAndReceiveWithQueueInternal(t *testing.T, redis *RedisTransport) {
	redis.Connect()

	var wg sync.WaitGroup
	wg.Add(2)

	// receiver
	go func() {
		out := redis.Receive()
		msg := <-out
		if !reflect.DeepEqual(msg.Content, []byte("foobar")) {
			t.Fatalf("Message does not contain correct payload.")
		} else {
			log.Debugln("Received message: ", msg)
		}
		wg.Done()
	}()

	// sender
	go func() {
		time.Sleep(250 * time.Millisecond) // give the receiver some time to get ready

		// hack should only be one message
		redis.Send(api.NewTextMessage([]byte("foobar")))
		redis.Send(api.NewTextMessage([]byte("foobar")))

		wg.Done()
	}()

	wg.Wait()
	redis.Disconnect()
}

func TestSendingAndReceiveWithTopicSingleHost(t *testing.T) {
	redis := NewRedisTransport(getRedisHost(), "", "topic:test-123", "topic:test-123")
	sendReceiveWithTopicInternal(t, redis)
}

func TestSendingAndReceiveWithTopicSentinel(t *testing.T) {
	redis := NewRedisTransport(getRedisSentinelHost(), getRedisMaster(), "topic:test-123", "topic:test-123")
	sendReceiveWithTopicInternal(t, redis)
}

func sendReceiveWithTopicInternal(t *testing.T, redis *RedisTransport) {
	redis.Connect()

	var wg sync.WaitGroup
	wg.Add(3)

	// receiver 1
	go func() {
		subscribe(t, redis)
		wg.Done()
	}()

	// receiver 2
	go func() {
		subscribe(t, redis)
		wg.Done()
	}()

	// sender
	go func() {
		time.Sleep(250 * time.Millisecond) // give the receiver some time to get ready
		redis.Send(api.NewTextMessage([]byte("foobar")))
		wg.Done()
	}()

	wg.Wait()
	redis.Disconnect()
}

//
// Helper methods
//
func subscribe(t *testing.T, redis *RedisTransport) {
	out := redis.Receive()
	msg := <-out
	log.Debug("receiver2 message: ", msg)
	if !reflect.DeepEqual(msg.Content, []byte("foobar")) {
		t.Fatalf("Message does not contain correct payload.")
	} else {
		log.Debugln("msg.content: ", msg.Content)
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
		log.Infoln("Cannot find Redis at 'localhost:6379'.")
		log.Infoln("Please start a local redis or specify TEST_REDIS_HOST or TEST_REDIS_SENTINEL_HOST variable.")
		panic("Cannot find Redis server.")
	}
	return redisClient
}

func cleanupRedis(client *redis.Client) {
	if client != nil {
		defer client.Close()
	}
}
