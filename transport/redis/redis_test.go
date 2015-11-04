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

func TestNewRedisTransportEmpty(t *testing.T) {
	r := NewRedisTransport("", "", "")

	if r.Address != "localhost:6379" {
		t.Fatalf("Unexpected redis address: ", r.Address)
	}
	if r.InputBinding != "queue.input" {
		t.Fatalf("Unexpected input binding: ", r.InputBinding)
	}
	if r.OutputBinding != "queue.output" {
		t.Fatalf("Unexpected output binding: ", r.OutputBinding)
	}
}

func TestNewRedisTransportQueue(t *testing.T) {
	r := NewRedisTransport("", "queue:foo", "queue:bar")

	if r.InputBinding != "queue.foo" {
		t.Fatalf("Unexpected input binding: ", r.InputBinding)
	}
	if r.OutputBinding != "queue.bar" {
		t.Fatalf("Unexpected output binding: ", r.OutputBinding)
	}
}

func TestNewRedisTransportTopic(t *testing.T) {
	r := NewRedisTransport("", "topic:foo", "topic:bar")

	if r.InputBinding != "topic.foo" {
		t.Fatalf("Unexpected input binding: ", r.InputBinding)
	}
	if r.OutputBinding != "topic.bar" {
		t.Fatalf("Unexpected output binding: ", r.OutputBinding)
	}
}

func TestConnectToNotExistingRedis(t *testing.T) {
	redis := NewRedisTransport("doesnotexist:6379", "input", "ouput")
	err := redis.Connect()

	if err == nil { // expect an error
		log.Debugln("Error: ", err)
		t.Fatalf("Expected connection to fail but it passed.")
	}
}

func TestConnectToExistingRedis(t *testing.T) {
	redis := NewRedisTransport(getRedisHost(), "input", "ouput")
	err := redis.Connect()
	if err != nil {
		t.Fatalf("Expected connection not established.")
	}
	redis.Disconnect()
}

func TestDisconnectWithoutConnecting(t *testing.T) {
	redis := NewRedisTransport(getRedisHost(), "input", "ouput")
	redis.Disconnect() // should not fail
}

func TestSendingAndReceiveWithQueue(t *testing.T) {
	redis := NewRedisTransport(getRedisHost(), "queue:test-123", "queue:test-123")
	redis.Connect()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	// receiver
	go func() {
		out := redis.Receive()
		msg := <-out
		if !reflect.DeepEqual(msg.Content, []byte("foobar")) {
			t.Fatalf("Message does not contain correct payload.")
		} else {
			log.Debugln("Received message: ", msg)
			wg.Done()
		}
	}()

	// sender
	go func() {
		redis.Send(api.NewTextMessage([]byte("foobar")))
	}()

	wg.Wait()
	redis.Disconnect()
}

func TestSendingAndReceiveWithTopic(t *testing.T) {
	redis := NewRedisTransport(getRedisHost(), "topic:test-123", "topic:test-123")
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

// Redis is needed to run this test
func pingRedis() *redis.Client {
	redisClient, err := redis.Dial("tcp", getRedisHost())
	if err != nil {
		log.Infoln("Cannot find Redis at 'localhost:6379'.")
		log.Infoln("Please start a local redis or specify TEST_REDIS_HOST variable.")
		panic("Cannot find Redis server.")
	}

	return redisClient
}

func cleanupRedis(client *redis.Client) {
	if client != nil {
		defer client.Close()
	}
}
