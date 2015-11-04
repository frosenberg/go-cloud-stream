package redis

import (
	"testing"
	log "github.com/Sirupsen/logrus"
	"github.com/mediocregopher/radix.v2/redis"
	"os"
	"reflect"
	"sync"
	"github.com/frosenberg/go-cloud-stream/api"
)

func init() {
	log.SetLevel(log.DebugLevel)
	client := pingRedis()
	defer cleanupRedis(client)
}

func TestConnectToNotExistingRedis(t *testing.T) {
	redis := NewRedisTransport("doesnotexist:6379", "input", "ouput")
	err := redis.Connect()

	if err == nil  { // expect an error
		log.Debugln("Error: ", err)
		t.Fatalf("Expected connection to fail but it passed.")
	}
}

func TestConnectToExistingRedis(t *testing.T) {
	redis := NewRedisTransport(getRedisHost(), "input", "ouput")
	err := redis.Connect()
	if err != nil  {
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
	wg.Add(2)

	// receiver
	go func() {
		out := redis.Receive()
		msg := <-out
		log.Debug("Got message: ", msg)
		if !reflect.DeepEqual(msg.Content, []byte("foobar")) {
			t.Fatalf("Message does not contain correct payload.")
		} else {
			wg.Done()
		}
	}()

	// sender
	go func() {
		redis.Send(api.NewTextMessage([]byte("foobar")))
		wg.Done()
	}()

	wg.Wait()
	redis.Disconnect()
}

func TestSendingAndReceiveWithTopic(t *testing.T) {
	redis := NewRedisTransport(getRedisHost(), "topic:test-123", "topic:test-123")
	redis.Connect()

	wg := &sync.WaitGroup{}
	wg.Add(3)


	// sender
	go func() {
		redis.Send(api.NewTextMessage([]byte("foobar")))
		wg.Done()
	}()

	// receiver 1
	go func() {
		out := redis.Receive()
		msg := <-out
		log.Debug("receiver1 message: ", msg)
		if !reflect.DeepEqual(msg.Content, []byte("foobar")) {
			t.Fatalf("Message does not contain correct payload.")
		} else {
			log.Debugln("msg.content: ", msg.Content)
			wg.Done()
		}
	}()

	// receiver 2
	go func() {
		out := redis.Receive()
		msg := <-out
		log.Debug("receiver2 message: ", msg)
		if !reflect.DeepEqual(msg.Content, []byte("foobar")) {
			t.Fatalf("Message does not contain correct payload.")
		} else {
			log.Debugln("msg.content: ", msg.Content)
			wg.Done()
		}
	}()

	wg.Wait()
	redis.Disconnect()
}


//
// Helper methods
//

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
