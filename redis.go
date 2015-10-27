package main

import (
	"fmt"
	"flag"
	"github.com/garyburd/redigo/redis"
)

// TODO this needs to be integrated into connect
var (
	redisAddress = flag.String("redis-address", ":6379", "Address to the Redis server")
	outputQueue = flag.String("output-queue", "ticktock", "Redis queue to push ticks to")
	maxConnections = flag.Int("max-connections", 10, "Max connections to Redis")
)

//
// Basic types
//

type RedisTransportProperties struct {
	TransportProperties
	address string;
	pool *redis.Pool
}


func (t *RedisTransportProperties) Connect() {
	fmt.Println("Connecting to Redis: ", t.address)
	fmt.Println("Connect to queue: ", t.inputName)

	flag.Parse()

	redisConn := redis.NewPool(func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", *redisAddress)

		if err != nil {
			return nil, err
		}

		return c, err
	}, *maxConnections)

	fmt.Println("Redis conn: ", redisConn)

	t.pool = redisConn;
	t.name = fmt.Sprint("queue.", *outputQueue)
	fmt.Printf("QueueName: %s\n", t.name)
}

func (t *RedisTransportProperties) Disconnect() {
	fmt.Println("Disconnecting to Redis: ", t.address)
	defer t.pool.Close();
}


func (t *RedisTransportProperties) Send(payload []byte) {
	fmt.Println("Send to ", t.outputName)
		status, err := t.pool.Get().Do("LPUSH", t.name, new(Converter).ConvertMessage(payload) )
		if err != nil {
			fmt.Printf("Cannot LPUSH on queue '%v': %v (%v)\n", t.name, err, status)
		} else {
			fmt.Printf("Pushed '%s' to queue '%s'\n", payload, t.name)
		}
}

func (ch *RedisTransportProperties) Receive(payload []byte) {
	fmt.Println("TODO: read from redis and get notification")
}
