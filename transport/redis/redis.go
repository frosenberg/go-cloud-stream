package redis

import (
	"github.com/garyburd/redigo/redis"
	"strings"
	log "github.com/Sirupsen/logrus"
	"github.com/frosenberg/go-cloud-stream/api"
	"fmt"
)

//
// Basic Redis transport information
//
type RedisTransport struct {
	api.Transport
	Address string;

	// Timeout for blocking receives in seconds
	Timeout int;
	MaxConnections int;
	Pool *redis.Pool
}

// Creates a new RedisTransport instance with
// sensible default values.
func NewRedisTransport(address string, inputBinding string, outputBinding string) (*RedisTransport) {

	// set some reasonable defaults
	if address == "" {
		address = "localhost:6379"
	}
	if inputBinding == "" {
		inputBinding = "input"
	}
	if outputBinding == "" {
		outputBinding = "output"
	}

	transport := &RedisTransport{
		Transport: api.Transport {	InputBinding: prefix(inputBinding),
			 						OutputBinding: prefix(outputBinding) },
		Address: address,
		Timeout: 1, // TODO parameterize via CLI?
		MaxConnections: 10, // TODO parameterize via CLI?
	}
	return transport
}

func (t *RedisTransport) Connect() (err error) {
	log.Debugln("Connecting to Redis: ", t.Address)

	// create redis pool
	redisConn := redis.NewPool(func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", t.Address)

		if err != nil {
			return nil, err
		}

		return c, err
	}, t.MaxConnections)

	t.Pool = redisConn;
	return err
}

func (t *RedisTransport) Disconnect() (err error) {
	log.Debugln("Disconnecting from Redis: ", t.Address)
	defer t.Pool.Close();
	return nil
}

func (t *RedisTransport) Send(m *api.Message) (err error) {
	conn := t.Pool.Get()

	if t.isOutputTopicSemantics() {
		status, err := conn.Do("PUBLISH", t.OutputBinding, m.ToByteArray())
		if err != nil {
			log.Errorf("Cannot PUBLISH on queue '%v': %v (%v)\n", t.OutputBinding, err, status)
		} else {
			log.Debugf("Published '%s' to topic '%s'\n", m.Content, t.OutputBinding)
		}
	} else {
		status, err := conn.Do("RPUSH", t.OutputBinding, m.ToByteArray())
		if err != nil {
			log.Errorf("Cannot RPUSH on queue '%v': %v (%v)\n", t.OutputBinding, err, status)
		} else {
			log.Debugf("Pushed '%s' to queue '%s'\n", m.Content, t.OutputBinding)
		}

	}
	return err
}

func (t *RedisTransport) Receive() <-chan api.Message {
	conn := t.Pool.Get()
	out := make(chan api.Message)

	if t.isInputTopicSemantics() { // topic processing
		psc := redis.PubSubConn{conn}
		psc.Subscribe(t.InputBinding)

		go func() {
			for {
				switch v := psc.Receive().(type) {
				case redis.Message:
					out<- *api.NewMessageFromRawBytes(v.Data)
				case error:
					fmt.Println("Error: %s", v)
					// TODO return v
				}
			}
		}()
	} else { // queue processing

		go func() {
			for {
				value, err := conn.Do("BRPOP", t.InputBinding, 0)
				if err != nil {
					log.Errorf("Cannot RPOP on '%v': %v (%v)\n", t.InputBinding, err, value)
				}
				if value != nil {
					// convert interface{} to byte[]
					bytes, ok := value.([]interface{})
					if ok {
						out <- *api.NewMessageFromRawBytes(bytes[1].([]uint8))
					}
				}
			}
		}()
	}
	return out
}

func (t *RedisTransport) isInputQueueSemantics() bool {
	return strings.HasPrefix(t.InputBinding, "queue.")
}

func (t *RedisTransport) isOutputQueueSemantics() bool {
	return strings.HasPrefix(t.OutputBinding, "queue.")
}

func (t *RedisTransport) isOutputTopicSemantics() bool {
	return strings.HasPrefix(t.OutputBinding, "topic.")
}

func (t *RedisTransport) isInputTopicSemantics() bool {
	return strings.HasPrefix(t.InputBinding, "topic.")
}


// Set the prefix of a binding correctly as it is
// expected by the underlying transformer.

func prefix(binding string) string {
	if strings.HasPrefix(binding, "topic:") {
		return strings.Replace(binding, "topic:", "topic.", 1)
	}

	if strings.HasPrefix(binding, "queue:") {
		return strings.Replace(binding, "queue:", "queue.", 1)
	} else {
		return fmt.Sprintf("queue.%s", binding)
	}
}
