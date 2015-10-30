package redis

import (
	"github.com/garyburd/redigo/redis"
	"strings"
	log "github.com/Sirupsen/logrus"
	"github.com/frosenberg/go-cloud-stream/api"
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
func NewRedisTransport() (*RedisTransport) {
	return &RedisTransport{
		Transport: api.Transport {InputBinding: "input", OutputBinding: "output"},
		Address: "localhost:6379",
		Timeout: 1,
		MaxConnections: 10,
	}
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
	status, err := t.Pool.Get().Do("RPUSH", t.OutputBinding, m.ToByteArray() )
	if err != nil {
		log.Errorf("Cannot LPUSH on queue '%v': %v (%v)\n", t.OutputBinding, err, status)
	} else {
		log.Debugf("Pushed '%s' to queue '%s'\n",  m.Content, t.OutputBinding)
	}
	return err
}

func (t *RedisTransport) ReceiveChan() <-chan api.Message {
	conn := t.Pool.Get()
	out := make(chan api.Message)

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
					out <- *api.NewMessageFromRawBytes( bytes[1].([]uint8) )
				}
			}
		}
	}()
	return out
}

//	psc := redis.PubSubConn{t.pool.Get()}
//	psc.Subscribe(t.outputName)

//	for {
//		switch v := psc.Receive().(type) {
//			case redis.Message:
//				fmt.Printf("%s: message: %s\n", v.Channel, v.Data)
//			case redis.Subscription:
//				fmt.Printf("%s: %s %d\n", v.Channel, v.Kind, v.Count)
//			case error:
//				fmt.Println("Error: %s", v)
//				// TODO return v
//			default:
//				fmt.Println("SAU")
//		}
//	}


func (t *RedisTransport) IsInputQueueSemantics() bool {
	return strings.HasPrefix(t.InputBinding, "queue:")
}

func (t *RedisTransport) IsOutputQueueSemantics() bool {
	return strings.HasPrefix(t.OutputBinding, "queue:")
}

func (t *RedisTransport) HasInputBinding() bool {
	return t.InputBinding != ""
}

func (t *RedisTransport) HasOutputBinding() bool {
	return t.OutputBinding != ""
}
