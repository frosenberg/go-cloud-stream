package redis

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/frosenberg/go-cloud-stream/api"
	"github.com/frosenberg/go-cloud-stream/transport"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/pubsub"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/mediocregopher/radix.v2/sentinel"
	"regexp"
)

//
// Basic Redis transport information
//
type RedisTransport struct {
	api.Transport
	Address        string
	SentinelMaster string

	// Timeout for blocking receives in seconds
	Timeout int

	MaxConnections int

	pool           *pool.Pool
	sentinelClient *sentinel.Client
}

// Creates a new RedisTransport instance with sensible default values.
// Either address or sentinel address (and also set sentinelMaster) has to be set.
func NewRedisTransport(address string, sentinelMaster string, inputBinding string, outputBinding string) *RedisTransport {

	// set some reasonable defaults
	if address == "" || address == ":6379" {
		address = "localhost:6379"
	} else { // check if it has a port

		match, _ := regexp.MatchString("^.+:\\d+$", address)
		if !match {
			log.Debugf("Appending default redis port :6379 to %s", address)
			address = address + ":6379"
		}
	}
	if inputBinding == "" {
		inputBinding = "input"
	}
	if outputBinding == "" {
		outputBinding = "output"
	}

	transport := &RedisTransport{
		Transport: api.Transport{
			InputBinding:   transport.Prefix(inputBinding),
			InputSemantic:  transport.GetBindingSemantic(inputBinding),
			OutputBinding:  transport.Prefix(outputBinding),
			OutputSemantic: transport.GetBindingSemantic(outputBinding),
		},
		Address:        address,
		SentinelMaster: sentinelMaster,
		Timeout:        1,   // TODO parameterize via CLI?
		MaxConnections: 100, // TODO parameterize via CLI?
	}
	return transport
}

func (t *RedisTransport) Connect() error {

	// TODO add retries in case of failures

	if t.isSentinel() {

		client, err := sentinel.NewClient("tcp", t.Address, t.MaxConnections, t.SentinelMaster)
		if err != nil {
			msg := fmt.Sprintf("Cannot connect to Redis host '%s': %s", t.Address, err)
			log.Errorln(msg)
			return err
		}
		t.sentinelClient = client

	} else { // Redis standalone

		// create redis pool
		pool, err := pool.New("tcp", t.Address, t.MaxConnections)
		if err != nil {
			msg := fmt.Sprintf("Cannot connect to Redis host '%s': %s", t.Address, err)
			log.Errorln(msg)
			return err
		}
		t.pool = pool
	}

	// ping to ensure we are really connected
	conn, err := t.getConnection()
	defer conn.Close()

	if err != nil {
		log.Fatalf("Cannot ping to Redis host: %s", err.Error())
		return err
	}
	t.pingRedis(conn)

	return nil
}

func (t *RedisTransport) RunSource(sourceFunc api.Source) {
	log.Debugf("RunSource called ...")

	channel := make(chan *api.Message)

	if t.OutputSemantic == api.TopicSemantic {
		go t.publish(channel)
	} else {
		go t.push(channel)
	}

	sourceFunc(channel)
}

func (t *RedisTransport) RunSink(sinkFunc api.Sink) {

	channel := make(chan *api.Message)

	if t.InputSemantic == api.TopicSemantic {
		go t.subscribe(channel) // topic processing
	} else {
		go t.pop(channel) // queue processing
	}

	sinkFunc(channel)
}

func (t *RedisTransport) RunProcessor(processorFunc api.Processor) {

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

// Disconnects from the Redis transport. It does not fai
// if you are not connected.
func (t *RedisTransport) Disconnect() {
	log.Debugln("Disconnecting from Redis: ", t.Address)

	// nothing to do for now
}

func (t *RedisTransport) publish(channel chan *api.Message) {
	conn, _ := t.getConnection()
	defer conn.Close()

	for m := range channel {
		resp := conn.Cmd("PUBLISH", t.OutputBinding, m.ToRawByteArray())
		log.Debugln("resp (publish): ", resp)
		if resp.Err != nil {
			log.Errorf("Cannot PUBLISH on queue '%v': %v", t.OutputBinding, resp.Err)
		} else {
			log.Debugf("Published '%s' to topic '%s'\n", m.Content, t.OutputBinding)
		}
	}
}

func (t *RedisTransport) subscribe(channel chan *api.Message) {
	conn, _ := t.getConnection() // todo handle messages better
	defer conn.Close()

	psc := pubsub.NewSubClient(conn)
	subClient := psc.Subscribe(t.InputBinding)
	log.Debugf("Processor subscribing to: %s", subClient)
	defer psc.Unsubscribe(t.InputBinding)

	for {
		resp := psc.Receive()
		//log.Debugln("after: ", resp)

		if resp.Err != nil {
			channel <- api.NewMessageFromRawBytes([]byte(resp.Err.Error()))
		} else {
			channel <- api.NewMessageFromRawBytes([]byte(resp.Message))
		}
	}
}

func (t *RedisTransport) push(channel <-chan *api.Message) {
	conn, _ := t.getConnection()
	defer conn.Close()

	for m := range channel {
		resp := conn.Cmd("RPUSH", t.OutputBinding, m.ToRawByteArray())
		//log.Debugln("resp (RPUSH): ", resp)
		if resp.Err != nil {
			log.Errorf("Cannot RPUSH on queue '%v': %v", t.OutputBinding, resp.Err)
		} else {
			log.Debugf("Pushed '%s' to queue '%s'\n", m.Content, t.OutputBinding)
		}
	}
}

func (t *RedisTransport) pop(channel chan *api.Message) {
	conn, _ := t.getConnection()
	defer conn.Close()

	for {
		content, err := conn.Cmd("BRPOP", t.InputBinding, 0).List()
		if err != nil {
			log.Errorf("Cannot BRPOP on '%v': %v", t.InputBinding, err)
		} else {
			channel <- api.NewMessageFromRawBytes([]byte(content[1]))
		}
	}
}

// Pings redis to check whether the connection works. A connectTo...() methods needs
// to be called before.
func (t *RedisTransport) pingRedis(client *redis.Client) error {

	resp := client.Cmd("PING")
	if resp.Err != nil {
		msg := fmt.Sprintf("Cannot connect to Redis host '%s': %s", t.Address, resp.Err)
		log.Fatal(msg)
		return resp.Err
	}

	return nil
}

func (t *RedisTransport) getConnection() (*redis.Client, error) {
	if t.isSentinel() {

		conn, err := t.sentinelClient.GetMaster(t.SentinelMaster)
		if err != nil {
			return nil, err
		}
		defer t.sentinelClient.PutMaster(t.SentinelMaster, conn)
		return conn, nil

	} else {

		conn, err := t.pool.Get()
		defer t.pool.Put(conn)

		if err != nil {
			return nil, err
		}

		t.pingRedis(conn)
		return conn, nil
	}
}

func (t *RedisTransport) isSentinel() bool {
	return t.SentinelMaster != ""
}

func (t *RedisTransport) isSingleRedis() bool {
	return t.SentinelMaster == ""
}
