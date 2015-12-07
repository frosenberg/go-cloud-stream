# go-cloud-stream

[![Build Status](https://travis-ci.org/frosenberg/go-cloud-stream.svg?branch=master)](https://travis-ci.org/frosenberg/go-cloud-stream?branch=master)
[![Coverage Status](https://coveralls.io/repos/frosenberg/go-cloud-stream/badge.svg?branch=master&service=github)](https://coveralls.io/github/frosenberg/go-cloud-stream?branch=master)

The idea behind this work is to provide a simple way to build highly scalable messaging microservices.
These message microservices use different transports such as Kafka or Redis for communication.
The ideas is to deploy those microservices on Docker, Kubernetes or any other container system.

This work was inspired by the awesome [Spring Cloud Stream](https://github.com/spring-cloud/spring-cloud-stream)
project. However, streaming modules written in Java are pretty heavyweight, therefore we chose
to implement the base framework in Go because it is lightweight and blazingly fast.

Our key design goals are to be minimalist and lightweight but still compatible with the
[Spring Cloud Stream Modules](https://github.com/spring-cloud/spring-cloud-stream-modules)
in terms of the most important command line arguments so they can be used interchangeably.
This does not mean that go-cloud-stream supports all the options that Spring Cloud Stream supports.
However, the key aspects of a modules are supported (e.g., queue and topic bindings, Redis and Kafka
as transports, ...)

Combined with [Spring Cloud Dataflow ](https://github.com/spring-cloud/spring-cloud-dataflow) or your
own "composition" and deployment framework, this work can easily be used to build highly scalable
messaging microservices.

### Build & Test

This project is intended to be used as "library" to build messaging microservices on top of either
Kafka or Redis as transport layer. However, Go does not supported shared libraries that can be dynamically
linked, we ship the code as is (without main) and compiles is as part of so-called "Go Cloud Stream Modules".
See some concrete examples here: https://github.com/frosenberg/go-cloud-stream-modules.

If you want to test and extend the code, you have start a Redis, Kafka and Zookeeper as they are needed
in the tests.

#### Redis Transport

We support both, Redis and Redis Sentinel mode, so start both of them with:

```
$ redis-server
$ redis-sentinel etc/sentinel.conf
```

#### Kafka Transport

```
$ cd $KAFKA_HOME/bin/zookeeper-server-start.sh &
$ cd $KAFKA_HOME/bin/kafka-server-start.sh
```

#### Run the tests

```
$ go test -v ./...
```

### Build a go-cloud-stream module

See here for examples: https://github.com/frosenberg/go-cloud-stream-modules
