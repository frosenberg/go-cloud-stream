# go-cloud-stream

The idea behind this work is to provide a simple way to build highly scalable messaging microservices.
These message microservices use different transports such as Kafka or Redis for communication.
The ideas is to deploy those microservices on Docker, Kubernetes or any other container system.

This work was inspired by the awesome [Spring Cloud Stream](https://github.com/spring-cloud/spring-cloud-stream)
project. However, streaming modules written in Java are pretty heavyweight, therefore we chose
to implement the base framework in Go because it is lightweight and blazingly fast.

Our key design goals are to be minimalist and lightweight but still compatible with the
[Spring Cloud Steam Modules](https://github.com/spring-cloud/spring-cloud-stream-modules)
in terms of the most important command line arguments so they can be used interchangeably.
This does not mean that go-cloud-stream supports all the options that Spring Cloud Stream supports.
However, the key aspects of a modules are supported (e.g., queue and topic bindings, Redis and Kafka
as transports, ...)

Combined with [Spring Cloud Dataflow ](https://github.com/spring-cloud/spring-cloud-dataflow) or your
own "composition" an deployment framework, this work can easily be used to build highly scalable
messaging microservices.

### Build

```
$ redis-server
$ go test *.go
```

### Build a go-cloud-stream module

TBD
