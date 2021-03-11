[![Go Report Card](https://goreportcard.com/badge/github.com/TomWright/gracesarama)](https://goreportcard.com/report/github.com/TomWright/gracesarama)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/tomwright/gracesarama)](https://pkg.go.dev/github.com/tomwright/gracesarama)
![GitHub License](https://img.shields.io/github/license/TomWright/gracesarama)
![GitHub tag (latest by date)](https://img.shields.io/github/v/tag/TomWright/gracesarama?label=latest%20release)

# Grace Sarama Runners

Sarama runners for use with [grace](https://github.com/TomWright/grace).

## Consumer Usage

```go
package main

import (
	"context"
	"fmt"
	"github.com/tomwright/grace"
	"github.com/tomwright/gracesarama"
	"github.com/Shopify/sarama"
	"log"
)

func main() {
	g := grace.Init(context.Background())

	config := sarama.NewConfig()
	// Set kafka version.
	config.Version = sarama.V2_1_0_0

	runner := gracesarama.NewConsumerGroupRunner(
		[]string{"localhost:9092"},
		"my-group",
		config,
		[]string{"topic-a", "topic-b"},
		&exampleConsumerGroupHandler{},
	)
	runner.ErrorHandlerFn = func(err error) {
		log.Printf("ERROR: %s\n", err.Error())
	}

	g.Run(runner)

	g.Wait()
}

type exampleConsumerGroupHandler struct{}

func (exampleConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (exampleConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h exampleConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		sess.MarkMessage(msg, "")
	}
	return nil
}
```

## Producer Usage

```go
package main

import (
	"context"
	"github.com/tomwright/grace"
	"github.com/tomwright/gracesarama"
	"github.com/Shopify/sarama"
	"log"
)

func main() {
	g := grace.Init(context.Background())

	config := sarama.NewConfig()
	// Set kafka version.
	config.Version = sarama.V2_1_0_0
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true

	runner := gracesarama.NewProducerRunner([]string{"localhost:9092"}, config)
	runner.SuccessHandlerFn = func(message *sarama.ProducerMessage) {
		log.Printf("message published to topic: %s\n", message.Topic)
	}
	runner.ErrorHandlerFn = func(err *sarama.ProducerError) {
		log.Printf("failed to publish to topic: %s: %s\n", err.Msg.Topic, err.Err.Error())
	}

	produceCh := runner.Input()

	g.Run(runner)

	go func() {
		produceCh <- &sarama.ProducerMessage{
			// ...
		}
	}()

	g.Wait()
}
```
