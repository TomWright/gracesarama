package gracesarama

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
)

// NewProducerRunner returns a producer runner that can publish messages async through channels.
func NewProducerRunner(addrs []string, config *sarama.Config) *ProducerRunner {
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	runner := &ProducerRunner{
		addrs:  addrs,
		config: config,
		input:  make(chan *sarama.ProducerMessage),
	}
	return runner
}

// ProducerRunner is used publish messages async through channels.
type ProducerRunner struct {
	addrs  []string
	config *sarama.Config

	// producer is the producer we'll use to produce.
	producer sarama.SyncProducer

	input chan *sarama.ProducerMessage

	// ErrorHandlerFn is used to handle errors when producing messages.
	// If this is null, errors are ignored.
	ErrorHandlerFn func(err *sarama.ProducerError)
	// SuccessHandlerFn is used to handle successfully produced messages.
	// If this is null, it is ignored.
	SuccessHandlerFn func(message *sarama.ProducerMessage)
}

func (cgr *ProducerRunner) Input() chan<- *sarama.ProducerMessage {
	return cgr.input
}

// Run starts up the producer.
func (cgr *ProducerRunner) Run(ctx context.Context) error {
	producer, err := sarama.NewSyncProducer(cgr.addrs, cgr.config)
	if err != nil {
		return fmt.Errorf("could not create new sync producer: %w", err)
	}
	cgr.producer = producer

	go cgr.handleCtxDone(ctx)

	// Read all input until the input chan is closed.
	for msg := range cgr.input {
		_, _, err := cgr.producer.SendMessage(msg)
		if err != nil {
			if cgr.ErrorHandlerFn != nil {
				cgr.ErrorHandlerFn(&sarama.ProducerError{
					Msg: msg,
					Err: err,
				})
			}
			continue
		}
		if cgr.SuccessHandlerFn != nil {
			cgr.SuccessHandlerFn(msg)
		}
	}

	// Input chan is closed which means a graceful shutdown is happening.
	if err := cgr.producer.Close(); err != nil {
		return err
	}

	return nil
}

func (cgr *ProducerRunner) handleCtxDone(ctx context.Context) {
	<-ctx.Done()
	if cgr.input != nil {
		close(cgr.input)
	}
}
