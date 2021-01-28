package gracesarama

import (
	"context"
	"gopkg.in/Shopify/sarama.v1"
	"sync"
)

// NewConsumerGroupRunner returns a runner that controls the start-up and graceful shutdown of a consumer group.
func NewConsumerGroupRunner(
	addrs []string, groupID string, config *sarama.Config,
	topics []string,
	handler sarama.ConsumerGroupHandler,
) *ConsumerGroupRunner {
	config.Consumer.Return.Errors = true
	return &ConsumerGroupRunner{
		addrs:              addrs,
		groupID:            groupID,
		config:             config,
		consumerGroup:      nil,
		topics:             topics,
		handler:            handler,
		shutdownFinishedCh: nil,
	}
}

// ConsumerGroupRunner is used to run and gracefully shutdown a consumer group.
type ConsumerGroupRunner struct {
	addrs              []string
	groupID            string
	config             *sarama.Config
	consumerGroup      sarama.ConsumerGroup
	topics             []string
	handler            sarama.ConsumerGroupHandler
	shutdownFinishedCh chan struct{}

	// ErrorHandlerFn handles any errors found while consuming.
	// If it is nil errors are ignored.
	ErrorHandlerFn func(err error)
}

// Run starts up the consumer group.
func (cgr *ConsumerGroupRunner) Run(ctx context.Context) error {
	group, err := sarama.NewConsumerGroup(cgr.addrs, cgr.groupID, cgr.config)
	if err != nil {
		return err
	}
	cgr.consumerGroup = group

	go cgr.handleCtxDone(ctx)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go cgr.handleErrors(ctx, wg)

consumeLoop:
	for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		if err := cgr.consumerGroup.Consume(ctx, cgr.topics, cgr.handler); err != nil {
			if err == sarama.ErrClosedConsumerGroup {
				// Wait for consumerGroup.Close() to return before exiting.
				if cgr.shutdownFinishedCh != nil {
					<-cgr.shutdownFinishedCh
				}
				break consumeLoop
			}

			return err
		}
	}

	wg.Wait()
	return nil
}

func (cgr *ConsumerGroupRunner) handleCtxDone(ctx context.Context) {
	<-ctx.Done()
	if cgr.consumerGroup != nil {
		cgr.shutdownFinishedCh = make(chan struct{})
		cgr.consumerGroup.Close()
		close(cgr.shutdownFinishedCh)
	}
}

func (cgr *ConsumerGroupRunner) handleErrors(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for err := range cgr.consumerGroup.Errors() {
		if cgr.ErrorHandlerFn != nil {
			cgr.ErrorHandlerFn(err)
		}
	}
}
