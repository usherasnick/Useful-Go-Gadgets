package kafka

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/rs/zerolog/log"
)

type ConsumerGroup struct {
	conf    *Config
	group   sarama.ConsumerGroup
	handler sarama.ConsumerGroupHandler
	done    chan struct{}
}

func NewConsumerGroup(cfg *Config, handler ProcessMessageFunc) *ConsumerGroup {
	conf := NewConfig(cfg)
	// consumer groups are supported from version 0.10.2.0
	conf.Version = sarama.V0_10_2_0
	conf.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	group, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.ConsumerGroup, conf)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create kafka consumer group")
	}
	return &ConsumerGroup{
		conf:    cfg,
		group:   group,
		handler: &SelfConsumerGroup{handler},
		done:    make(chan struct{}),
	}
}

func (cg *ConsumerGroup) Run() {
	defer log.Info().Msg("consumer group has been stopped")
	defer close(cg.done)

	log.Info().Msg("starting consumer group")
	ctx := context.Background()
	for {
		// consumer group has been closed, just return
		if cg.group == nil {
			return
		}
		if err := cg.group.Consume(ctx, []string{cg.conf.Topic}, cg.handler); err != nil {
			// consumer group has been closed, just return
			if err == sarama.ErrClosedConsumerGroup {
				return
			}
			log.Error().Err(err).Msg("failed to consume kafka")
		}
	}
}

func (cg *ConsumerGroup) Close() {
	if cg.group != nil {
		cg.group.Close() // nolint
	}
	<-cg.done
}

type SelfConsumerGroup struct {
	process ProcessMessageFunc
}

func (h *SelfConsumerGroup) Setup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *SelfConsumerGroup) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *SelfConsumerGroup) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if err := h.process(msg); err == nil {
			session.MarkMessage(msg, "")
		}
	}
	return nil
}
