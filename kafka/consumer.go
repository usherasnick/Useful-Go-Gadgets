package kafka

import (
	"os"

	"github.com/Shopify/sarama"
	"github.com/rs/zerolog/log"
)

type ProcessMessageFunc func(*sarama.ConsumerMessage) error

type Consumer struct {
	conf      *Config // nolint
	consumer  sarama.Consumer
	partition int32
	pc        sarama.PartitionConsumer
	handler   ProcessMessageFunc
}

func NewConsumer(cfg *Config, handler ProcessMessageFunc, partition int32, offset int64) *Consumer {
	conf := sarama.NewConfig()
	client, err := sarama.NewClient(cfg.Brokers, conf)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to kafka")
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create kafka consumer")
	}

	if offset < 0 {
		if os.Getenv("CONSUME_NOW") == "1" {
			log.Warn().Msgf("consume partition %d from oldest...", partition)
			offset = sarama.OffsetOldest
		} else {
			log.Info().Msgf("consume partition %d from newest...", partition)
			offset = sarama.OffsetNewest
		}
	}

	pc, err := consumer.ConsumePartition(cfg.Topic, partition, offset)
	if err != nil {
		if perr, ok := err.(*sarama.ConsumerError); ok && perr.Err == sarama.ErrOffsetOutOfRange {
			if os.Getenv("CONSUME_NOW") == "1" {
				offset = sarama.OffsetOldest
			} else {
				offset = sarama.OffsetNewest
			}
		} else {
			log.Fatal().Err(err).Msgf("failed to create kafka consumer, partition: %v, offset: %v", partition, offset)
		}
	}

	log.Info().Msgf("create kafka consumer, partition: %v, offset: %v", partition, offset)

	return &Consumer{
		consumer:  consumer,
		partition: partition,
		pc:        pc,
		handler:   handler,
	}
}

func (c *Consumer) Run() {
	log.Info().Msgf("consumer is running, partition: %v", c.partition)
	for msg := range c.pc.Messages() {
		if err := c.handler(msg); err != nil {
			log.Warn().Err(err).Msg("failed to process message")
		}
	}
	log.Info().Msgf("consumer has been closed, partition: %v", c.partition)
}

func (c *Consumer) Close() {
	if err := c.pc.Close(); err != nil {
		log.Warn().Err(err).Msgf("failed to close consumer, partition: %v", c.partition)
	}
	if err := c.consumer.Close(); err != nil {
		log.Warn().Err(err).Msg("failed to close consumer")
	}
}
