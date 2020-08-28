package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/rs/zerolog/log"
)

type Producer struct {
	conf     *Config
	producer sarama.AsyncProducer
}

func NewProducer(cfg *Config) *Producer {
	conf := NewConfig(cfg)
	producer, err := sarama.NewAsyncProducer(cfg.Brokers, conf)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create kafka producer")
	}
	return &Producer{
		conf:     cfg,
		producer: producer,
	}
}

func (p *Producer) PublishToDefaultTopic(bytes []byte) error {
	return p.publish(p.conf.Topic, "", -1, bytes)
}

func (p *Producer) Publish(topic string, bytes []byte) error {
	return p.publish(topic, "", -1, bytes)
}

func (p *Producer) PublishByKey(topic, key string, bytes []byte) error {
	return p.publish(topic, key, -1, bytes)
}

func (p *Producer) PublishByPartition(topic string, partition int32, bytes []byte) error {
	return p.publish(topic, "", partition, bytes)
}

func (p *Producer) publish(topic, key string, partition int32, bytes []byte) error {
	if p.producer == nil {
		log.Warn().Msg("producer has been closed")
		return nil
	}

	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(bytes),
	}
	if partition >= 0 {
		message.Partition = partition
	}
	if len(key) > 0 {
		message.Key = sarama.StringEncoder(key)
	}

	select {
	case p.producer.Input() <- message:
		return nil
	case err := <-p.producer.Errors():
		return err
	}
}

func (p *Producer) Close() {
	if p.producer != nil {
		if err := p.producer.Close(); err != nil {
			log.Error().Err(err).Msg("failed to close kafka producer")
		}
		p.producer = nil
	}
}
