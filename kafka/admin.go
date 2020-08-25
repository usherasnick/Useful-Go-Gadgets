package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/rs/zerolog/log"
)

type Admin struct {
	conf  *Config
	admin sarama.ClusterAdmin
}

func NewAdmin(cfg *Config) *Admin {
	conf := NewConfig(cfg)
	// complete admin functions are supported from version 0.10.2.0
	conf.Version = sarama.V0_10_2_0
	admin, err := sarama.NewClusterAdmin(cfg.Brokers, conf)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create kafka cluster admin")
	}
	return &Admin{
		conf:  cfg,
		admin: admin,
	}
}

func (a *Admin) CreateTopic(topic string) error {
	return a.CreateTopicWithPartition(topic, 1, 1)
}

func (a *Admin) CreateTopicWithPartition(topic string, p int32, rf int16) error {
	if err := a.admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     p,
		ReplicationFactor: rf,
	}, false); err != nil {
		if terr, ok := err.(*sarama.TopicError); ok && terr.Err == sarama.ErrTopicAlreadyExists {
			return nil
		}
		return err
	}
	return nil
}

func (a *Admin) CreateTopics(topics []string) error {
	for _, topic := range topics {
		if err := a.CreateTopic(topic); err != nil {
			return err
		}
	}
	return nil
}

func (a *Admin) ListTopics() ([]string, error) {
	ret, err := a.admin.ListTopics()
	if err != nil {
		return nil, err
	}
	topics := make([]string, 0)
	for k := range ret {
		topics = append(topics, k)
	}
	return topics, nil
}

func (a *Admin) DeleteTopic(topic string) error {
	return a.admin.DeleteTopic(topic)
}

func (a *Admin) Close() {
	if a.admin != nil {
		if err := a.admin.Close(); err != nil {
			log.Error().Err(err).Msgf("failed to close kafka cluster admin")
		}
		a.admin = nil
	}
}
