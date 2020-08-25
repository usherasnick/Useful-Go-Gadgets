package kafka

import (
	"os"

	"github.com/Shopify/sarama"
	"github.com/rs/zerolog/log"
)

type Config struct {
	Brokers       []string `json:"brokers"`
	Topic         string   `json:"topic"`
	ConsumerGroup string   `json:"consumer_group"`
	FromOldest    bool     `json:"from_oldest"`
	ClientID      string   `json:"client_id"`
}

func NewConfig(cfg *Config) *sarama.Config {
	conf := sarama.NewConfig()
	if cfg.FromOldest {
		conf.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	conf.ClientID = cfg.ClientID
	GetKafkaAccessEnv(conf)
	return conf
}

func GetKafkaAccessEnv(cfg *sarama.Config) {
	usr := os.Getenv("KAFKA_USERNAME")
	pwd := os.Getenv("KAFKA_PASSWORD")
	if usr == "" || pwd == "" {
		log.Warn().Msg("access kafka without SASL setting")
		return
	}
	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	cfg.Net.SASL.User = usr
	cfg.Net.SASL.Password = pwd
	cfg.Net.SASL.Version = sarama.SASLHandshakeV1
}
