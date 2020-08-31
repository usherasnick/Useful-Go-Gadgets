package predator

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rs/zerolog/log"
)

// DataIngressPredator ...
type DataIngressPredator struct {
	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once

	topic       string
	cgroup      sarama.ConsumerGroup
	handler     *predatorImpl
	concurrency int
	msgCh       chan *sarama.ConsumerMessage
}

// KafkaConsumerCfg kafka消费者端配置
type KafkaConsumerCfg struct {
	Brokers       []string `json:"brokers"`
	Topic         string   `json:"topic"`
	ConsumerGroup string   `json:"consumer_group"`
	FromOldest    bool     `json:"from_oldest"`
	Concurrency   int      `json:"concurrency"`
	CacaheSize    int      `json:"cacahe_size"`
	Version       string   `json:"version"`
}

func setKafkaAccess(cfg *sarama.Config) {
	usr := os.Getenv("KAFKA_USERNAME")
	pwd := os.Getenv("KAFKA_PASSWORD")
	if usr == "" || pwd == "" {
		log.Warn().Msg("access kafka without SASL settings")
		return
	}
	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	cfg.Net.SASL.User = usr
	cfg.Net.SASL.Password = pwd
	cfg.Net.SASL.Version = sarama.SASLHandshakeV1
}

// NewDataIngressPredator 返回DataIngressPredator实例.
func NewDataIngressPredator(cfg *KafkaConsumerCfg) *DataIngressPredator {
	var err error

	if cfg.Concurrency == 0 {
		cfg.Concurrency = 1
	}
	if cfg.CacaheSize == 0 {
		cfg.CacaheSize = 1024
	}

	kafkaCfg := sarama.NewConfig()
	kafkaCfg.Version, err = sarama.ParseKafkaVersion(cfg.Version)
	if err != nil {
		log.Fatal().Err(err).Msgf("failed to parse kafka version %s", cfg.Version)
	}
	setKafkaAccess(kafkaCfg)
	kafkaCfg.ClientID = "data-ingress-predator"
	kafkaCfg.Consumer.Return.Errors = true
	kafkaCfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	kafkaCfg.Consumer.Group.Session.Timeout = 20 * time.Second
	kafkaCfg.Consumer.Group.Heartbeat.Interval = 6 * time.Second
	kafkaCfg.Consumer.MaxProcessingTime = 500 * time.Millisecond
	if cfg.FromOldest {
		kafkaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	p := DataIngressPredator{
		topic:       cfg.Topic,
		concurrency: cfg.Concurrency,
		msgCh:       make(chan *sarama.ConsumerMessage, cfg.CacaheSize),
	}
	p.cgroup, err = sarama.NewConsumerGroup(cfg.Brokers, cfg.ConsumerGroup, kafkaCfg)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to setup new consumer group")
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.handler = &predatorImpl{predator: &p}

	return &p
}

// Start 开启消费通道消费数据.
func (p *DataIngressPredator) Start() {
	var wg sync.WaitGroup

	go func() {
		wg.Add(1)
	CONSUME_LOOP:
		for {
			select {
			case err, ok := <-p.cgroup.Errors():
				if !ok {
					log.Warn().Msg("consume loop is stopped")
					break CONSUME_LOOP
				}
				log.Error().Err(err).Msgf("failed to consume topic %s", p.topic)
			case <-p.ctx.Done():
				log.Warn().Msg("consume loop is stopped")
				break CONSUME_LOOP
			}
		}
		wg.Done()
	}()

	for i := 0; i < p.concurrency; i++ {
		wg.Add(1)
		go p.consumeMsg(i, &wg)
	}

	wg.Wait()
}

// Stop 关闭消费通道.
func (p *DataIngressPredator) Stop() {
	p.once.Do(func() {
		if p.cancel != nil {
			p.cancel()
		}
		close(p.msgCh)
		p.cgroup.Close()
	})
}

// Messages 用于读取消息数据.
func (p *DataIngressPredator) Messages() chan *sarama.ConsumerMessage {
	return p.msgCh
}

func (p *DataIngressPredator) consumeMsg(id int, wg *sync.WaitGroup) {
	defer wg.Done()

CONSUME_LOOP:
	for {
		if err := p.cgroup.Consume(p.ctx, []string{p.topic}, p.handler); err != nil {
			log.Panic().Err(err).Msgf("failed to consume topic %s on consume goroutine-%d", p.topic, id)
		}

		select {
		case <-p.ctx.Done():
			log.Warn().Msgf("consume loop on consume goroutine-%d is stopped", id)
			break CONSUME_LOOP
		default:
			log.Info().Msgf("consumer group rebalances for consume goroutine-%d", id)
		}
	}
}

func (p *DataIngressPredator) cacheMsg(msg *sarama.ConsumerMessage) {
	p.msgCh <- msg
}

type predatorImpl struct {
	predator *DataIngressPredator
}

// Setup ...
func (handler *predatorImpl) Setup(session sarama.ConsumerGroupSession) error {
	log.Debug().Msgf("setup consumer: %v", session.Claims())
	return nil
}

// Cleanup ...
func (handler *predatorImpl) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Debug().Msgf("cleanup consumer: %v", session.Claims())
	return nil
}

func (handler *predatorImpl) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
CONSUME_LOOP:
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				log.Warn().Msg("consume channel is closed")
				break CONSUME_LOOP
			}
			handler.predator.cacheMsg(msg)
			session.MarkMessage(msg, "")
		case <-handler.predator.ctx.Done():
			log.Warn().Msg("consume channel is closed")
			break CONSUME_LOOP
		}
	}
	return nil
}
