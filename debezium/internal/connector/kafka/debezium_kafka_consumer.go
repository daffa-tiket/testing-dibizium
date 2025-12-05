package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/internal/config"
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/internal/connector/embedded"
)

type DebeziumKafkaConsumer struct {
	consumer   sarama.ConsumerGroup
	runner     *embedded.EmbeddedConnectorRunner
	topic      string
	pollMillis time.Duration
	cfg        *config.DebeziumConfig
}

func NewDebeziumKafkaConsumer(cfg *config.DebeziumConfig, runner *embedded.EmbeddedConnectorRunner) *DebeziumKafkaConsumer {
	consumerGroup, err := sarama.NewConsumerGroup([]string{cfg.KafkaConfig.Bootstrap}, cfg.KafkaConfig.GroupID, nil)
	if err != nil {
		panic(err)
	}
	return &DebeziumKafkaConsumer{
		consumer:   consumerGroup,
		runner:     runner,
		topic:      cfg.KafkaConfig.Topic,
		pollMillis: time.Millisecond * time.Duration(cfg.KafkaConfig.PollIntervalMs),
		cfg:        cfg,
	}
}

func (c *DebeziumKafkaConsumer) StartPolling() {
	ctx := context.Background()
	for {
		handler := &consumerGroupHandler{runner: c.runner}
		if err := c.consumer.Consume(ctx, []string{c.topic}, handler); err != nil {
			fmt.Println("kafka_consumer_failed:", err)
		}
		time.Sleep(c.pollMillis)
	}
}

type consumerGroupHandler struct {
	runner *embedded.EmbeddedConnectorRunner
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Println("kafka_consumer_received")
		h.runner.HandleChangeFromJSON(string(msg.Value))
		fmt.Println("kafka_consumer_processed")
		sess.MarkMessage(msg, "")
	}
	return nil
}
