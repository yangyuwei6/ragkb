package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"

	"ragkb/internal/config"
)

// Producer 封装 kafka writer，用于投递摄取任务消息。
type Producer struct {
	writer *kafka.Writer
}

// NewProducer 创建一个面向指定 topic 的生产者。
func NewProducer(cfg config.KafkaConfig) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(cfg.Brokers...),
			Topic:    cfg.Topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

// Publish 投递一条消息。key 用于分区（同一文件的消息落同一分区，保证顺序）。
func (p *Producer) Publish(ctx context.Context, key, value []byte) error {
	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: value,
	}); err != nil {
		return fmt.Errorf("write kafka message: %w", err)
	}
	return nil
}

// Close 关闭 writer，刷出缓冲中的消息。
func (p *Producer) Close() error {
	return p.writer.Close()
}
