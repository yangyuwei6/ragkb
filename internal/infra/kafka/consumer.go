package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"ragkb/internal/config"
)

// Handler 处理单条消息。返回 error 表示处理失败（阶段 1 接入重试/DLQ）。
type Handler interface {
	Handle(ctx context.Context, msg kafka.Message) error
}

// Consumer 封装 kafka reader，按消费组拉取消息并交给 Handler 处理。
type Consumer struct {
	reader    *kafka.Reader
	dlqWriter *kafka.Writer
	handler   Handler
	logger    *zap.Logger
}

// NewConsumer 创建消费者。使用消费组以支持 worker 水平扩容。
func NewConsumer(cfg config.KafkaConfig, handler Handler, logger *zap.Logger) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: cfg.Brokers,
			Topic:   cfg.Topic,
			GroupID: cfg.GroupID,
		}),
		dlqWriter: &kafka.Writer{
			Addr:     kafka.TCP(cfg.Brokers...),
			Topic:    cfg.DLQTopic,
			Balancer: &kafka.LeastBytes{},
		},
		handler: handler,
		logger:  logger,
	}
}

// Run 持续消费直到 ctx 被取消。
// 手动提交位移：处理成功才 CommitMessages，保证 at-least-once。
func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			c.logger.Error("fetch kafka message failed", zap.Error(err))
			return err
		}

		if err := c.handleWithRetry(ctx, msg); err != nil {
			c.logger.Error("handle kafka message failed",
				zap.String("topic", msg.Topic),
				zap.Int("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
				zap.Error(err),
			)
			if err := c.publishDLQ(ctx, msg, err); err != nil {
				return fmt.Errorf("publish dlq: %w", err)
			}
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("commit kafka message failed", zap.Error(err))
		}
	}
}

// Close 关闭 reader。
func (c *Consumer) Close() error {
	if err := c.reader.Close(); err != nil {
		_ = c.dlqWriter.Close()
		return err
	}
	return c.dlqWriter.Close()
}

func (c *Consumer) handleWithRetry(ctx context.Context, msg kafka.Message) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if err := c.handler.Handle(ctx, msg); err != nil {
			lastErr = err
			c.logger.Warn("handle kafka message attempt failed",
				zap.Int("attempt", attempt+1),
				zap.String("topic", msg.Topic),
				zap.Int64("offset", msg.Offset),
				zap.Error(err),
			)
			if err := sleepRetry(ctx, attempt); err != nil {
				return err
			}
			continue
		}
		return nil
	}
	return lastErr
}

func (c *Consumer) publishDLQ(ctx context.Context, msg kafka.Message, cause error) error {
	payload, err := json.Marshal(map[string]any{
		"topic":     msg.Topic,
		"partition": msg.Partition,
		"offset":    msg.Offset,
		"key":       string(msg.Key),
		"value":     string(msg.Value),
		"error":     cause.Error(),
		"failed_at": time.Now().Format(time.RFC3339),
	})
	if err != nil {
		return err
	}
	return c.dlqWriter.WriteMessages(ctx, kafka.Message{
		Key:   msg.Key,
		Value: payload,
	})
}

func sleepRetry(ctx context.Context, attempt int) error {
	delay := time.Duration(500*(1<<attempt)) * time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
