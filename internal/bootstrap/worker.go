package bootstrap

import (
	"context"
	"errors"

	kafkalib "github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"ragkb/internal/config"
	"ragkb/internal/infra/kafka"
)

var errIngestHandlerNotImplemented = errors.New("worker ingest handler is not implemented")

// Worker 持有后台进程需要长期运行的资源。
type Worker struct {
	consumer *kafka.Consumer
	logger   *zap.Logger
}

// NewWorker 装配 worker 进程依赖。
// 当前只接入 Kafka consumer 骨架；具体摄取用例会在后续接入 service/ingest。
func NewWorker(cfg *config.Config, logger *zap.Logger) (*Worker, error) {
	handler := notImplementedHandler{logger: logger}
	consumer := kafka.NewConsumer(cfg.Kafka, handler, logger)

	return &Worker{
		consumer: consumer,
		logger:   logger,
	}, nil
}

// Run 启动 worker 主循环，并在 ctx 取消时退出。
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("worker consumer starting")
	return w.consumer.Run(ctx)
}

// Close 释放 worker 持有的外部资源。
func (w *Worker) Close() {
	if err := w.consumer.Close(); err != nil {
		w.logger.Error("close worker consumer failed", zap.Error(err))
	}
}

// notImplementedHandler 是 worker 消息处理的临时占位实现。
// 它返回错误以阻止未实现的摄取消息被静默当作成功处理。
type notImplementedHandler struct {
	logger *zap.Logger
}

func (h notImplementedHandler) Handle(ctx context.Context, msg kafkalib.Message) error {
	h.logger.Warn("worker message handler is not implemented",
		zap.String("topic", msg.Topic),
		zap.Int("partition", msg.Partition),
		zap.Int64("offset", msg.Offset),
	)
	return errIngestHandlerNotImplemented
}
