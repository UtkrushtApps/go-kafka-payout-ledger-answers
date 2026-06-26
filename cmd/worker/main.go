package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/utkrusht/go-kafka-payout-ledger/internal/config"
	"github.com/utkrusht/go-kafka-payout-ledger/internal/consumer"
	"github.com/utkrusht/go-kafka-payout-ledger/internal/logging"
	"github.com/utkrusht/go-kafka-payout-ledger/internal/producer"
	"github.com/utkrusht/go-kafka-payout-ledger/internal/service"
)

func main() {
	cfg := config.Local()
	log := logging.New()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	repo := service.NewMemoryRepository()
	payouts := service.NewPayoutService(repo)
	dlq := producer.NewDeadLetterProducer(cfg.Brokers, cfg.DeadLetterTopic)
	defer func() {
		if err := dlq.Close(); err != nil {
			log.Error("failed to close producer", slog.String("error", err.Error()))
		}
	}()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Brokers,
		GroupID:  cfg.GroupID,
		Topic:    cfg.SettlementTopic,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  500 * time.Millisecond,
		// Use explicit commits: the consumer commits only after a record has either
		// been processed successfully or intentionally routed to the DLQ.
		CommitInterval: 0,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Error("failed to close consumer", slog.String("error", err.Error()))
		}
	}()

	handler := consumer.NewHandler(payouts, dlq, log)
	worker := consumer.New(reader, handler, log)

	log.Info("consumer started", slog.String("topic", cfg.SettlementTopic), slog.String("group", cfg.GroupID))
	if err := worker.Run(ctx); err != nil && ctx.Err() == nil {
		log.Error("consumer stopped with error", slog.String("error", err.Error()))
		os.Exit(1)
	}
	log.Info("consumer stopped")
}
