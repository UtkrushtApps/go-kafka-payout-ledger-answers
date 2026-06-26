package consumer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"
	"github.com/utkrusht/go-kafka-payout-ledger/internal/events"
	"github.com/utkrusht/go-kafka-payout-ledger/internal/service"
)

type Reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

type DeadLetterPublisher interface {
	Publish(ctx context.Context, original kafka.Message, reason error) error
}

type Handler struct {
	service *service.PayoutService
	dlq     DeadLetterPublisher
	log     *slog.Logger
}

func NewHandler(svc *service.PayoutService, dlq DeadLetterPublisher, log *slog.Logger) *Handler {
	return &Handler{service: svc, dlq: dlq, log: log}
}

func (h *Handler) Handle(ctx context.Context, msg kafka.Message) error {
	ev, err := events.DecodeSettlement(msg.Value)
	if err != nil {
		attrs := kafkaAttrs(msg)
		attrs = append(attrs, slog.String("error", err.Error()))

		if events.IsInvalid(err) {
			if publishErr := h.dlq.Publish(ctx, msg, err); publishErr != nil {
				attrs = append(attrs, slog.String("dlq_error", publishErr.Error()))
				h.log.LogAttrs(ctx, slog.LevelError, "settlement decode failed and dead-letter publish failed", attrs...)
				return fmt.Errorf("dead-letter invalid settlement record: %w", publishErr)
			}

			h.log.LogAttrs(ctx, slog.LevelWarn, "settlement routed to dead letter", attrs...)
			return nil
		}

		h.log.LogAttrs(ctx, slog.LevelError, "settlement decode failed", attrs...)
		return err
	}

	if err := h.service.Process(ctx, ev); err != nil {
		attrs := kafkaAttrs(msg)
		attrs = append(attrs,
			slog.String("payment_id", ev.PaymentID),
			slog.String("merchant_id", ev.MerchantID),
			slog.String("error", err.Error()),
		)
		h.log.LogAttrs(ctx, slog.LevelError, "settlement processing failed", attrs...)
		return err
	}

	attrs := kafkaAttrs(msg)
	attrs = append(attrs,
		slog.String("payment_id", ev.PaymentID),
		slog.String("merchant_id", ev.MerchantID),
	)
	if ev.TraceID != "" {
		attrs = append(attrs, slog.String("trace_id", ev.TraceID))
	}
	h.log.LogAttrs(ctx, slog.LevelInfo, "settlement processed", attrs...)
	return nil
}

type Consumer struct {
	reader  Reader
	handler *Handler
	log     *slog.Logger
}

func New(reader Reader, handler *Handler, log *slog.Logger) *Consumer {
	return &Consumer{reader: reader, handler: handler, log: log}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch settlement message: %w", err)
		}

		if err := c.handler.Handle(ctx, msg); err != nil {
			attrs := kafkaAttrs(msg)
			attrs = append(attrs, slog.String("error", err.Error()))
			c.log.LogAttrs(ctx, slog.LevelError, "message handling failed; offset not committed", attrs...)
			return fmt.Errorf("handle settlement message topic=%s partition=%d offset=%d key=%q: %w", msg.Topic, msg.Partition, msg.Offset, string(msg.Key), err)
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			attrs := kafkaAttrs(msg)
			attrs = append(attrs, slog.String("error", err.Error()))
			c.log.LogAttrs(ctx, slog.LevelError, "message commit failed", attrs...)
			return fmt.Errorf("commit settlement message topic=%s partition=%d offset=%d key=%q: %w", msg.Topic, msg.Partition, msg.Offset, string(msg.Key), err)
		}
	}
}

func kafkaAttrs(msg kafka.Message) []slog.Attr {
	return []slog.Attr{
		slog.String("topic", msg.Topic),
		slog.Int("partition", msg.Partition),
		slog.Int64("offset", msg.Offset),
		slog.String("key", string(msg.Key)),
	}
}
