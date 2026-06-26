package consumer_test

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/utkrusht/go-kafka-payout-ledger/internal/consumer"
	"github.com/utkrusht/go-kafka-payout-ledger/internal/service"
)

type capturedDLQ struct {
	messages []kafka.Message
	reasons  []string
}

func (d *capturedDLQ) Publish(ctx context.Context, original kafka.Message, reason error) error {
	d.messages = append(d.messages, original)
	d.reasons = append(d.reasons, reason.Error())
	return nil
}

func TestHandlerDoesNotCreateDuplicatePayoutOnReplay(t *testing.T) {
	repo := service.NewMemoryRepository()
	svc := service.NewPayoutService(repo)
	dlq := &capturedDLQ{}
	h := consumer.NewHandler(svc, dlq, slog.New(slog.NewTextHandler(io.Discard, nil)))
	msg := kafka.Message{
		Topic:     "payment-settled.v1",
		Partition: 1,
		Offset:    42,
		Key:       []byte("merchant-7"),
		Value:     []byte(`{"payment_id":"pay-replay-1","merchant_id":"merchant-7","amount_cents":7500,"currency":"USD","settled_at":"2026-06-26T10:00:00Z","trace_id":"trace-replay"}`),
	}

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("first delivery failed: %v", err)
	}
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("second delivery failed: %v", err)
	}
	if got := repo.CountForPayment("pay-replay-1"); got != 1 {
		t.Fatalf("expected one payout record after replay, got %d", got)
	}
}

func TestHandlerRoutesInvalidSettlementWithoutRecordingPayout(t *testing.T) {
	repo := service.NewMemoryRepository()
	svc := service.NewPayoutService(repo)
	dlq := &capturedDLQ{}
	h := consumer.NewHandler(svc, dlq, slog.New(slog.NewTextHandler(io.Discard, nil)))
	msg := kafka.Message{
		Topic:     "payment-settled.v1",
		Partition: 2,
		Offset:    17,
		Key:       []byte("merchant-bad"),
		Value:     []byte(`{"payment_id":"pay-bad-1","amount_cents":9900,"currency":"USD","settled_at":"2026-06-26T10:00:00Z"}`),
		Headers: []kafka.Header{
			{Key: "trace_id", Value: []byte("trace-bad-1")},
		},
	}

	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("invalid record should reach a safe terminal path: %v", err)
	}
	if got := repo.CountForPayment("pay-bad-1"); got != 0 {
		t.Fatalf("invalid record created payout entries: %d", got)
	}
	if len(dlq.messages) != 1 {
		t.Fatalf("expected one failed record, got %d", len(dlq.messages))
	}
	if string(dlq.messages[0].Key) != "merchant-bad" {
		t.Fatalf("failure path changed key: %q", string(dlq.messages[0].Key))
	}
	if !strings.Contains(dlq.reasons[0], "merchant_id") {
		t.Fatalf("failure reason did not describe validation problem: %q", dlq.reasons[0])
	}
	foundTrace := false
	for _, h := range dlq.messages[0].Headers {
		if h.Key == "trace_id" && string(h.Value) == "trace-bad-1" {
			foundTrace = true
		}
	}
	if !foundTrace {
		t.Fatalf("trace header was not preserved")
	}
}
