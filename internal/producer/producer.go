package producer

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

type DeadLetterProducer struct {
	writer *kafka.Writer
}

func NewDeadLetterProducer(brokers []string, topic string) *DeadLetterProducer {
	return &DeadLetterProducer{
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:      brokers,
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: int(kafka.RequireAll),
		}),
	}
}

func (p *DeadLetterProducer) Publish(ctx context.Context, original kafka.Message, reason error) error {
	now := time.Now().UTC()
	failureReason := "unknown failure"
	if reason != nil {
		failureReason = reason.Error()
	}

	headers := make([]kafka.Header, 0, len(original.Headers)+5)
	headers = append(headers, original.Headers...)
	headers = append(headers, kafka.Header{Key: "failure_reason", Value: []byte(failureReason)})
	headers = append(headers, kafka.Header{Key: "failed_at", Value: []byte(now.Format(time.RFC3339Nano))})
	headers = append(headers, kafka.Header{Key: "original_topic", Value: []byte(original.Topic)})
	headers = append(headers, kafka.Header{Key: "original_partition", Value: []byte(strconv.Itoa(original.Partition))})
	headers = append(headers, kafka.Header{Key: "original_offset", Value: []byte(strconv.FormatInt(original.Offset, 10))})

	msg := kafka.Message{
		Key:     append([]byte(nil), original.Key...),
		Value:   append([]byte(nil), original.Value...),
		Headers: headers,
		Time:    now,
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("publish failed settlement record: %w", err)
	}
	return nil
}

func (p *DeadLetterProducer) Close() error {
	return p.writer.Close()
}
