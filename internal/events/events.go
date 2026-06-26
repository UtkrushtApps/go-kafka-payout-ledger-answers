package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrInvalidEvent = errors.New("invalid settlement event")

type SettlementEvent struct {
	PaymentID   string    `json:"payment_id"`
	MerchantID  string    `json:"merchant_id"`
	AmountCents int64     `json:"amount_cents"`
	Currency    string    `json:"currency"`
	SettledAt   time.Time `json:"settled_at"`
	TraceID     string    `json:"trace_id,omitempty"`
}

func DecodeSettlement(data []byte) (SettlementEvent, error) {
	var ev SettlementEvent
	if err := json.Unmarshal(data, &ev); err != nil {
		return SettlementEvent{}, fmt.Errorf("%w: %v", ErrInvalidEvent, err)
	}
	if strings.TrimSpace(ev.PaymentID) == "" {
		return SettlementEvent{}, fmt.Errorf("%w: payment_id is required", ErrInvalidEvent)
	}
	if strings.TrimSpace(ev.MerchantID) == "" {
		return SettlementEvent{}, fmt.Errorf("%w: merchant_id is required", ErrInvalidEvent)
	}
	if ev.AmountCents <= 0 {
		return SettlementEvent{}, fmt.Errorf("%w: amount_cents must be positive", ErrInvalidEvent)
	}
	if strings.TrimSpace(ev.Currency) == "" {
		return SettlementEvent{}, fmt.Errorf("%w: currency is required", ErrInvalidEvent)
	}
	if ev.SettledAt.IsZero() {
		return SettlementEvent{}, fmt.Errorf("%w: settled_at is required", ErrInvalidEvent)
	}
	return ev, nil
}

func IsInvalid(err error) bool {
	return errors.Is(err, ErrInvalidEvent)
}
