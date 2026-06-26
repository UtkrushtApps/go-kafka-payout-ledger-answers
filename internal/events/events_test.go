package events_test

import (
	"testing"

	"github.com/utkrusht/go-kafka-payout-ledger/internal/events"
)

func TestDecodeSettlementValid(t *testing.T) {
	data := []byte(`{"payment_id":"pay-100","merchant_id":"m-9","amount_cents":4200,"currency":"USD","settled_at":"2026-06-26T10:00:00Z","trace_id":"trace-100"}`)
	ev, err := events.DecodeSettlement(data)
	if err != nil {
		t.Fatalf("expected valid event: %v", err)
	}
	if ev.PaymentID != "pay-100" || ev.MerchantID != "m-9" || ev.AmountCents != 4200 || ev.TraceID != "trace-100" {
		t.Fatalf("decoded unexpected event: %+v", ev)
	}
}

func TestDecodeSettlementRejectsInvalidBusinessFields(t *testing.T) {
	cases := map[string]string{
		"missing payment":  `{"merchant_id":"m-9","amount_cents":4200,"currency":"USD","settled_at":"2026-06-26T10:00:00Z"}`,
		"missing merchant": `{"payment_id":"pay-100","amount_cents":4200,"currency":"USD","settled_at":"2026-06-26T10:00:00Z"}`,
		"bad amount":       `{"payment_id":"pay-100","merchant_id":"m-9","amount_cents":0,"currency":"USD","settled_at":"2026-06-26T10:00:00Z"}`,
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := events.DecodeSettlement([]byte(payload))
			if err == nil {
				t.Fatalf("expected validation error")
			}
			if !events.IsInvalid(err) {
				t.Fatalf("expected invalid event error, got %v", err)
			}
		})
	}
}
