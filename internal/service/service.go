package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/utkrusht/go-kafka-payout-ledger/internal/events"
)

type Payout struct {
	PaymentID   string
	MerchantID  string
	AmountCents int64
	Currency    string
	SettledAt   time.Time
	CreatedAt   time.Time
}

type PayoutRepository interface {
	Create(ctx context.Context, payout Payout) error
}

type MemoryRepository struct {
	mu        sync.Mutex
	payouts   []Payout
	byPayment map[string]Payout
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		payouts:   make([]Payout, 0),
		byPayment: make(map[string]Payout),
	}
}

// Create records a payout idempotently by payment_id. Kafka can redeliver records
// after rebalances, restarts, or offset rewinds; from the ledger service's point
// of view the same settled payment must not create more than one payout entry.
func (r *MemoryRepository) Create(ctx context.Context, payout Payout) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.byPayment == nil {
		r.byPayment = make(map[string]Payout)
		for _, existing := range r.payouts {
			r.byPayment[existing.PaymentID] = existing
		}
	}

	if _, exists := r.byPayment[payout.PaymentID]; exists {
		return nil
	}

	r.payouts = append(r.payouts, payout)
	r.byPayment[payout.PaymentID] = payout
	return nil
}

func (r *MemoryRepository) CountForPayment(paymentID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, payout := range r.payouts {
		if payout.PaymentID == paymentID {
			count++
		}
	}
	return count
}

func (r *MemoryRepository) All() []Payout {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Payout, len(r.payouts))
	copy(out, r.payouts)
	return out
}

type PayoutService struct {
	repo PayoutRepository
	now  func() time.Time
}

func NewPayoutService(repo PayoutRepository) *PayoutService {
	return &PayoutService{repo: repo, now: time.Now}
}

func (s *PayoutService) Process(ctx context.Context, ev events.SettlementEvent) error {
	payout := Payout{
		PaymentID:   ev.PaymentID,
		MerchantID:  ev.MerchantID,
		AmountCents: ev.AmountCents,
		Currency:    ev.Currency,
		SettledAt:   ev.SettledAt,
		CreatedAt:   s.now().UTC(),
	}
	if err := s.repo.Create(ctx, payout); err != nil {
		return fmt.Errorf("record payout for payment %s: %w", ev.PaymentID, err)
	}
	return nil
}
