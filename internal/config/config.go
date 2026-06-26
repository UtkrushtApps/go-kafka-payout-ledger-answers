package config

type Config struct {
	Brokers         []string
	GroupID         string
	SettlementTopic string
	DeadLetterTopic string
}

func Local() Config {
	return Config{
		Brokers:         []string{"kafka:9092"},
		GroupID:         "payout-ledger-v1",
		SettlementTopic: "payment-settled.v1",
		DeadLetterTopic: "payment-settled.dlq",
	}
}
