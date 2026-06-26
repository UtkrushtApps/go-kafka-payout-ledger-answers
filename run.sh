#!/usr/bin/env bash
set -e

cd /root/task

on_error() {
  echo "startup failed; recent service logs follow"
  docker compose logs --tail=120 || true
}
trap on_error ERR

echo "building and starting services"
docker compose up -d --build

echo "waiting for Kafka readiness"
ready=0
for i in $(seq 1 60); do
  if docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 2
done

if [ "$ready" != "1" ]; then
  echo "Kafka did not become ready in time"
  exit 1
fi

echo "creating assessment topics"
for topic in payment-settled.v1 payment-settled.dlq; do
  docker compose exec -T kafka kafka-topics \
    --bootstrap-server kafka:9092 \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1 >/dev/null
done

echo "validating worker container"
for i in $(seq 1 30); do
  if docker compose ps --status running payout-ledger | grep -q payout-ledger; then
    break
  fi
  sleep 1
  if [ "$i" -eq 30 ]; then
    echo "payout-ledger worker is not running"
    exit 1
  fi
done

printf '%s\n' 'smoke-1|{"payment_id":"smoke-1","merchant_id":"merchant-smoke","amount_cents":1299,"currency":"USD","settled_at":"2026-06-26T10:00:00Z","trace_id":"trace-smoke"}' | \
  docker compose exec -T kafka kafka-console-producer \
    --bootstrap-server kafka:9092 \
    --topic payment-settled.v1 \
    --property parse.key=true \
    --property key.separator='|' >/dev/null

sleep 2

echo "deployment ready"
docker compose ps
echo "worker logs contain the live processing trace for the smoke event"
