# Solution Steps

1. Make payout creation idempotent by using payment_id as the repository identity key. Keep the existing service API, but update the in-memory repository to maintain a map of payment IDs and no-op when a replayed settlement for an already-recorded payment is processed.

2. Route invalid settlement events to the configured dead-letter topic instead of returning a handler error. Decode and validation failures should publish the original Kafka message to the DLQ with the original key/value/headers preserved, plus failure metadata headers.

3. Keep invalid records terminal after successful DLQ publishing: return nil from the handler so the consumer can commit the source offset and continue processing later records in the partition.

4. Switch consumer offset handling from auto-committing reads to explicit fetch/process/commit. Use kafka.Reader.FetchMessage and CommitMessages, and configure CommitInterval to 0 so commits happen synchronously only after successful processing or successful DLQ routing.

5. If processing fails for reasons other than a terminal invalid event, do not commit the offset. Log the failure with Kafka metadata and return the error so the worker restarts/retries instead of silently skipping an unprocessed valid record.

6. Add structured Kafka metadata to logs at every important point: topic, partition, offset, and key for processed messages, DLQ-routed messages, handler failures, and commit failures.

7. Enhance the dead-letter producer so DLQ messages include failure_reason, failed_at, original_topic, original_partition, and original_offset headers for on-call investigation.

8. Run the unit tests to confirm replays do not create duplicate payouts and invalid events are routed to the DLQ without creating payout records. Then run the compose smoke script to verify the worker starts and logs a processed settlement with Kafka metadata.

