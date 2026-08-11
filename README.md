# Queuemaxxing 💪

A backend-only HTTP queue service that composes **FIFO/LIFO**, **priority**, and **delay** semantics. It persists its own append-only, `fsync`'d write-ahead log (WAL), so storage is not delegated to a database or another queue.

## What is included

- Go HTTP API
- FIFO and LIFO queues
- Optional per-queue priority ordering
- Delayed messages using `available_at`
- At-least-once delivery with visibility leases
- Ack and nack/redelivery
- Durable WAL replay after process restart
- Concurrency-safe queue operations
- Small Go CLI client for interacting with the HTTP service
- Unit tests covering ordering, priority, delay, redelivery, and restart recovery

There is intentionally **no frontend**.

## Design

Each queue has two independent knobs: `discipline` (`fifo` or `lifo`) and `priority` (`true`/`false`). Each message has an `available_at` timestamp. Dequeue filters out delayed and currently leased messages, then orders by priority if enabled, and finally by FIFO/LIFO sequence order. This yields FIFO, LIFO, priority FIFO, priority LIFO, and delayed variants from one implementation.

Durability uses an append-only JSONL WAL. State-changing operations are written and `fsync`ed before their in-memory mutations are considered committed. On startup, the WAL is replayed to reconstruct queue state. This keeps persistence embedded in the application rather than relying on Redis, Postgres, SQLite, or a separate message broker.

Concurrency is protected by synchronization around queue state and WAL writes. A dequeue creates a visibility lease with a unique receipt so the same message cannot be concurrently delivered again while its lease is active.

Delivery semantics are **at least once**. A consumer acknowledges a message to remove it. It can also nack the message, optionally adding a delay before redelivery. If the consumer crashes or never acknowledges it, the visibility lease expires and the message becomes available again.

## Run

Requires Go 1.22+.

```bash
go test ./...
go run ./cmd/server -addr :8080 -data ./data/queue.wal
```

Health check:

```bash
curl localhost:8080/healthz
```

Expected response:

```json
{"status":"ok"}
```

## CLI client

In another terminal, use the included client application:

```bash
go run ./cmd/client list
```

Create a priority LIFO queue:

```bash
go run ./cmd/client create -name jobs -discipline lifo -priority
```

Enqueue an immediately available message:

```bash
go run ./cmd/client enqueue -queue jobs -body "resize image" -priority 10
```

Enqueue a delayed message:

```bash
go run ./cmd/client enqueue -queue jobs -body "send reminder" -priority 3 -delay 5s
```

Dequeue with a 30-second visibility timeout:

```bash
go run ./cmd/client dequeue -queue jobs -visibility 30s -wait 1s
```

The response contains the message ID and a delivery `receipt`. Use both to ack:

```bash
go run ./cmd/client ack -queue jobs -id <MESSAGE_ID> -receipt <RECEIPT>
```

Or nack and delay redelivery by three seconds:

```bash
go run ./cmd/client nack -queue jobs -id <MESSAGE_ID> -receipt <RECEIPT> -delay 3s
```

Queue statistics:

```bash
go run ./cmd/client stats -queue jobs
```

To point the client at a different server:

```bash
go run ./cmd/client -server http://127.0.0.1:9000 list
```

## HTTP API

### Create a queue

```bash
curl -s localhost:8080/api/queues \
  -H 'content-type: application/json' \
  -d '{"name":"jobs","discipline":"lifo","priority":true}'
```

### List queues

```bash
curl -s localhost:8080/api/queues
```

### Enqueue

```bash
curl -s localhost:8080/api/queues/jobs/messages \
  -H 'content-type: application/json' \
  -d '{"body":"resize image","priority":10,"delay_ms":5000}'
```

### Dequeue

```bash
curl -s -X POST \
  'localhost:8080/api/queues/jobs/dequeue?visibility_ms=30000&wait_ms=1000'
```

### Ack

```bash
curl -s localhost:8080/api/queues/jobs/messages/<MESSAGE_ID>/ack \
  -H 'content-type: application/json' \
  -d '{"receipt":"<RECEIPT>"}'
```

### Nack / redelay

```bash
curl -s localhost:8080/api/queues/jobs/messages/<MESSAGE_ID>/nack \
  -H 'content-type: application/json' \
  -d '{"receipt":"<RECEIPT>","delay_ms":3000}'
```

### Stats

```bash
curl -s localhost:8080/api/queues/jobs/stats
```

## Replay messages

There are two relevant forms of replay.

**Consumer retry/redelivery:** dequeue only leases a message. If the consumer does not ack before the visibility timeout, the message becomes eligible for delivery again. That provides at-least-once delivery. Consumers should use `message.id` as an idempotency key when processing has side effects.

**Process restart:** the service replays the WAL to reconstruct queue configuration and message state. A lease whose wall-clock deadline has already expired is treated as available again.

A production implementation should add producer idempotency keys, WAL checksums, segment rotation, and snapshots/compaction so duplicate producer retries can be collapsed and startup replay stays bounded.

## How I would refactor it into Pub/Sub

The WAL and immutable message identity can stay. The core change is replacing the single delivery state attached to a queue message with one delivery cursor/state per subscription.

A topic append would persist one immutable message. Every durable subscription would independently track its own acknowledgement or offset state, visibility leases, filters, and delivery policy. The same topic message could therefore be delivered independently to many subscribers. Compaction could reclaim a topic record only after all required durable subscriptions have advanced past it, subject to retention rules.

## What I would add with more time

- WAL segmentation, snapshots, and compaction
- CRC checksums and torn-write recovery
- Batch enqueue/dequeue/ack endpoints
- Producer idempotency keys and duplicate suppression
- Dead-letter queues and maximum-attempt policies
- Message TTL and queue retention policies
- Payload size limits and disk quotas/backpressure
- Prometheus/OpenTelemetry metrics
- Authentication and TLS
- Graceful draining during shutdown
- Consumer groups and fair scheduling
- Fuzz/property tests
- Replication via Raft for high availability

## Why use this instead of SQS, RabbitMQ, or Pulsar?

For a production system with broad requirements, users should generally choose one of those mature systems. SQS provides managed durability and scale, RabbitMQ provides mature routing and protocol support, and Pulsar provides distributed durable streaming and multi-tenancy.

Queuemaxxing's narrower advantage is that it is a tiny, inspectable, zero-external-storage service with intentionally composable scheduling semantics: FIFO/LIFO + priority + delay. It is useful when operational simplicity and the ability to understand or modify the storage and scheduling implementation matter more than ecosystem breadth or distributed guarantees.

## Crash-consistency notes

The key invariant is **WAL first, memory second**. If append or `fsync` fails, the in-memory mutation is not committed.

A crash after the WAL write succeeds but before the HTTP response reaches a producer can cause the producer to retry an enqueue that was already committed. Producer idempotency keys would address that in a production version.

A crash after a consumer performs an external side effect but before its ack is persisted can cause the message to be delivered again. That is expected under at-least-once semantics, so consumers with side effects must be idempotent.
