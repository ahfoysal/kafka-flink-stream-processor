# 07 — Distributed Stream Processor

**Stack:** Rust (log broker, Kafka-compatible wire) · Go (processor engine) · Protobuf · RocksDB (state) · gRPC · Docker Compose

## Full Vision
Kafka-compatible log + Flink-style processor, exactly-once, event-time windows + watermarks, stateful operators with RocksDB, checkpoint/restore, SQL API, connectors.

## MVP (1 weekend)
Single-node append-only log + producers/consumers + offsets (at-least-once).

## Milestones
- **M1 (Week 1):** Append-only log + consumer groups + offset commit
- **M2 (Week 3):** Partitioning + replication (ISR model)
- **M3 (Week 6):** Stream DAG + stateful ops (count, join) with RocksDB
- **M4 (Week 9):** Event-time + watermarks + tumbling/sliding/session windows
- **M5 (Week 12):** Exactly-once (2PC) + checkpoints + SQL frontend

## Key References
- Kafka design doc
- Flink state/checkpointing paper
- "Streaming Systems" (Akidau et al.)
