# Distributed Stream Processor

**Stack (full vision):** Rust (log broker, Kafka-compatible wire) · Go (processor engine) · Protobuf · RocksDB (state) · gRPC · Docker Compose

**Stack (MVP):** Go 1.26 only — single module, no external dependencies. A split Rust/Go stack is overkill at this stage; the MVP collapses both tiers into Go.

## Full Vision
Kafka-compatible log + Flink-style processor, exactly-once, event-time windows + watermarks, stateful operators with RocksDB, checkpoint/restore, SQL API, connectors.

## MVP Status — M1 done

Single-node, disk-persistent, append-only log broker with HTTP API + consumer groups + at-least-once delivery + a sample stream processor DAG (source -> map -> sink).

**What works:**
- `POST /produce?topic=X` — append a record, returns assigned offset
- `GET  /consume?topic=X&group=G&max=N` — at-least-once read; commits offset on success
- `GET  /offset?topic=X&group=G` — inspect committed offset
- Topics are directories of length-prefixed segment files under `data/<topic>/`
- Consumer-group offsets persisted as JSON under `data/_offsets/<topic>__<group>.json`
- Segment roll at 64 MiB; crash-recovery by scanning last segment on startup
- Sample processor reads `numbers`, doubles each int, writes to `doubled`

**Layout:**

```
mvp/
  cmd/broker/      HTTP broker server
  cmd/producer/    CLI producer
  cmd/consumer/    CLI consumer (verifies order + count)
  cmd/processor/   sample DAG: numbers -> (x*2) -> doubled
  internal/log/    append-only segment log + offset store
  internal/broker/ HTTP handlers
scripts/demo.sh    end-to-end demo (see below)
```

## Demo

```bash
# one-shot: builds, starts broker, produces 100, consumes, runs processor, verifies
./scripts/demo.sh
```

Manual:

```bash
cd mvp
go build ./...

# terminal 1
go run ./cmd/broker -addr :9092 -data ../data

# terminal 2
go run ./cmd/producer -broker http://127.0.0.1:9092 -topic demo -count 100
go run ./cmd/consumer -broker http://127.0.0.1:9092 -topic demo -group g1 -expect 100

# sample processor
for i in $(seq 1 10); do curl -s -X POST --data-binary "$i" "http://127.0.0.1:9092/produce?topic=numbers"; done
go run ./cmd/processor -in numbers -out doubled -group doubler
go run ./cmd/consumer -topic doubled -group verify -expect 10 -print
```

## Milestones
- **M1 (Week 1):** Append-only log + consumer groups + offset commit — DONE (MVP)
- **M2 (Week 3):** Partitioning + replication (ISR model)
- **M3 (Week 6):** Stream DAG + stateful ops (count, join) with RocksDB
- **M4 (Week 9):** Event-time + watermarks + tumbling/sliding/session windows
- **M5 (Week 12):** Exactly-once (2PC) + checkpoints + SQL frontend

## Key References
- Kafka design doc
- Flink state/checkpointing paper
- "Streaming Systems" (Akidau et al.)
