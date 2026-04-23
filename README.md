# Distributed Stream Processor

**Stack (full vision):** Rust (log broker, Kafka-compatible wire) · Go (processor engine) · Protobuf · RocksDB (state) · gRPC · Docker Compose

**Stack (MVP):** Go 1.26 only — single module, no external dependencies. A split Rust/Go stack is overkill at this stage; the MVP collapses both tiers into Go.

## Full Vision
Kafka-compatible log + Flink-style processor, exactly-once, event-time windows + watermarks, stateful operators with RocksDB, checkpoint/restore, SQL API, connectors.

## MVP Status — M2 done

Multi-broker cluster with per-topic partitioning and synchronous in-sync-replica (ISR) replication. Producers route by key hash; consumer groups split partitions across members.

**What works (on top of M1):**
- `POST /admin/create_topic?topic=X&partitions=N[&replication=R]` — any broker acts as controller: it computes a deterministic leader / follower assignment and fans it out to peers via `POST /admin/assign`.
- `GET  /metadata[?topic=X]` — returns the full partition map (leader + followers) plus the broker id-to-URL table, so clients can route directly.
- `POST /produce?topic=X[&key=K][&partition=P]` — leader check first; non-leaders return `307` redirect to the partition leader. The leader writes to its local log, synchronously calls `POST /replicate` on every follower (static ISR), and only acks the client after every replica has written the record at the exact same offset.
- `POST /replicate?topic=X&partition=P&offset=O` — follower-side endpoint; uses `log.AppendAt(offset, payload)` so every replica is byte-identical.
- Partition-aware `GET /consume?topic=X&partition=P&group=G` — committed offsets are keyed as `(topic, partition, group)`.
- Consumer group range-assignment (`-members N -member-id i`): members fetch metadata and split partitions deterministically (no on-broker coordinator yet — M3 adds one).
- Producer hashes the message key with FNV-1a and routes to `leader(key_hash % N)`; keyless produces fall back to a round-robin counter on the broker.

**Cluster layout on disk:**
```
data_m2/
  b0/<topic>/p0/000...log     (leader replica)
  b0/<topic>/p1/000...log     (follower replica for p1)
  b0/<topic>/p2/000...log     (follower replica for p2)
  b0/<topic>/p3/000...log     (leader replica)
  b1/<topic>/...              (same shape, different roles)
  b2/<topic>/...
  b0/_offsets/<topic>__p<k>__<group>.json
```

### M2 demo

```bash
./scripts/demo_m2.sh
```

Output (Apr 2026, `./scripts/demo_m2.sh`):

```
>>> starting 3-broker cluster
    brokers up: b0=19092 b1=19093 b2=19094
>>> creating topic=events partitions=4 replication=3 (via b0)
    {"topic":"events","partitions":[
       {"partition":0,"leader":0,"followers":[1,2]},
       {"partition":1,"leader":1,"followers":[2,0]},
       {"partition":2,"leader":2,"followers":[0,1]},
       {"partition":3,"leader":0,"followers":[1,2]}]}
>>> producing 1000 keyed messages via b1
    produced 1000 messages to topic="events" across 4 partitions
      partition 0: 250 msgs (leader=b0)
      partition 1: 250 msgs (leader=b1)
      partition 2: 250 msgs (leader=b2)
      partition 3: 250 msgs (leader=b0)
>>> consuming with 2-member group g1
    [m0] assigned partitions=[0 1] ... consumed 500 records
    [m1] assigned partitions=[2 3] ... consumed 500 records
>>> tally: m0=500  m1=500  total=1000
>>> verifying replicas: segment sizes should match across b0/b1/b2 per partition
    partition 0: b0=4473 b1=4473 b2=4473
    partition 1: b0=4472 b1=4472 b2=4472
    partition 2: b0=4472 b1=4472 b2=4472
    partition 3: b0=4473 b1=4473 b2=4473
>>> DEMO PASSED
```

Zero duplicates, zero gaps, every follower byte-matches its leader. The few-byte delta between partitions reflects that different FNV buckets land slightly different counts of 2- vs 3-digit payloads (`ev-9` vs `ev-100`).

**M2 non-goals (deferred):**
- Failure detection, leader re-election, ISR shrink/expand. A downed broker currently stalls writes to the partitions it hosts.
- Persistent cluster metadata. Topics must be re-created after a full-cluster restart (the segment files survive on disk, the assignment doesn't).
- Server-side consumer group coordinator. Members negotiate assignment via `-members/-member-id` flags.

---

## MVP Status — M1 done

Single-node, disk-persistent, append-only log broker with HTTP API + consumer groups + at-least-once delivery + a sample stream processor DAG (source -> map -> sink). Superseded by M2 above; `scripts/demo.sh` is not updated to the new partitioned API (use `demo_m2.sh`).

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
  cmd/broker/      HTTP broker server (M2: -id, -peers flags)
  cmd/producer/    CLI producer (M2: metadata-aware, key-hash routing)
  cmd/consumer/    CLI consumer (M2: -members/-member-id range assignment)
  cmd/processor/   sample DAG: numbers -> (x*2) -> doubled (M2-updated)
  internal/log/    append-only segment log + offset store (+ AppendAt for replication)
  internal/broker/ HTTP handlers, cluster metadata, ISR replication
scripts/demo.sh     M1 end-to-end demo (pre-partitioning; kept for reference)
scripts/demo_m2.sh  M2 end-to-end demo: 3 brokers, 4 partitions, 2-member group
```

## Milestones
- **M1 (Week 1):** Append-only log + consumer groups + offset commit — DONE
- **M2 (Week 3):** Partitioning + replication (ISR model) — DONE
- **M3 (Week 6):** Stream DAG + stateful ops (count, join) with RocksDB
- **M4 (Week 9):** Event-time + watermarks + tumbling/sliding/session windows
- **M5 (Week 12):** Exactly-once (2PC) + checkpoints + SQL frontend

## Key References
- Kafka design doc
- Flink state/checkpointing paper
- "Streaming Systems" (Akidau et al.)
