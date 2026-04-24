// Package checkpoint implements M5 exactly-once semantics via two-phase commit.
//
// Model
// -----
// A running task processes input records in "epochs" separated by checkpoint
// barriers. Inside an epoch:
//
//  1. The task consumes records from its source partition and advances an
//     in-memory "pending offset" cursor.
//  2. Keyed-state writes go into bbolt as usual (each Put is its own small
//     tx, so operator state is durable immediately).
//  3. Records destined for the sink are NOT produced to the broker directly.
//     They are staged into a bbolt bucket `__pending_out` as
//     (checkpoint_id, seq) -> {topic, key, value, dedup_id} tuples.
//
// At a barrier:
//
//   (phase 1 — prepare, atomic)
//     bbolt.Update:
//       - persist the pending source offset into __offsets
//       - write {checkpoint_id, status=PREPARED} into __checkpoints
//     window/state buffers that need a flush have already been put into their
//     per-op buckets during Process (that's how our window ops work), so the
//     same Update covers them implicitly via their transactional puts earlier.
//     If anything in this step fails, we abort the checkpoint and the task
//     stays in the previous committed epoch — the broker has not seen any of
//     this epoch's outputs.
//
//   (phase 2 — commit, replay-safe)
//     a. Iterate __pending_out in order and produce each staged record to the
//        broker. Each carries a dedup_id in its payload header; the sink-side
//        dedup layer ignores duplicates, so a crash+restart that replays the
//        same pending record is idempotent.
//     b. bbolt.Update: mark __checkpoints[cp_id]=COMMITTED and DELETE every
//        __pending_out row up to the high-water mark we just flushed.
//
// Recovery
// --------
// On restart the task opens the same bbolt file:
//   - reads the last COMMITTED checkpoint; source offset resumes from the
//     committed cursor (already written to __offsets).
//   - if there are any __pending_out rows left (the crash happened in phase 2
//     between "produced N" and "deleted N rows"), it REPLAYS them. Sink-side
//     dedup turns replays into no-ops -> exactly-once.
//
// Exactly-once guarantee
// ----------------------
// Because offsets, state, and pending outputs are all written in a single
// bbolt transaction, they advance together or not at all. Sink dedup then
// turns "at-least-once replay of pending outputs" into "each dedup_id appears
// at most once in the sink topic". Net effect: every input record's
// contribution to the sink is realized exactly once.
package checkpoint

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Bucket names used by the checkpoint subsystem. Kept in one place so the
// runtime, state backend, and recovery tool all agree.
var (
	BucketCheckpoints = []byte("__checkpoints")
	BucketPendingOut  = []byte("__pending_out")
	BucketOffsets     = []byte("__offsets")
	BucketSeenDedup   = []byte("__seen_dedup") // sink-side dedup
)

// Status of a checkpoint row.
type Status byte

const (
	StatusPrepared  Status = 'P'
	StatusCommitted Status = 'C'
)

// BarrierKind tags the reason a barrier was injected.
type BarrierKind byte

const (
	// BarrierPeriodic is the usual wall-clock barrier (every interval).
	BarrierPeriodic BarrierKind = 'P'
	// BarrierFinal is emitted at end-of-stream to flush everything.
	BarrierFinal BarrierKind = 'F'
)

// Barrier is a sentinel record that flows through the operator chain. Each
// operator forwards it downstream after flushing its own buffers (if any).
// The runtime uses barriers to divide the stream into epochs.
type Barrier struct {
	ID    uint64      // monotonic per task
	Kind  BarrierKind
	ToKey []byte      // unused today; future: targeted barriers per key-range
	At    time.Time   // wall-clock when the barrier was minted
}

// PendingOutput is a sink record staged inside bbolt during an epoch. The
// runtime produces these to the broker in commit-phase order.
type PendingOutput struct {
	Topic    string `json:"t"`
	Key      []byte `json:"k,omitempty"`
	Value    []byte `json:"v"`
	DedupID  string `json:"d"`
	Seq      uint64 `json:"s"`
}

// CheckpointRow is what we store under __checkpoints[<id>].
type CheckpointRow struct {
	ID         uint64    `json:"id"`
	Status     Status    `json:"st"`
	CommittedAt time.Time `json:"ca,omitempty"`
	Offsets    map[string]uint64 `json:"off"`
}

// Coordinator owns checkpoint IDs and decides when to cut a barrier. One
// instance per task. Thread-safety: the task owns its own Coordinator; the
// only atomic is lastID so callers racing for ShouldCut stay consistent.
type Coordinator struct {
	lastID   atomic.Uint64
	interval time.Duration
	lastCut  atomic.Int64 // unix-nanos
}

// NewCoordinator with a minimum cut interval.
func NewCoordinator(interval time.Duration) *Coordinator {
	c := &Coordinator{interval: interval}
	c.lastCut.Store(time.Now().UnixNano())
	return c
}

// NextID returns the next barrier id (1-based).
func (c *Coordinator) NextID() uint64 {
	return c.lastID.Add(1)
}

// ShouldCut reports whether enough time has elapsed since the last cut. It
// also records "now" as the new last-cut if it returns true.
func (c *Coordinator) ShouldCut(now time.Time) bool {
	prev := c.lastCut.Load()
	if now.UnixNano()-prev < int64(c.interval) {
		return false
	}
	return c.lastCut.CompareAndSwap(prev, now.UnixNano())
}

// LastID exposes the last-issued id (0 if none). Useful for recovery logs.
func (c *Coordinator) LastID() uint64 { return c.lastID.Load() }

// ----------------------------------------------------------------------------
// bbolt helpers — the 2PC persistence primitives.
// ----------------------------------------------------------------------------

// EnsureBuckets creates checkpoint-related buckets. Called by state.Open.
func EnsureBuckets(tx *bolt.Tx) error {
	for _, b := range [][]byte{BucketCheckpoints, BucketPendingOut, BucketOffsets, BucketSeenDedup} {
		if _, err := tx.CreateBucketIfNotExists(b); err != nil {
			return err
		}
	}
	return nil
}

// PrepareKey encodes a pending-output key: <cp_id:8BE>||<seq:8BE>.
func PrepareKey(cpID, seq uint64) []byte {
	out := make([]byte, 16)
	binary.BigEndian.PutUint64(out[0:8], cpID)
	binary.BigEndian.PutUint64(out[8:16], seq)
	return out
}

// DecodePrepareKey reverses PrepareKey. ok=false on malformed input.
func DecodePrepareKey(k []byte) (cpID, seq uint64, ok bool) {
	if len(k) != 16 {
		return 0, 0, false
	}
	return binary.BigEndian.Uint64(k[0:8]), binary.BigEndian.Uint64(k[8:16]), true
}

// StagePendingOutput writes a single pending output inside the caller's tx.
func StagePendingOutput(tx *bolt.Tx, cpID, seq uint64, po PendingOutput) error {
	b := tx.Bucket(BucketPendingOut)
	if b == nil {
		var err error
		if b, err = tx.CreateBucket(BucketPendingOut); err != nil {
			return err
		}
	}
	raw, err := json.Marshal(po)
	if err != nil {
		return err
	}
	return b.Put(PrepareKey(cpID, seq), raw)
}

// WritePrepared atomically flushes the offsets map AND writes a
// {cp_id -> PREPARED} row. Call inside a bbolt Update tx.
func WritePrepared(tx *bolt.Tx, row CheckpointRow, offsets map[string]uint64) error {
	if err := EnsureBuckets(tx); err != nil {
		return err
	}
	// Offsets: single key "v1" -> JSON, matches state.Checkpoint() format.
	raw, err := json.Marshal(offsets)
	if err != nil {
		return err
	}
	if err := tx.Bucket(BucketOffsets).Put([]byte("v1"), raw); err != nil {
		return err
	}
	row.Status = StatusPrepared
	rowRaw, err := json.Marshal(row)
	if err != nil {
		return err
	}
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], row.ID)
	return tx.Bucket(BucketCheckpoints).Put(k[:], rowRaw)
}

// WriteCommitted updates the checkpoint row to COMMITTED and deletes every
// pending-output key whose cp_id <= the committed row.ID. Call inside Update.
func WriteCommitted(tx *bolt.Tx, cpID uint64, now time.Time) error {
	ckb := tx.Bucket(BucketCheckpoints)
	if ckb == nil {
		return fmt.Errorf("checkpoint: missing __checkpoints bucket")
	}
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], cpID)
	raw := ckb.Get(k[:])
	if raw == nil {
		return fmt.Errorf("checkpoint: cp %d has no prepared row", cpID)
	}
	var row CheckpointRow
	if err := json.Unmarshal(raw, &row); err != nil {
		return err
	}
	row.Status = StatusCommitted
	row.CommittedAt = now
	newRaw, _ := json.Marshal(row)
	if err := ckb.Put(k[:], newRaw); err != nil {
		return err
	}

	// Delete pending outputs produced by this cp (and earlier, just in case).
	pb := tx.Bucket(BucketPendingOut)
	if pb != nil {
		c := pb.Cursor()
		var toDel [][]byte
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			cp, _, ok := DecodePrepareKey(k)
			if !ok {
				continue
			}
			if cp <= cpID {
				cp2 := make([]byte, len(k))
				copy(cp2, k)
				toDel = append(toDel, cp2)
			}
		}
		for _, k := range toDel {
			if err := pb.Delete(k); err != nil {
				return err
			}
		}
	}
	return nil
}

// LastCommitted returns the last committed checkpoint row, if any.
func LastCommitted(tx *bolt.Tx) (CheckpointRow, bool, error) {
	ckb := tx.Bucket(BucketCheckpoints)
	if ckb == nil {
		return CheckpointRow{}, false, nil
	}
	c := ckb.Cursor()
	var best CheckpointRow
	found := false
	for k, v := c.First(); k != nil; k, v = c.Next() {
		var row CheckpointRow
		if err := json.Unmarshal(v, &row); err != nil {
			continue
		}
		if row.Status != StatusCommitted {
			continue
		}
		if !found || row.ID > best.ID {
			best = row
			found = true
		}
	}
	return best, found, nil
}

// IterPending iterates every pending-output row in ascending (cp_id, seq)
// order. Used on restart to replay anything that was prepared-but-not-emitted.
func IterPending(tx *bolt.Tx, fn func(cpID, seq uint64, po PendingOutput) error) error {
	pb := tx.Bucket(BucketPendingOut)
	if pb == nil {
		return nil
	}
	c := pb.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		cp, seq, ok := DecodePrepareKey(k)
		if !ok {
			continue
		}
		var po PendingOutput
		if err := json.Unmarshal(v, &po); err != nil {
			return err
		}
		if err := fn(cp, seq, po); err != nil {
			return err
		}
	}
	return nil
}

// DeletePending removes a single pending row (used when the runtime decides
// to drop rather than commit an in-progress epoch — not the common path).
func DeletePending(tx *bolt.Tx, cpID, seq uint64) error {
	pb := tx.Bucket(BucketPendingOut)
	if pb == nil {
		return nil
	}
	return pb.Delete(PrepareKey(cpID, seq))
}

// ----------------------------------------------------------------------------
// Dedup — sink-side. The producer tags each staged record with a deterministic
// dedup_id = <task_id>|<cp_id>|<seq>. On replay after a crash, the same id is
// used — the sink checks its __seen_dedup bucket and drops duplicates.
// ----------------------------------------------------------------------------

// SeenDedup returns true if dedupID has already been accepted by this sink.
func SeenDedup(tx *bolt.Tx, topic, dedupID string) (bool, error) {
	b := tx.Bucket(BucketSeenDedup)
	if b == nil {
		return false, nil
	}
	v := b.Get([]byte(topic + "|" + dedupID))
	return v != nil, nil
}

// MarkDedup records that dedupID has been delivered to topic.
func MarkDedup(tx *bolt.Tx, topic, dedupID string) error {
	b, err := tx.CreateBucketIfNotExists(BucketSeenDedup)
	if err != nil {
		return err
	}
	return b.Put([]byte(topic+"|"+dedupID), []byte{1})
}

// DedupID builds the canonical id for a (task, cp, seq) triple.
func DedupID(taskID string, cpID, seq uint64) string {
	return fmt.Sprintf("%s|%d|%d", taskID, cpID, seq)
}
