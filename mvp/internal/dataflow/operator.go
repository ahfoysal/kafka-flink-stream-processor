package dataflow

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"strconv"
	"sync"
	"time"

	"github.com/ahfoysal/kafka-flink-stream-processor/mvp/internal/state"
)

// Emitter is the "collector" handed to every stateless operator. Stateful
// operators receive an EmitterKeyed that also carries the partition and a
// reference to the state store.
type Emitter interface {
	Emit(Record) error
}

// Operator is the runtime interface every DAG node implements. Process is
// called once per input record; Finish flushes any buffered state at
// end-of-stream (used by Count to emit its running totals).
type Operator interface {
	Name() string
	Process(ctx context.Context, in Record, out Emitter) error
	Finish(ctx context.Context, out Emitter) error
}

// ----------------------------------------------------------------------------
// Stateless operators
// ----------------------------------------------------------------------------

// MapOp: f(record) -> record.
type MapOp struct {
	OpName string
	Fn     func(Record) (Record, error)
}

func (o *MapOp) Name() string { return o.OpName }
func (o *MapOp) Process(_ context.Context, in Record, out Emitter) error {
	r, err := o.Fn(in)
	if err != nil {
		return err
	}
	return out.Emit(r)
}
func (o *MapOp) Finish(context.Context, Emitter) error { return nil }

// FilterOp: drop records where Fn returns false.
type FilterOp struct {
	OpName string
	Fn     func(Record) bool
}

func (o *FilterOp) Name() string { return o.OpName }
func (o *FilterOp) Process(_ context.Context, in Record, out Emitter) error {
	if !o.Fn(in) {
		return nil
	}
	return out.Emit(in)
}
func (o *FilterOp) Finish(context.Context, Emitter) error { return nil }

// FlatMapOp: one record in, N records out (used by the word-count tokenizer).
type FlatMapOp struct {
	OpName string
	Fn     func(Record) ([]Record, error)
}

func (o *FlatMapOp) Name() string { return o.OpName }
func (o *FlatMapOp) Process(_ context.Context, in Record, out Emitter) error {
	rs, err := o.Fn(in)
	if err != nil {
		return err
	}
	for _, r := range rs {
		if err := out.Emit(r); err != nil {
			return err
		}
	}
	return nil
}
func (o *FlatMapOp) Finish(context.Context, Emitter) error { return nil }

// GroupByKeyOp is a marker: it doesn't transform records, it just asserts
// "downstream operators see records keyed by Record.Key and can trust that
// all records with the same key land on the same task". In our M3 runtime
// partitioning is already key-driven at Source (the M2 broker hashes by key),
// so GroupByKey is a no-op re-keyer that can also re-key via a user fn.
type GroupByKeyOp struct {
	OpName string
	KeyFn  func(Record) []byte // optional; nil = keep existing Key
}

func (o *GroupByKeyOp) Name() string { return o.OpName }
func (o *GroupByKeyOp) Process(_ context.Context, in Record, out Emitter) error {
	if o.KeyFn != nil {
		in.Key = o.KeyFn(in)
	}
	return out.Emit(in)
}
func (o *GroupByKeyOp) Finish(context.Context, Emitter) error { return nil }

// ----------------------------------------------------------------------------
// Stateful operators
// ----------------------------------------------------------------------------

// CountOp: per-key running count, state-backed. On every input record it
// increments state[key] and emits (key, count) downstream. Downstream sinks
// see the latest count per update (like a Kafka Streams KTable changelog).
type CountOp struct {
	OpName    string
	Store     *state.Store
	Partition int
}

func (o *CountOp) Name() string { return o.OpName }
func (o *CountOp) Process(_ context.Context, in Record, out Emitter) error {
	cur, _, err := o.Store.GetUint64(o.OpName, o.Partition, in.Key)
	if err != nil {
		return err
	}
	cur++
	if err := o.Store.PutUint64(o.OpName, o.Partition, in.Key, cur); err != nil {
		return err
	}
	return out.Emit(Record{
		Key:       in.Key,
		Value:     []byte(strconv.FormatUint(cur, 10)),
		Timestamp: in.Timestamp,
	})
}
func (o *CountOp) Finish(context.Context, Emitter) error { return nil }

// ReduceOp: per-key running reduction. Fn receives (accumulator, newValue)
// and returns the new accumulator, persisted to state.
type ReduceOp struct {
	OpName    string
	Store     *state.Store
	Partition int
	Fn        func(acc, val []byte) ([]byte, error)
	// InitFn yields the initial accumulator for a key that has no state yet.
	InitFn func(val []byte) ([]byte, error)
}

func (o *ReduceOp) Name() string { return o.OpName }
func (o *ReduceOp) Process(_ context.Context, in Record, out Emitter) error {
	acc, ok, err := o.Store.Get(o.OpName, o.Partition, in.Key)
	if err != nil {
		return err
	}
	var next []byte
	if !ok {
		next, err = o.InitFn(in.Value)
	} else {
		next, err = o.Fn(acc, in.Value)
	}
	if err != nil {
		return err
	}
	if err := o.Store.Put(o.OpName, o.Partition, in.Key, next); err != nil {
		return err
	}
	return out.Emit(Record{Key: in.Key, Value: next, Timestamp: in.Timestamp})
}
func (o *ReduceOp) Finish(context.Context, Emitter) error { return nil }

// ----------------------------------------------------------------------------
// Stream-stream join with a time bound.
//
// For each record on the LEFT side we probe a time-bounded buffer of RIGHT
// records with the same key, and vice versa. Buffers live in the keyed-state
// store (per partition) so they survive restarts.
//
// Record layout in the buffer bucket: key = <side>|<ts_nanos>|<seq> -> value.
// JoinOp is wired up by the builder only when the user calls
// .Join(rightStream, bound) — the runtime merges two partition channels into
// a single input stream tagged with "side".
// ----------------------------------------------------------------------------

type JoinSide byte

const (
	SideLeft  JoinSide = 'L'
	SideRight JoinSide = 'R'
)

// JoinOp buffers both sides in state and emits (left, right) pairs that fall
// within Bound of each other. Fn builds the output payload from the pair.
type JoinOp struct {
	OpName    string
	Store     *state.Store
	Partition int
	Bound     time.Duration
	Fn        func(left, right Record) (Record, error)

	mu  sync.Mutex
	seq uint64 // monotonic per-op, so buffer keys are unique
}

func (o *JoinOp) Name() string { return o.OpName }

// JoinInput is what the runtime feeds into JoinOp.Process: the original
// record plus which side it came from.
type JoinInput struct {
	Record
	Side JoinSide
}

// Process signature is the operator interface, but the runtime wraps each
// record in a JoinInput encoded into Value prefix "<side>|" — this keeps the
// operator interface uniform.
func (o *JoinOp) Process(_ context.Context, in Record, out Emitter) error {
	if len(in.Value) < 2 || (in.Value[0] != 'L' && in.Value[0] != 'R') || in.Value[1] != '|' {
		return fmt.Errorf("JoinOp expects side-prefixed value, got %q", in.Value)
	}
	side := JoinSide(in.Value[0])
	payload := in.Value[2:]

	bucket := fmt.Sprintf("%s__%c", o.OpName, side)
	otherBucket := fmt.Sprintf("%s__%c", o.OpName, flipSide(side))
	ts := in.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}

	// Stash this side's record.
	o.mu.Lock()
	o.seq++
	seq := o.seq
	o.mu.Unlock()
	bufKey := makeJoinKey(in.Key, ts, seq)
	if err := o.Store.Put(bucket, o.Partition, bufKey, payload); err != nil {
		return err
	}

	// Probe the opposite side for matches within the time bound.
	var emitErr error
	err := o.Store.Iterate(otherBucket, o.Partition, func(k, v []byte) error {
		otherKey, otherTs, ok := parseJoinKey(k)
		if !ok {
			return nil
		}
		if !bytesEqual(otherKey, in.Key) {
			return nil
		}
		if absDur(ts.Sub(otherTs)) > o.Bound {
			return nil
		}
		var left, right Record
		if side == SideLeft {
			left = Record{Key: in.Key, Value: payload, Timestamp: ts}
			right = Record{Key: in.Key, Value: append([]byte(nil), v...), Timestamp: otherTs}
		} else {
			right = Record{Key: in.Key, Value: payload, Timestamp: ts}
			left = Record{Key: in.Key, Value: append([]byte(nil), v...), Timestamp: otherTs}
		}
		joined, err := o.Fn(left, right)
		if err != nil {
			emitErr = err
			return err
		}
		if err := out.Emit(joined); err != nil {
			emitErr = err
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return emitErr
}
func (o *JoinOp) Finish(context.Context, Emitter) error { return nil }

func flipSide(s JoinSide) JoinSide {
	if s == SideLeft {
		return SideRight
	}
	return SideLeft
}

// Join-buffer key layout: big-endian ts_nanos (8B) || seq (8B) || key.
// Puts all records with the same key in arbitrary order (they share bucket),
// so the iterator sees every entry and filters by key in-memory. Fine for
// modest buffers; M4 will add time-based compaction.
func makeJoinKey(key []byte, ts time.Time, seq uint64) []byte {
	out := make([]byte, 16+len(key))
	binary.BigEndian.PutUint64(out[0:8], uint64(ts.UnixNano()))
	binary.BigEndian.PutUint64(out[8:16], seq)
	copy(out[16:], key)
	return out
}
func parseJoinKey(buf []byte) (key []byte, ts time.Time, ok bool) {
	if len(buf) < 16 {
		return nil, time.Time{}, false
	}
	tsNs := int64(binary.BigEndian.Uint64(buf[0:8]))
	return buf[16:], time.Unix(0, tsNs), true
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func absDur(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// partitionOf hashes a key to a partition using FNV-1a, matching the
// producer-side routing used by the broker (see cmd/producer/main.go).
func partitionOf(key []byte, n int) int {
	if n <= 0 {
		return 0
	}
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32() % uint32(n))
}
