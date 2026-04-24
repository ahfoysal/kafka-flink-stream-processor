// Package dataflow — M6 stream-stream interval join.
//
// An interval join pairs records from stream A with records from stream B
// that share a key AND fall within a time interval around each other:
//
//     for every a in A, every b in B where
//         a.key == b.key
//         lowerBound <= a.ts - b.ts <= upperBound
//     emit Fn(a, b)
//
// (Flink-style: IntervalJoin. Asymmetric bounds are useful — e.g. "join
// order-created against payment events that happened within the last 5m but
// no more than 30s in the future".)
//
// State layout (bbolt, per partition):
//
//     bucket: "<op>__A__p<part>"   key: <ts_nanos><seq><user_key> -> user_value
//     bucket: "<op>__B__p<part>"   key: <ts_nanos><seq><user_key> -> user_value
//
// Every incoming A-record is buffered and probed against B; every incoming
// B-record is buffered and probed against A. Expired entries (older than
// the longest relevant bound) are GC'd opportunistically on each probe so
// the buffer does not grow without bound on long-running streams.
//
// This sits alongside the M3 JoinOp (which uses a single symmetric ±Bound)
// and offers asymmetric, Flink-style interval semantics.
package dataflow

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ahfoysal/kafka-flink-stream-processor/mvp/internal/state"
)

// IntervalSide identifies which input stream a record belongs to.
type IntervalSide byte

const (
	IntervalSideA IntervalSide = 'A'
	IntervalSideB IntervalSide = 'B'
)

// IntervalJoinOp implements an asymmetric, time-bounded stream-stream join.
//
// For a record a ∈ A and b ∈ B with matching keys:
//
//     LowerBound <= a.Timestamp - b.Timestamp <= UpperBound   ⇒  emit Fn(a, b)
//
// Both bounds may be negative. Typical usage:
//
//     // "for every order, join with payments that arrived
//     //  between 0 and 5 minutes after the order"
//     LowerBound = 0
//     UpperBound = 5 * time.Minute
type IntervalJoinOp struct {
	OpName    string
	Store     *state.Store
	Partition int

	LowerBound time.Duration // a.ts - b.ts >= LowerBound
	UpperBound time.Duration // a.ts - b.ts <= UpperBound

	Fn func(a, b Record) (Record, error)

	seq uint64 // atomic, per-op monotonic; keeps buffer keys unique under ties
	mu  sync.Mutex
	// lastGC throttles the (relatively expensive) scan-and-delete pass.
	lastGC time.Time
}

// IntervalJoinInput wraps a record with its side tag. The runtime is expected
// to merge the two input streams into a single channel of IntervalJoinInput.
type IntervalJoinInput struct {
	Record
	Side IntervalSide
}

func (o *IntervalJoinOp) Name() string { return o.OpName }

// Process ingests one side-tagged record. The caller encodes the side into
// the first two bytes of Record.Value as "A|" or "B|" — matching the
// convention used by the M3 JoinOp so both can share a runtime merge shim.
func (o *IntervalJoinOp) Process(_ context.Context, in Record, out Emitter) error {
	if len(in.Value) < 2 || (in.Value[0] != byte(IntervalSideA) && in.Value[0] != byte(IntervalSideB)) || in.Value[1] != '|' {
		return fmt.Errorf("IntervalJoinOp expects side-prefixed value, got %q", in.Value)
	}
	side := IntervalSide(in.Value[0])
	payload := append([]byte(nil), in.Value[2:]...)

	ts := in.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}

	myBucket := intervalBucket(o.OpName, side)
	otherBucket := intervalBucket(o.OpName, flipInterval(side))

	// Buffer this record so future records on the other side can join it.
	seq := atomic.AddUint64(&o.seq, 1)
	bufKey := makeIntervalKey(ts, seq, in.Key)
	if err := o.Store.Put(myBucket, o.Partition, bufKey, payload); err != nil {
		return err
	}

	// Probe the opposite side's buffer for matches within the interval.
	// We iterate the whole bucket (bbolt's ForEach is cheap for small
	// buffers) and filter by key + interval in Go.
	var emitErr error
	err := o.Store.Iterate(otherBucket, o.Partition, func(k, v []byte) error {
		otherTs, otherKey, ok := parseIntervalKey(k)
		if !ok {
			return nil
		}
		if !bytesEqual(otherKey, in.Key) {
			return nil
		}
		// Compute a.ts - b.ts regardless of which side just arrived.
		var diff time.Duration
		if side == IntervalSideA {
			diff = ts.Sub(otherTs)
		} else {
			diff = otherTs.Sub(ts)
		}
		if diff < o.LowerBound || diff > o.UpperBound {
			return nil
		}

		var a, b Record
		if side == IntervalSideA {
			a = Record{Key: in.Key, Value: payload, Timestamp: ts}
			b = Record{Key: in.Key, Value: append([]byte(nil), v...), Timestamp: otherTs}
		} else {
			b = Record{Key: in.Key, Value: payload, Timestamp: ts}
			a = Record{Key: in.Key, Value: append([]byte(nil), v...), Timestamp: otherTs}
		}
		joined, err := o.Fn(a, b)
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
	if emitErr != nil {
		return emitErr
	}

	// Periodic GC: drop buffer entries older than the longest possibly
	// relevant horizon. For an A arriving at ts, the oldest B it could
	// match had ts_B = ts - UpperBound; anything older than ts - max(|lo|, |up|)
	// on either side can be dropped.
	o.maybeGC(ts)
	return nil
}

func (o *IntervalJoinOp) Finish(context.Context, Emitter) error { return nil }

// maybeGC drops buffer entries with ts < now - horizon on both sides. We
// rate-limit the scan to once per second to keep hot-path cost low.
func (o *IntervalJoinOp) maybeGC(now time.Time) {
	o.mu.Lock()
	if now.Sub(o.lastGC) < time.Second {
		o.mu.Unlock()
		return
	}
	o.lastGC = now
	o.mu.Unlock()

	horizon := o.UpperBound
	if -o.LowerBound > horizon {
		horizon = -o.LowerBound
	}
	if horizon <= 0 {
		return
	}
	cutoff := now.Add(-horizon).UnixNano()

	for _, side := range []IntervalSide{IntervalSideA, IntervalSideB} {
		bucket := intervalBucket(o.OpName, side)
		var stale [][]byte
		_ = o.Store.Iterate(bucket, o.Partition, func(k, _ []byte) error {
			if len(k) < 16 {
				return nil
			}
			ts := int64(binary.BigEndian.Uint64(k[0:8]))
			if ts < cutoff {
				stale = append(stale, append([]byte(nil), k...))
			}
			return nil
		})
		for _, k := range stale {
			_ = o.Store.Delete(bucket, o.Partition, k)
		}
	}
}

// ---- key layout helpers ----------------------------------------------------

func intervalBucket(op string, side IntervalSide) string {
	return fmt.Sprintf("%s__%c", op, side)
}

func flipInterval(s IntervalSide) IntervalSide {
	if s == IntervalSideA {
		return IntervalSideB
	}
	return IntervalSideA
}

// Buffer-key: [8B ts_nanos][8B seq][user_key]. ts-prefixed so iteration is
// already in time order (cheap time-range scans once we add them).
func makeIntervalKey(ts time.Time, seq uint64, userKey []byte) []byte {
	out := make([]byte, 16+len(userKey))
	binary.BigEndian.PutUint64(out[0:8], uint64(ts.UnixNano()))
	binary.BigEndian.PutUint64(out[8:16], seq)
	copy(out[16:], userKey)
	return out
}

func parseIntervalKey(buf []byte) (ts time.Time, userKey []byte, ok bool) {
	if len(buf) < 16 {
		return time.Time{}, nil, false
	}
	tsNs := int64(binary.BigEndian.Uint64(buf[0:8]))
	return time.Unix(0, tsNs), buf[16:], true
}
