package dataflow

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ahfoysal/kafka-flink-stream-processor/mvp/internal/state"
)

type captureEmitter struct {
	mu   sync.Mutex
	recs []Record
}

func (c *captureEmitter) Emit(r Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recs = append(c.recs, r.Copy())
	return nil
}

func (c *captureEmitter) snapshot() []Record {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Record, len(c.recs))
	copy(out, c.recs)
	return out
}

// Feed an A then a B within bound -> 1 emission.
// Feed an A then a B outside bound -> 0 emissions.
func TestIntervalJoinAsymmetricBounds(t *testing.T) {
	dir := t.TempDir()
	st, err := state.Open(filepath.Join(dir, "s.bolt"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	op := &IntervalJoinOp{
		OpName:     "oj",
		Store:      st,
		Partition:  0,
		LowerBound: 0,
		UpperBound: 5 * time.Minute,
		Fn: func(a, b Record) (Record, error) {
			return Record{
				Key:   a.Key,
				Value: []byte(fmt.Sprintf("%s+%s", a.Value, b.Value)),
			}, nil
		},
	}
	out := &captureEmitter{}
	t0 := time.Now()

	// B at t0.  A at t0+30s -> diff = +30s in [0, 5m] -> match.
	must(t, op.Process(context.Background(),
		Record{Key: []byte("k1"), Value: []byte("B|pay-1"), Timestamp: t0},
		out))
	must(t, op.Process(context.Background(),
		Record{Key: []byte("k1"), Value: []byte("A|order-1"), Timestamp: t0.Add(30 * time.Second)},
		out))

	if len(out.snapshot()) != 1 {
		t.Fatalf("want 1 emission, got %d: %+v", len(out.snapshot()), out.snapshot())
	}
	got := string(out.snapshot()[0].Value)
	if got != "order-1+pay-1" {
		t.Fatalf("unexpected payload: %q", got)
	}

	// B at t0 - 10m (well outside UpperBound from A at t0+30s): no new match.
	must(t, op.Process(context.Background(),
		Record{Key: []byte("k2"), Value: []byte("B|old"), Timestamp: t0.Add(-10 * time.Minute)},
		out))
	must(t, op.Process(context.Background(),
		Record{Key: []byte("k2"), Value: []byte("A|order-2"), Timestamp: t0.Add(30 * time.Second)},
		out))

	if len(out.snapshot()) != 1 {
		t.Fatalf("expected stale B to not match, got %d emissions", len(out.snapshot()))
	}

	// Different keys must not match even within bound.
	must(t, op.Process(context.Background(),
		Record{Key: []byte("kA"), Value: []byte("A|order-3"), Timestamp: t0},
		out))
	must(t, op.Process(context.Background(),
		Record{Key: []byte("kB"), Value: []byte("B|pay-3"), Timestamp: t0},
		out))

	if len(out.snapshot()) != 1 {
		t.Fatalf("expected no cross-key matches, got %d", len(out.snapshot()))
	}
}

// B arriving before A (within bound) should still match — both sides probe.
func TestIntervalJoinBArrivesFirst(t *testing.T) {
	dir := t.TempDir()
	st, err := state.Open(filepath.Join(dir, "s.bolt"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	op := &IntervalJoinOp{
		OpName: "oj", Store: st, Partition: 0,
		LowerBound: -1 * time.Minute,
		UpperBound: 1 * time.Minute,
		Fn: func(a, b Record) (Record, error) {
			return Record{Key: a.Key, Value: append(append([]byte{}, a.Value...), b.Value...)}, nil
		},
	}
	out := &captureEmitter{}
	t0 := time.Now()
	must(t, op.Process(context.Background(),
		Record{Key: []byte("k"), Value: []byte("A|a1"), Timestamp: t0}, out))
	must(t, op.Process(context.Background(),
		Record{Key: []byte("k"), Value: []byte("B|b1"), Timestamp: t0.Add(30 * time.Second)}, out))
	if len(out.snapshot()) != 1 {
		t.Fatalf("want 1, got %d", len(out.snapshot()))
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
