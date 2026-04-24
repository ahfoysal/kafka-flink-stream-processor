package checkpoint_test

import (
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/ahfoysal/kafka-flink-stream-processor/mvp/internal/checkpoint"
)

// Verify the 2PC state machine end-to-end on a fresh bbolt file.
//
//  1. Stage two pending outputs for cp=1.
//  2. WritePrepared -> cp row exists with PREPARED.
//  3. WriteCommitted -> cp row exists with COMMITTED, pending rows gone.
//  4. IterPending is empty.
//  5. LastCommitted returns cp=1.
func TestPrepareCommitCycle(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "t.db")
	db, err := bolt.Open(dbPath, 0o600, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Bootstrap buckets.
	if err := db.Update(func(tx *bolt.Tx) error { return checkpoint.EnsureBuckets(tx) }); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	// Stage pending rows for cp=1.
	err = db.Update(func(tx *bolt.Tx) error {
		if err := checkpoint.StagePendingOutput(tx, 1, 1, checkpoint.PendingOutput{
			Topic: "sink", Key: []byte("k1"), Value: []byte("v1"),
			DedupID: "task0|1|1", Seq: 1,
		}); err != nil {
			return err
		}
		return checkpoint.StagePendingOutput(tx, 1, 2, checkpoint.PendingOutput{
			Topic: "sink", Value: []byte("v2"), DedupID: "task0|1|2", Seq: 2,
		})
	})
	if err != nil {
		t.Fatalf("stage: %v", err)
	}

	// Phase 1: prepare.
	offs := map[string]uint64{"topic|0|grp": 100}
	err = db.Update(func(tx *bolt.Tx) error {
		return checkpoint.WritePrepared(tx, checkpoint.CheckpointRow{ID: 1, Offsets: offs}, offs)
	})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// Phase 2: commit.
	err = db.Update(func(tx *bolt.Tx) error {
		return checkpoint.WriteCommitted(tx, 1, time.Now())
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Pending must be empty.
	var nPending int
	err = db.View(func(tx *bolt.Tx) error {
		return checkpoint.IterPending(tx, func(cpID, seq uint64, po checkpoint.PendingOutput) error {
			nPending++
			return nil
		})
	})
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	if nPending != 0 {
		t.Fatalf("pending not cleared: %d", nPending)
	}

	// LastCommitted should return cp=1.
	err = db.View(func(tx *bolt.Tx) error {
		row, ok, err := checkpoint.LastCommitted(tx)
		if err != nil {
			return err
		}
		if !ok {
			t.Fatalf("no committed row")
		}
		if row.ID != 1 {
			t.Fatalf("want id=1, got %d", row.ID)
		}
		if row.Status != checkpoint.StatusCommitted {
			t.Fatalf("want COMMITTED, got %v", row.Status)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("last: %v", err)
	}
}

// Simulate a crash between prepare and commit: pending rows survive, replay
// via IterPending sees them, dedup check prevents double-delivery.
func TestCrashReplayDedup(t *testing.T) {
	dir := t.TempDir()
	db, err := bolt.Open(filepath.Join(dir, "t.db"), 0o600, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	_ = db.Update(func(tx *bolt.Tx) error { return checkpoint.EnsureBuckets(tx) })

	// Stage + prepare (NO commit = crash).
	err = db.Update(func(tx *bolt.Tx) error {
		if err := checkpoint.StagePendingOutput(tx, 1, 1, checkpoint.PendingOutput{
			Topic: "sink", Value: []byte("hi"), DedupID: "p0|1|1", Seq: 1,
		}); err != nil {
			return err
		}
		return checkpoint.WritePrepared(tx, checkpoint.CheckpointRow{ID: 1}, map[string]uint64{})
	})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// Restart: iterate pending; first time not seen, second time seen.
	var deliveries int
	err = db.View(func(tx *bolt.Tx) error {
		return checkpoint.IterPending(tx, func(cp, seq uint64, po checkpoint.PendingOutput) error {
			seen, _ := checkpoint.SeenDedup(tx, po.Topic, po.DedupID)
			if !seen {
				deliveries++
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	if deliveries != 1 {
		t.Fatalf("want 1 delivery, got %d", deliveries)
	}
	// Mark seen, then iterate again — must be zero.
	_ = db.Update(func(tx *bolt.Tx) error {
		return checkpoint.MarkDedup(tx, "sink", "p0|1|1")
	})
	deliveries = 0
	err = db.View(func(tx *bolt.Tx) error {
		return checkpoint.IterPending(tx, func(cp, seq uint64, po checkpoint.PendingOutput) error {
			seen, _ := checkpoint.SeenDedup(tx, po.Topic, po.DedupID)
			if !seen {
				deliveries++
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("iter2: %v", err)
	}
	if deliveries != 0 {
		t.Fatalf("replay should dedup, got %d deliveries", deliveries)
	}
}
