// Package state is the M3 keyed-state backend.
//
// Design: per-operator state is sharded by the partition key of the input
// record. We use bbolt (pure-Go, no CGo, no RocksDB) as the on-disk KV. Every
// operator that needs state gets its own bucket per partition, inside a
// single bbolt file per task.
//
// Scope for M3: durable enough to survive a restart and resume from the last
// snapshot. True exactly-once + aligned checkpoints lands in M4.
package state

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Store is a minimal keyed-state KV plus a periodic checkpoint hook.
// It is NOT safe to share a single Store across goroutines writing the same
// bucket concurrently — the runtime owns one Store per task thread.
type Store struct {
	db   *bolt.DB
	path string

	mu       sync.Mutex
	offsets  map[string]uint64 // "topic|partition|group" -> committed offset
	stopCh   chan struct{}
	stopped  bool
	interval time.Duration
}

// Open opens (or creates) a bbolt-backed state file at path. interval
// controls the background snapshot loop; set to 0 to disable.
func Open(path string, interval time.Duration) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("bbolt open %s: %w", path, err)
	}
	s := &Store{
		db:       db,
		path:     path,
		offsets:  map[string]uint64{},
		stopCh:   make(chan struct{}),
		interval: interval,
	}
	// Bootstrap the system buckets.
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{[]byte("__meta"), []byte("__offsets")} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	if err := s.loadOffsets(); err != nil {
		db.Close()
		return nil, err
	}
	if interval > 0 {
		go s.checkpointLoop()
	}
	return s, nil
}

// Close flushes and closes the underlying bbolt file.
func (s *Store) Close() error {
	s.mu.Lock()
	if !s.stopped {
		close(s.stopCh)
		s.stopped = true
	}
	s.mu.Unlock()
	if err := s.Checkpoint(); err != nil {
		return err
	}
	return s.db.Close()
}

// ----------------------------------------------------------------------------
// Keyed state API (used by GroupByKey / Count / Reduce / Join).
// ----------------------------------------------------------------------------

func bucketFor(op string, partition int) []byte {
	return []byte(fmt.Sprintf("op__%s__p%d", op, partition))
}

// Get reads a keyed value for (operator, partition, key).
// ok=false means the key was not found.
func (s *Store) Get(op string, partition int, key []byte) (val []byte, ok bool, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFor(op, partition))
		if b == nil {
			return nil
		}
		v := b.Get(key)
		if v == nil {
			return nil
		}
		// bbolt returns a slice into mmap; copy so we can outlive the tx.
		val = append(val, v...)
		ok = true
		return nil
	})
	return
}

// Put overwrites a keyed value.
func (s *Store) Put(op string, partition int, key, val []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketFor(op, partition))
		if err != nil {
			return err
		}
		return b.Put(key, val)
	})
}

// Delete removes a key from (op, partition). No-op if missing.
func (s *Store) Delete(op string, partition int, key []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFor(op, partition))
		if b == nil {
			return nil
		}
		return b.Delete(key)
	})
}

// GetUint64 / PutUint64 are a typed convenience used by Count.
func (s *Store) GetUint64(op string, partition int, key []byte) (uint64, bool, error) {
	v, ok, err := s.Get(op, partition, key)
	if err != nil || !ok {
		return 0, ok, err
	}
	if len(v) != 8 {
		return 0, true, fmt.Errorf("state: expected 8-byte uint64 for key %q, got %d", key, len(v))
	}
	return binary.BigEndian.Uint64(v), true, nil
}

func (s *Store) PutUint64(op string, partition int, key []byte, v uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return s.Put(op, partition, key, buf[:])
}

// Iterate walks every (key,value) under (op, partition). fn may not mutate
// the underlying buffers — copy if you need to outlive the callback.
func (s *Store) Iterate(op string, partition int, fn func(k, v []byte) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFor(op, partition))
		if b == nil {
			return nil
		}
		return b.ForEach(fn)
	})
}

// ----------------------------------------------------------------------------
// Source offset tracking (so the pipeline can resume after a restart).
// ----------------------------------------------------------------------------

func offsetKey(topic string, partition int, group string) string {
	return fmt.Sprintf("%s|%d|%s", topic, partition, group)
}

// CommitOffset records the next offset to read for a source partition.
// It is staged in memory and flushed by the periodic checkpoint.
func (s *Store) CommitOffset(topic string, partition int, group string, nextOffset uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offsets[offsetKey(topic, partition, group)] = nextOffset
}

// LoadOffset reads the last-committed next offset (0 = start from beginning).
func (s *Store) LoadOffset(topic string, partition int, group string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.offsets[offsetKey(topic, partition, group)]
}

func (s *Store) loadOffsets() error {
	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("__offsets"))
		if b == nil {
			return nil
		}
		raw := b.Get([]byte("v1"))
		if raw == nil {
			return nil
		}
		var m map[string]uint64
		if err := json.Unmarshal(raw, &m); err != nil {
			return err
		}
		s.offsets = m
		return nil
	})
}

// ----------------------------------------------------------------------------
// Checkpoint stub — M4 will replace this with aligned barriers + 2PC.
// For now we just atomically snapshot in-memory offsets to bbolt every tick.
// Keyed state writes are already durable because each Put is its own tx.
// ----------------------------------------------------------------------------

// DB returns the underlying bbolt handle. The checkpoint subsystem needs
// direct access to compose multi-bucket transactions. Callers should only
// use this for Update/View; never close the returned DB.
func (s *Store) DB() *bolt.DB { return s.db }

// Offsets returns a snapshot of the in-memory offset cursor. The
// checkpoint subsystem writes this into __offsets as part of its 2PC.
func (s *Store) Offsets() map[string]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]uint64, len(s.offsets))
	for k, v := range s.offsets {
		out[k] = v
	}
	return out
}

// ReloadOffsets re-reads __offsets from disk into the in-memory cursor.
// Used on recovery: after a crash the task must resume from what was
// durably committed, not from whatever the dying process had staged.
func (s *Store) ReloadOffsets() error {
	s.mu.Lock()
	s.offsets = map[string]uint64{}
	s.mu.Unlock()
	return s.loadOffsets()
}

// Checkpoint writes current source offsets into the __offsets bucket.
func (s *Store) Checkpoint() error {
	s.mu.Lock()
	snap := make(map[string]uint64, len(s.offsets))
	for k, v := range s.offsets {
		snap[k] = v
	}
	s.mu.Unlock()
	raw, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("__offsets"))
		if b == nil {
			var err error
			b, err = tx.CreateBucket([]byte("__offsets"))
			if err != nil {
				return err
			}
		}
		return b.Put([]byte("v1"), raw)
	})
}

func (s *Store) checkpointLoop() {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-t.C:
			_ = s.Checkpoint() // best-effort; M4 will surface errors via the task manager
		}
	}
}
