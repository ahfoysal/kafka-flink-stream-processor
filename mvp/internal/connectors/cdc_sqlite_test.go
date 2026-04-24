package connectors

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// fakeProducer captures produced records in-memory.
type fakeProducer struct {
	mu   sync.Mutex
	msgs []produced
}
type produced struct {
	Topic string
	Key   string
	Body  map[string]any
}

func (p *fakeProducer) Produce(topic string, key, payload []byte, _ int) (int, error) {
	var body map[string]any
	_ = json.Unmarshal(payload, &body)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.msgs = append(p.msgs, produced{Topic: topic, Key: string(key), Body: body})
	return 0, nil
}

func (p *fakeProducer) snapshot() []produced {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]produced, len(p.msgs))
	copy(out, p.msgs)
	return out
}

func TestCDCInsertUpdateDelete(t *testing.T) {
	dir := t.TempDir()
	dsn := "file:" + filepath.Join(dir, "cdc.db")

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO users (id, name, score) VALUES (1, 'alice', 10), (2, 'bob', 20)`); err != nil {
		t.Fatal(err)
	}

	prod := &fakeProducer{}
	src, err := New(Config{
		DSN:          dsn,
		Table:        "users",
		PKColumn:     "id",
		Topic:        "users.cdc",
		PollInterval: 50 * time.Millisecond,
		Producer:     prod,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		_ = src.Run(ctx)
		close(done)
	}()

	// Wait for initial snapshot (2 creates).
	waitFor(t, func() bool { return len(prod.snapshot()) >= 2 }, time.Second)

	// Update alice's score.
	if _, err := db.Exec(`UPDATE users SET score = 42 WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return hasOp(prod.snapshot(), "u") }, 2*time.Second)

	// Delete bob.
	if _, err := db.Exec(`DELETE FROM users WHERE id = 2`); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return hasOp(prod.snapshot(), "d") }, 2*time.Second)

	// Insert a new row.
	if _, err := db.Exec(`INSERT INTO users (id, name, score) VALUES (3, 'carol', 7)`); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		snap := prod.snapshot()
		count := 0
		for _, m := range snap {
			if m.Body["op"] == "c" {
				count++
			}
		}
		return count >= 3 // 2 initial + 1 new
	}, 2*time.Second)

	cancel()
	<-done

	// All messages should be keyed by PK.
	for _, m := range prod.snapshot() {
		if m.Key == "" {
			t.Fatalf("empty key in %+v", m)
		}
		if m.Topic != "users.cdc" {
			t.Fatalf("wrong topic: %q", m.Topic)
		}
	}
}

func hasOp(msgs []produced, op string) bool {
	for _, m := range msgs {
		if m.Body["op"] == op {
			return true
		}
	}
	return false
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}
