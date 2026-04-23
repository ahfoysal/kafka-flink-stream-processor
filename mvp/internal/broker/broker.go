// Package broker exposes the append-only log over a minimal HTTP API.
//
// Endpoints:
//
//	POST /produce?topic=X        body = raw bytes payload; returns {"offset":N}
//	GET  /consume?topic=X&group=G&max=N
//	                              returns {"records":[{"offset":..,"payload":"base64"}],"next":N}
//	                              commits next offset on success (at-least-once).
//	GET  /offset?topic=X&group=G -> {"offset":N}
//	GET  /health                 -> ok
package broker

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	stdlog "log"
	"net/http"
	"strconv"
	"sync"

	mvplog "github.com/ahfoysal/kafka-flink-stream-processor/mvp/internal/log"
)

type Broker struct {
	dataDir string

	mu     sync.Mutex
	topics map[string]*mvplog.Topic
	offs   *mvplog.OffsetStore
}

func New(dataDir string) (*Broker, error) {
	offs, err := mvplog.NewOffsetStore(dataDir)
	if err != nil {
		return nil, err
	}
	return &Broker{
		dataDir: dataDir,
		topics:  make(map[string]*mvplog.Topic),
		offs:    offs,
	}, nil
}

func (b *Broker) getTopic(name string) (*mvplog.Topic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if t, ok := b.topics[name]; ok {
		return t, nil
	}
	t, err := mvplog.OpenTopic(b.dataDir, name)
	if err != nil {
		return nil, err
	}
	b.topics[name] = t
	return t, nil
}

func (b *Broker) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/produce", b.handleProduce)
	mux.HandleFunc("/consume", b.handleConsume)
	mux.HandleFunc("/offset", b.handleOffset)
	return mux
}

func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var firstErr error
	for _, t := range b.topics {
		if err := t.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (b *Broker) handleProduce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "missing topic", http.StatusBadRequest)
		return
	}
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	t, err := b.getTopic(topic)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	off, err := t.Append(payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"offset": off})
}

type consumeRecord struct {
	Offset  uint64 `json:"offset"`
	Payload string `json:"payload"` // base64
}

func (b *Broker) handleConsume(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	group := r.URL.Query().Get("group")
	if topic == "" || group == "" {
		http.Error(w, "missing topic or group", http.StatusBadRequest)
		return
	}
	maxStr := r.URL.Query().Get("max")
	maxRecords := 100
	if maxStr != "" {
		if n, err := strconv.Atoi(maxStr); err == nil && n > 0 {
			maxRecords = n
		}
	}
	t, err := b.getTopic(topic)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	start, err := b.offs.Get(topic, group)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	recs, next, err := t.Read(start, maxRecords)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// At-least-once: commit offset AFTER we've handed the data to the client
	// (MVP: the ack is implicit in receiving a 2xx; a real design uses a
	// separate /commit call from the consumer).
	if next > start {
		if err := b.offs.Commit(topic, group, next); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	out := make([]consumeRecord, 0, len(recs))
	for _, rec := range recs {
		out = append(out, consumeRecord{
			Offset:  rec.Offset,
			Payload: base64.StdEncoding.EncodeToString(rec.Payload),
		})
	}
	writeJSON(w, map[string]any{"records": out, "next": next})
}

func (b *Broker) handleOffset(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	group := r.URL.Query().Get("group")
	if topic == "" || group == "" {
		http.Error(w, "missing topic or group", http.StatusBadRequest)
		return
	}
	off, err := b.offs.Get(topic, group)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"offset": off})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		stdlog.Printf("writeJSON: %v", err)
	}
}

// Listen is a convenience used by the broker main.
func (b *Broker) Listen(addr string) error {
	stdlog.Printf("broker listening on %s (data=%s)", addr, b.dataDir)
	s := &http.Server{Addr: addr, Handler: b.Handler()}
	return s.ListenAndServe()
}

// SanityError helps main surface config problems.
func SanityError(msg string) error { return fmt.Errorf("broker: %s", msg) }
