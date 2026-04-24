// Package view implements M6 materialized views — the KStream → KTable half
// of the processor. Where Count/Reduce emit a changelog stream, a View
// absorbs that changelog into a queryable key/value snapshot backed by
// bbolt, and exposes interactive queries over HTTP.
//
// Semantics (KTable-style):
//
//   - Insert/Update:   the View.Upsert call replaces the value for key k.
//   - Delete (tombstone): value == nil deletes the key.
//   - Aggregations (count, sum) are built as thin wrappers around Upsert:
//     the caller computes the new aggregate and writes it.
//
// Wire format on disk (bbolt):
//
//     bucket: "view__<name>"    key: <user_key> -> <raw_value_bytes>
//
// The interactive query HTTP API is a single endpoint:
//
//     GET /view/<name>?key=<k>     → 200 {"key":"k","value":"v"}  or 404
//     GET /view/<name>             → 200 {"name":"...","size":N}  (summary)
//
// Values are returned as plain strings; the aggregation helpers encode
// numeric accumulators as decimal ASCII for readability.
package view

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	bolt "go.etcd.io/bbolt"
)

// View is a single named materialized view backed by a bbolt bucket.
type View struct {
	name string
	db   *bolt.DB
}

// Registry holds every view registered with the HTTP server. Views share one
// bbolt file for operational simplicity — each view is just a bucket.
type Registry struct {
	db    *bolt.DB
	mu    sync.RWMutex
	views map[string]*View
}

// Open creates (or attaches to) a bbolt file backing all views in this registry.
func Open(path string) (*Registry, error) {
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("view: open %s: %w", path, err)
	}
	return &Registry{db: db, views: map[string]*View{}}, nil
}

// Close flushes and closes the underlying bbolt file.
func (r *Registry) Close() error { return r.db.Close() }

// Register returns a View handle for the given name, creating the backing
// bucket on first use. Registering the same name twice is idempotent.
func (r *Registry) Register(name string) (*View, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if v, ok := r.views[name]; ok {
		return v, nil
	}
	err := r.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName(name)))
		return err
	})
	if err != nil {
		return nil, err
	}
	v := &View{name: name, db: r.db}
	r.views[name] = v
	return v, nil
}

// Get looks up a view by name. ok=false if it was not registered.
func (r *Registry) Get(name string) (*View, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.views[name]
	return v, ok
}

// Names returns the set of registered view names (for introspection).
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.views))
	for n := range r.views {
		out = append(out, n)
	}
	return out
}

func bucketName(view string) string { return "view__" + view }

// ---- View — primitive ops -------------------------------------------------

// Name returns the view's logical name.
func (v *View) Name() string { return v.name }

// Upsert writes value for key. If value is nil the key is deleted (tombstone).
func (v *View) Upsert(key, value []byte) error {
	return v.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName(v.name)))
		if b == nil {
			return fmt.Errorf("view %q: bucket missing", v.name)
		}
		if value == nil {
			return b.Delete(key)
		}
		return b.Put(key, value)
	})
}

// Lookup reads the value for key. ok=false if missing.
func (v *View) Lookup(key []byte) (value []byte, ok bool, err error) {
	err = v.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName(v.name)))
		if b == nil {
			return nil
		}
		raw := b.Get(key)
		if raw == nil {
			return nil
		}
		value = append(value, raw...)
		ok = true
		return nil
	})
	return
}

// Size returns the number of keys in the view. O(bucket) — meant for debug
// / small admin responses, not hot-path use.
func (v *View) Size() (int, error) {
	n := 0
	err := v.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName(v.name)))
		if b == nil {
			return nil
		}
		return b.ForEach(func(_, _ []byte) error { n++; return nil })
	})
	return n, err
}

// ---- Aggregation helpers --------------------------------------------------

// IncrCount bumps a uint64 counter at key by 1 and returns the new value.
// Values are stored as 8-byte big-endian. Safe against concurrent callers
// because bbolt serializes writes.
func (v *View) IncrCount(key []byte) (uint64, error) {
	var next uint64
	err := v.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName(v.name)))
		if b == nil {
			return fmt.Errorf("view %q: bucket missing", v.name)
		}
		cur := uint64(0)
		if raw := b.Get(key); len(raw) == 8 {
			cur = binary.BigEndian.Uint64(raw)
		}
		next = cur + 1
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], next)
		return b.Put(key, buf[:])
	})
	return next, err
}

// AddSum adds delta to an int64 sum at key and returns the new total.
func (v *View) AddSum(key []byte, delta int64) (int64, error) {
	var next int64
	err := v.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName(v.name)))
		if b == nil {
			return fmt.Errorf("view %q: bucket missing", v.name)
		}
		cur := int64(0)
		if raw := b.Get(key); len(raw) == 8 {
			cur = int64(binary.BigEndian.Uint64(raw))
		}
		next = cur + delta
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(next))
		return b.Put(key, buf[:])
	})
	return next, err
}

// LookupUint64 decodes an 8-byte counter value.
func (v *View) LookupUint64(key []byte) (uint64, bool, error) {
	raw, ok, err := v.Lookup(key)
	if err != nil || !ok {
		return 0, ok, err
	}
	if len(raw) != 8 {
		return 0, true, fmt.Errorf("view %q key %q: not a uint64 (len=%d)", v.name, key, len(raw))
	}
	return binary.BigEndian.Uint64(raw), true, nil
}

// LookupInt64 decodes an 8-byte signed sum.
func (v *View) LookupInt64(key []byte) (int64, bool, error) {
	u, ok, err := v.LookupUint64(key)
	return int64(u), ok, err
}

// ---- HTTP interactive query API -------------------------------------------

// Handler returns an http.Handler that serves `/view/<name>[?key=K]`.
// Mount at /view/ (with trailing slash) on your mux:
//
//     mux.Handle("/view/", reg.Handler())
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		path := strings.TrimPrefix(req.URL.Path, "/view/")
		path = strings.Trim(path, "/")
		if path == "" {
			r.writeJSON(w, 200, map[string]any{"views": r.Names()})
			return
		}
		name := path
		v, ok := r.Get(name)
		if !ok {
			r.writeJSON(w, 404, map[string]string{"error": "view not found: " + name})
			return
		}
		key := req.URL.Query().Get("key")
		if key == "" {
			n, err := v.Size()
			if err != nil {
				r.writeJSON(w, 500, map[string]string{"error": err.Error()})
				return
			}
			r.writeJSON(w, 200, map[string]any{"name": v.Name(), "size": n})
			return
		}
		raw, ok, err := v.Lookup([]byte(key))
		if err != nil {
			r.writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		if !ok {
			r.writeJSON(w, 404, map[string]string{"error": "key not found: " + key})
			return
		}
		// Prefer a decoded uint64 rendering for 8-byte values (the common
		// aggregation case); otherwise hand back the raw string.
		resp := map[string]any{"key": key, "value": string(raw)}
		if len(raw) == 8 {
			resp["uint64"] = strconv.FormatUint(binary.BigEndian.Uint64(raw), 10)
		}
		r.writeJSON(w, 200, resp)
	})
}

func (r *Registry) writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}
