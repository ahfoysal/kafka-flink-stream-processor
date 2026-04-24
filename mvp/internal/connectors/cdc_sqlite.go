// Package connectors — M6 CDC source connector (SQLite flavour).
//
// Debezium-style change-data-capture: polls a SQLite table at a configured
// interval, diffs the current snapshot against the last snapshot, and emits
// one change event per insert / update / delete into a Kafka-compatible
// topic on the M2 broker.
//
// This is the "M6 simple" variant deliberately: real Debezium tails the
// write-ahead log / logical replication stream. SQLite's WAL is per-page
// binary and not usable as a logical change feed without extensions, so
// this connector does polling-with-diff instead. The emitted event shape
// matches Debezium's {op, before, after, source} envelope so downstream
// consumers don't need to know how the events were produced.
//
// Event payload (JSON):
//
//     {
//       "op":     "c" | "u" | "d",         // create / update / delete
//       "before": {col: val, ...} | null,   // row prior to the change (u/d)
//       "after":  {col: val, ...} | null,   // row after the change (c/u)
//       "source": {"db": "...", "table": "...", "ts_ms": 1712345678901}
//     }
//
// Each change event is produced to the configured topic keyed by the
// primary-key value (stringified) so downstream hash-partitioning keeps
// updates for the same row on a single partition — necessary for correct
// ordering into a materialized view.
package connectors

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Producer is the minimal interface the connector needs from the broker
// client. Matches (*dataflow.BrokerView).Produce exactly so the real
// broker client can be passed in directly — but the interface keeps this
// package independent of internal/dataflow (no import cycle risk).
type Producer interface {
	Produce(topic string, key, payload []byte, rr int) (int, error)
}

// Config parameters for a SQLite CDC source.
type Config struct {
	// DSN is the SQLite connection string, e.g. "file:/tmp/cdc.db".
	DSN string
	// Table to watch.
	Table string
	// PKColumn is the primary-key column (used as the event key).
	PKColumn string
	// Topic to emit change events to.
	Topic string
	// PollInterval — how often to snapshot+diff. 1s is a reasonable default.
	PollInterval time.Duration
	// Producer is the broker producer.
	Producer Producer
	// Logger, optional. If nil, logs are discarded.
	Logger func(format string, args ...any)
}

// Source is a running CDC poller. Start it with Run; it stops when the
// context is cancelled.
type Source struct {
	cfg      Config
	db       *sql.DB
	snapshot map[string]map[string]any // pk -> row
	rr       int
}

// New opens the SQLite database and prepares a Source. The caller must
// call Run to start polling.
func New(cfg Config) (*Source, error) {
	if cfg.DSN == "" || cfg.Table == "" || cfg.PKColumn == "" || cfg.Topic == "" {
		return nil, fmt.Errorf("cdc_sqlite: DSN, Table, PKColumn, Topic are required")
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.Producer == nil {
		return nil, fmt.Errorf("cdc_sqlite: Producer is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = func(string, ...any) {}
	}
	db, err := sql.Open("sqlite", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("cdc_sqlite: open: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("cdc_sqlite: ping: %w", err)
	}
	return &Source{
		cfg:      cfg,
		db:       db,
		snapshot: map[string]map[string]any{},
	}, nil
}

// Close releases the underlying DB handle.
func (s *Source) Close() error { return s.db.Close() }

// Run polls the table at cfg.PollInterval and emits change events until
// the context is cancelled. On the very first tick every row is emitted
// as an "r" (read / initial-snapshot) event so downstream materialized
// views can bootstrap themselves from an empty state.
func (s *Source) Run(ctx context.Context) error {
	// Initial snapshot: emit every current row as a create-equivalent
	// so consumers can build state from scratch.
	first, err := s.readAll(ctx)
	if err != nil {
		return fmt.Errorf("cdc_sqlite: initial snapshot: %w", err)
	}
	for pk, row := range first {
		if err := s.emit(ctx, "c", nil, row); err != nil {
			return err
		}
		s.snapshot[pk] = row
	}
	s.cfg.Logger("cdc_sqlite: initial snapshot emitted rows=%d table=%s topic=%s",
		len(first), s.cfg.Table, s.cfg.Topic)

	t := time.NewTicker(s.cfg.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := s.pollOnce(ctx); err != nil {
				// Best-effort: log and continue. Transient errors shouldn't
				// kill the connector.
				s.cfg.Logger("cdc_sqlite: poll error: %v", err)
			}
		}
	}
}

// pollOnce snapshots the table, diffs against the previous snapshot,
// and emits c/u/d events accordingly.
func (s *Source) pollOnce(ctx context.Context) error {
	cur, err := s.readAll(ctx)
	if err != nil {
		return err
	}

	// Inserts and updates.
	for pk, row := range cur {
		prev, had := s.snapshot[pk]
		if !had {
			if err := s.emit(ctx, "c", nil, row); err != nil {
				return err
			}
			continue
		}
		if !rowsEqual(prev, row) {
			if err := s.emit(ctx, "u", prev, row); err != nil {
				return err
			}
		}
	}
	// Deletes: in previous snapshot but not in current.
	for pk, prev := range s.snapshot {
		if _, stillThere := cur[pk]; !stillThere {
			if err := s.emit(ctx, "d", prev, nil); err != nil {
				return err
			}
		}
	}
	s.snapshot = cur
	return nil
}

// readAll pulls the entire table into a map[pk]row. For M6-demo scale this
// is fine; production would tail the WAL / logical replication stream.
func (s *Source) readAll(ctx context.Context) (map[string]map[string]any, error) {
	q := fmt.Sprintf("SELECT * FROM %s", quoteIdent(s.cfg.Table))
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := make(map[string]map[string]any, 64)
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]any, len(cols))
		var pk string
		for i, c := range cols {
			v := normalize(vals[i])
			row[c] = v
			if c == s.cfg.PKColumn {
				pk = fmt.Sprintf("%v", v)
			}
		}
		if pk == "" {
			return nil, fmt.Errorf("cdc_sqlite: row missing PK column %q", s.cfg.PKColumn)
		}
		out[pk] = row
	}
	return out, rows.Err()
}

// emit marshals the envelope and produces to the broker.
func (s *Source) emit(_ context.Context, op string, before, after map[string]any) error {
	env := map[string]any{
		"op":     op,
		"before": before,
		"after":  after,
		"source": map[string]any{
			"db":    s.cfg.DSN,
			"table": s.cfg.Table,
			"ts_ms": time.Now().UnixMilli(),
		},
	}
	payload, err := json.Marshal(env)
	if err != nil {
		return err
	}
	var keyRow map[string]any
	if after != nil {
		keyRow = after
	} else {
		keyRow = before
	}
	pk := ""
	if v, ok := keyRow[s.cfg.PKColumn]; ok {
		pk = fmt.Sprintf("%v", v)
	}
	s.rr++
	if _, err := s.cfg.Producer.Produce(s.cfg.Topic, []byte(pk), payload, s.rr); err != nil {
		return fmt.Errorf("cdc_sqlite: produce: %w", err)
	}
	return nil
}

// ---- helpers --------------------------------------------------------------

// normalize coerces driver-returned values into JSON-friendly types. The
// modernc.org/sqlite driver returns []byte for TEXT in some configurations;
// we flatten those to strings for stable envelope hashing.
func normalize(v any) any {
	switch x := v.(type) {
	case []byte:
		return string(x)
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	default:
		return x
	}
}

// rowsEqual compares two rows by their JSON-normalized fields. We stable-
// sort column names to get a deterministic serialization — cheap for the
// small row widths typical of CDC.
func rowsEqual(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	keys := make([]string, 0, len(a))
	for k := range a {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		av, bv := a[k], b[k]
		if fmt.Sprintf("%v", av) != fmt.Sprintf("%v", bv) {
			return false
		}
	}
	return true
}

// quoteIdent wraps an identifier in double quotes, doubling any embedded
// double-quote character (SQL standard). Defends against "weird" table
// names but not against true injection — callers control the config.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
