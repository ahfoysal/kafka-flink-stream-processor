package sql

import (
	"testing"
	"time"
)

func TestParseBasic(t *testing.T) {
	src := `SELECT word, COUNT(*)
	FROM lines
	GROUP BY word, TUMBLE(event_time, INTERVAL '5' SECOND)
	EMIT ON WATERMARK
	INTO counts`
	q, err := Parse(src)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if q.KeyCol != "word" {
		t.Errorf("key col: got %q, want %q", q.KeyCol, "word")
	}
	if q.SourceTopic != "lines" {
		t.Errorf("source: got %q, want %q", q.SourceTopic, "lines")
	}
	if q.TsCol != "event_time" {
		t.Errorf("ts col: got %q, want %q", q.TsCol, "event_time")
	}
	if q.WindowSize != 5*time.Second {
		t.Errorf("window: got %v, want 5s", q.WindowSize)
	}
	if q.SinkTopic != "counts" {
		t.Errorf("sink: got %q, want %q", q.SinkTopic, "counts")
	}
}

func TestParseNoSink(t *testing.T) {
	q, err := Parse(`SELECT k, COUNT(*) FROM t GROUP BY k, TUMBLE(ts, INTERVAL '100' MILLISECOND) EMIT ON WATERMARK`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if q.SinkTopic != "" {
		t.Errorf("expected empty sink, got %q", q.SinkTopic)
	}
	if q.WindowSize != 100*time.Millisecond {
		t.Errorf("window: %v", q.WindowSize)
	}
}

func TestParseErrors(t *testing.T) {
	bad := []string{
		`SELECT k FROM t`, // missing COUNT(*)
		`SELECT k, COUNT(*) FROM t GROUP BY other, TUMBLE(ts, INTERVAL '5' SECOND) EMIT ON WATERMARK`,
		`SELECT k, COUNT(*) FROM t GROUP BY k, HOP(ts, INTERVAL '5' SECOND) EMIT ON WATERMARK`,
		``,
	}
	for _, s := range bad {
		if _, err := Parse(s); err == nil {
			t.Errorf("expected error for %q", s)
		}
	}
}
