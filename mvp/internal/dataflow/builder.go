package dataflow

import (
	"fmt"
	"time"
)

// StreamGraph is what the builder API produces. It is linear (one chain of
// operators) in M3 — branching will come with multi-output DAGs in M4.
type StreamGraph struct {
	SourceTopic string
	SourceGroup string

	// Ops are applied in order; each record flows through all of them.
	Ops []opSpec

	// Sink, if set, is the final output topic. Nil means "emit to stdout".
	SinkTopic string

	// Joined is set when the graph has a second source (stream-stream join).
	Joined     *joinSpec
	BrokerAddr string
	StatePath  string

	CheckpointInterval time.Duration

	// --- M4 event-time configuration ---
	// Extractor pulls event-time out of each record. If nil, falls back to
	// IngestionTime (Record.Timestamp, which is wall-clock at the source).
	Extractor TimestampExtractor
	// AllowedLateness: records with event-time < watermark - allowed_late
	// are dropped (or sent to LateFn if set). Also controls when windows fire:
	// window_end + allowed_late <= watermark.
	AllowedLateness time.Duration
	// MaxOutOfOrderness is passed to the source-side watermark generator:
	// watermark = max_event_time_seen - max_out_of_orderness.
	MaxOutOfOrderness time.Duration
	// WatermarkInterval controls how often the source emits a watermark
	// sentinel downstream. Defaults to 200ms.
	WatermarkInterval time.Duration
	// LateFn, if set, receives every record dropped as too-late.
	LateFn func(Record)
}

type opSpec struct {
	kind   string // "map", "filter", "flatmap", "groupby", "count", "reduce", "join", "tumbling", "sliding", "session"
	name   string
	mapFn  func(Record) (Record, error)
	filt   func(Record) bool
	fm     func(Record) ([]Record, error)
	keyFn  func(Record) []byte
	redFn  func(acc, v []byte) ([]byte, error)
	redIni func(v []byte) ([]byte, error)

	// Windowing fields (M4).
	winSize  time.Duration
	winSlide time.Duration
	winGap   time.Duration
}

type joinSpec struct {
	rightTopic string
	rightGroup string
	bound      time.Duration
	fn         func(left, right Record) (Record, error)
}

// ---- builder fluent API ----------------------------------------------------

// Source starts a new StreamGraph reading from topic. group is the consumer
// group used for offset tracking. Each call to Build() then Run() spawns one
// task per input partition.
func Source(brokerAddr, topic, group string) *StreamGraph {
	return &StreamGraph{
		SourceTopic:        topic,
		SourceGroup:        group,
		BrokerAddr:         brokerAddr,
		CheckpointInterval: 2 * time.Second,
	}
}

// WithState configures where bbolt state files live on disk. One file per
// task (i.e. per input partition). Default: "state-<topic>-<group>".
func (g *StreamGraph) WithState(path string) *StreamGraph {
	g.StatePath = path
	return g
}

// WithCheckpointInterval sets the background snapshot interval (M4 will turn
// this into aligned-barrier checkpointing).
func (g *StreamGraph) WithCheckpointInterval(d time.Duration) *StreamGraph {
	g.CheckpointInterval = d
	return g
}

func (g *StreamGraph) Map(name string, fn func(Record) (Record, error)) *StreamGraph {
	g.Ops = append(g.Ops, opSpec{kind: "map", name: name, mapFn: fn})
	return g
}

func (g *StreamGraph) Filter(name string, fn func(Record) bool) *StreamGraph {
	g.Ops = append(g.Ops, opSpec{kind: "filter", name: name, filt: fn})
	return g
}

func (g *StreamGraph) FlatMap(name string, fn func(Record) ([]Record, error)) *StreamGraph {
	g.Ops = append(g.Ops, opSpec{kind: "flatmap", name: name, fm: fn})
	return g
}

// GroupByKey re-keys records. Pass nil to keep the existing key. Records
// downstream of GroupByKey are guaranteed by our M2 broker (hash-by-key
// routing) to land on the same partition task for a given key.
func (g *StreamGraph) GroupByKey(name string, keyFn func(Record) []byte) *StreamGraph {
	g.Ops = append(g.Ops, opSpec{kind: "groupby", name: name, keyFn: keyFn})
	return g
}

// Count emits running per-key counts, backed by state. Must follow GroupByKey.
func (g *StreamGraph) Count(name string) *StreamGraph {
	g.Ops = append(g.Ops, opSpec{kind: "count", name: name})
	return g
}

// Reduce runs a per-key reducer. initFn returns the accumulator for the
// first record of a key; fn merges a new value into an existing accumulator.
func (g *StreamGraph) Reduce(name string,
	initFn func(v []byte) ([]byte, error),
	fn func(acc, v []byte) ([]byte, error)) *StreamGraph {
	g.Ops = append(g.Ops, opSpec{kind: "reduce", name: name, redIni: initFn, redFn: fn})
	return g
}

// WithTimestamps sets the event-time extractor and out-of-orderness bound.
// Passing maxOutOfOrderness configures the source-side watermark generator:
// watermark = max_event_time_seen - maxOutOfOrderness.
func (g *StreamGraph) WithTimestamps(extractor TimestampExtractor, maxOutOfOrderness time.Duration) *StreamGraph {
	g.Extractor = extractor
	g.MaxOutOfOrderness = maxOutOfOrderness
	if g.WatermarkInterval == 0 {
		g.WatermarkInterval = 200 * time.Millisecond
	}
	return g
}

// WithAllowedLateness controls how much after window_end a record may still
// be accepted. Also controls when windows fire: window_end + allowed_late <= watermark.
func (g *StreamGraph) WithAllowedLateness(d time.Duration) *StreamGraph {
	g.AllowedLateness = d
	return g
}

// OnLate sets a callback for records dropped as too-late. If nil, late
// records are silently dropped.
func (g *StreamGraph) OnLate(fn func(Record)) *StreamGraph {
	g.LateFn = fn
	return g
}

// TumblingWindow inserts a tumbling-window count operator. The operator is
// keyed by Record.Key and emits one (key, window, count) record per window
// once the watermark passes window_end + allowed_lateness.
func (g *StreamGraph) TumblingWindow(name string, size time.Duration) *StreamGraph {
	g.Ops = append(g.Ops, opSpec{kind: "tumbling", name: name, winSize: size})
	return g
}

// SlidingWindow inserts a sliding-window count operator.
func (g *StreamGraph) SlidingWindow(name string, size, slide time.Duration) *StreamGraph {
	g.Ops = append(g.Ops, opSpec{kind: "sliding", name: name, winSize: size, winSlide: slide})
	return g
}

// SessionWindow inserts a session-window count operator: windows grow while
// records for a key arrive within `gap`; the window closes when the watermark
// passes last_event_time + gap.
func (g *StreamGraph) SessionWindow(name string, gap time.Duration) *StreamGraph {
	g.Ops = append(g.Ops, opSpec{kind: "session", name: name, winGap: gap})
	return g
}

// Sink sets the destination topic. Downstream records are produced with
// Record.Key as the routing key, so keyed updates for the same key land on
// the same output partition — just like Kafka Streams.
func (g *StreamGraph) Sink(topic string) *StreamGraph {
	g.SinkTopic = topic
	return g
}

// Validate checks that the graph is well-formed.
func (g *StreamGraph) Validate() error {
	if g.SourceTopic == "" {
		return fmt.Errorf("Source topic is empty")
	}
	if g.BrokerAddr == "" {
		return fmt.Errorf("broker addr is empty")
	}
	return nil
}
