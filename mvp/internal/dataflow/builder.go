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
}

type opSpec struct {
	kind   string // "map", "filter", "flatmap", "groupby", "count", "reduce", "join"
	name   string
	mapFn  func(Record) (Record, error)
	filt   func(Record) bool
	fm     func(Record) ([]Record, error)
	keyFn  func(Record) []byte
	redFn  func(acc, v []byte) ([]byte, error)
	redIni func(v []byte) ([]byte, error)
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
