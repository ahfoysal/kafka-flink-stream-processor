// Package dataflow is the M3 stream-processing runtime: a builder API
// (Source/Map/Filter/GroupByKey/Count/Reduce/Join/Sink) that compiles to a
// DAG of operators driven by our M2 partitioned log broker.
//
// Flink terminology, much smaller scope:
//   - a Stream is a logical topic.
//   - an Operator is a node in the DAG.
//   - records flow left-to-right, one partition per goroutine.
//   - keyed operators (GroupByKey, Count, Reduce, Join) shard state by key.
package dataflow

import "time"

// Record is the unit of data flowing through the DAG. Key drives partitioning
// and keyed-state lookups; Value is the payload. Timestamp is the event-time
// stamp (M4 will use this for watermarks — M3 treats it as ingestion time).
type Record struct {
	Key       []byte
	Value     []byte
	Timestamp time.Time

	// Source-tracked metadata, used for offset commits. Downstream operators
	// ignore these fields.
	Topic     string
	Partition int
	Offset    uint64
}

// Copy returns a deep copy — operators mutate Record in place for throughput
// but the source/sink boundaries must own their own buffers.
func (r Record) Copy() Record {
	c := r
	if r.Key != nil {
		c.Key = append([]byte(nil), r.Key...)
	}
	if r.Value != nil {
		c.Value = append([]byte(nil), r.Value...)
	}
	return c
}
