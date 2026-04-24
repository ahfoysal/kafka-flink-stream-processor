package dataflow

import (
	"math"
	"time"
)

// TimestampExtractor pulls an event-time timestamp out of a Record. The M2
// broker does not carry per-record timestamps on the wire (yet), so M4
// extractors typically parse the event-time out of the payload. The default
// fallback (IngestionTime) just uses Record.Timestamp, which is set to
// time.Now() at source.
type TimestampExtractor func(Record) time.Time

// IngestionTime is the default extractor: trust Record.Timestamp.
func IngestionTime(r Record) time.Time { return r.Timestamp }

// ----------------------------------------------------------------------------
// Watermarks
//
// A watermark is a monotonically-increasing timestamp threshold: the source is
// asserting "no more records with event_time < W will arrive (modulo
// allowed_lateness)". Downstream window operators use watermarks to decide
// when a window is safe to emit.
//
// We piggy-back the watermark onto the regular record channel as a sentinel
// record (IsWatermark == true). This keeps the operator interface uniform:
// every operator sees watermarks in-band and can forward / transform them.
// ----------------------------------------------------------------------------

// WatermarkHeader is the first byte of Value on a watermark sentinel record.
// We pick a byte that cannot occur in normal payloads of the event-time demo
// (tab-separated <event_time_ms>\t<word>), so we can't ambiguate accidentally.
const watermarkMarker = "\x00__WATERMARK__\x00"

// IsWatermark reports whether r carries a watermark sentinel.
func IsWatermark(r Record) bool {
	return r.WatermarkNanos != 0
}

// NewWatermark builds a sentinel record carrying a watermark timestamp.
// Downstream operators forward this without processing as a normal record.
func NewWatermark(ts time.Time) Record {
	ns := ts.UnixNano()
	if ns == 0 {
		// 0 is our "not a watermark" signal; bump by 1ns so the caller gets a
		// record that round-trips through IsWatermark.
		ns = 1
	}
	return Record{
		Value:          []byte(watermarkMarker),
		Timestamp:      ts,
		WatermarkNanos: ns,
	}
}

// WatermarkOf returns the watermark time carried by r. Only meaningful if
// IsWatermark(r) is true.
func WatermarkOf(r Record) time.Time {
	return time.Unix(0, r.WatermarkNanos)
}

// MinTime / MaxTime are convenience bounds used when tracking watermarks
// across multiple input partitions.
var (
	MinTime = time.Unix(0, math.MinInt64+1)
	MaxTime = time.Unix(0, math.MaxInt64)
)

// BoundedOutOfOrdernessGenerator tracks the max event-time seen so far and
// emits watermarks = max_event_time - allowed_lateness. This is the
// canonical "bounded out-of-orderness" strategy from Flink.
type BoundedOutOfOrdernessGenerator struct {
	MaxSeen  time.Time
	MaxDelay time.Duration
}

func NewBoundedOutOfOrdernessGenerator(maxDelay time.Duration) *BoundedOutOfOrdernessGenerator {
	return &BoundedOutOfOrdernessGenerator{MaxDelay: maxDelay, MaxSeen: MinTime}
}

// Observe records a new event-time; call this for every incoming record.
func (g *BoundedOutOfOrdernessGenerator) Observe(ts time.Time) {
	if ts.After(g.MaxSeen) {
		g.MaxSeen = ts
	}
}

// Current returns the current watermark. Returns MinTime until at least one
// record has been observed (so operators never prematurely close windows).
func (g *BoundedOutOfOrdernessGenerator) Current() time.Time {
	if g.MaxSeen.Equal(MinTime) {
		return MinTime
	}
	return g.MaxSeen.Add(-g.MaxDelay)
}
