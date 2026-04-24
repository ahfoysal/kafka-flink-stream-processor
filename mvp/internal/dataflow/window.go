package dataflow

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/ahfoysal/kafka-flink-stream-processor/mvp/internal/state"
)

// Window operators implement event-time aggregation. They are keyed
// (partition + record.Key) and store per-window partial state in bbolt.
// When a watermark passes window_end + allowed_lateness the window is
// "fired": the operator emits one record per (key, window) and garbage-
// collects the window state.
//
// Late records (event_time < watermark - allowed_lateness) are either
// dropped or forwarded to a side output via LateFn.
//
// All window operators here implement a simple aggregation: per-key COUNT.
// Generic per-window reducers are a trivial extension (pass a Fn plus the
// accumulator encoding), but the M4 scope is event-time windowed count.

// WindowKind identifies the windowing strategy, used to encode state keys.
type WindowKind byte

const (
	WKTumbling WindowKind = 'T'
	WKSliding  WindowKind = 'S'
	WKSession  WindowKind = 'E'
)

// Window is a half-open time interval [Start, End).
type Window struct {
	Start time.Time
	End   time.Time
}

// state key layout for a windowed count:
//
//	<start_nanos:8BE> || <end_nanos:8BE> || <user_key>
//
// We sort purely by start/end so range-scans on expiration are cheap.
func encodeWindowKey(w Window, key []byte) []byte {
	out := make([]byte, 16+len(key))
	binary.BigEndian.PutUint64(out[0:8], uint64(w.Start.UnixNano()))
	binary.BigEndian.PutUint64(out[8:16], uint64(w.End.UnixNano()))
	copy(out[16:], key)
	return out
}

func decodeWindowKey(buf []byte) (Window, []byte, bool) {
	if len(buf) < 16 {
		return Window{}, nil, false
	}
	s := int64(binary.BigEndian.Uint64(buf[0:8]))
	e := int64(binary.BigEndian.Uint64(buf[8:16]))
	return Window{Start: time.Unix(0, s), End: time.Unix(0, e)}, buf[16:], true
}

// ----------------------------------------------------------------------------
// Tumbling window
// ----------------------------------------------------------------------------

// TumblingWindowOp: fixed-size, non-overlapping windows. A record with
// event-time t goes into the unique window [floor(t/size)*size, +size).
type TumblingWindowOp struct {
	OpName         string
	Store          *state.Store
	Partition      int
	Size           time.Duration
	AllowedLate    time.Duration
	Extractor      TimestampExtractor
	LateFn         func(Record) // called on dropped-as-late records
	currentWM      time.Time
	maxEventTime   time.Time
	wmInitialized  bool
	recordsDropped uint64
}

func NewTumblingWindowOp(name string, store *state.Store, partition int,
	size, allowedLate time.Duration, extractor TimestampExtractor) *TumblingWindowOp {
	if extractor == nil {
		extractor = IngestionTime
	}
	return &TumblingWindowOp{
		OpName: name, Store: store, Partition: partition,
		Size: size, AllowedLate: allowedLate, Extractor: extractor,
		currentWM: MinTime,
	}
}

func (o *TumblingWindowOp) Name() string { return o.OpName }

func tumblingWindowFor(t time.Time, size time.Duration) Window {
	ns := t.UnixNano()
	sz := int64(size)
	start := (ns / sz) * sz
	if ns < 0 && ns%sz != 0 {
		start -= sz
	}
	return Window{
		Start: time.Unix(0, start),
		End:   time.Unix(0, start+sz),
	}
}

func (o *TumblingWindowOp) Process(ctx context.Context, in Record, out Emitter) error {
	if IsWatermark(in) {
		newWM := WatermarkOf(in)
		if newWM.After(o.currentWM) {
			o.currentWM = newWM
			if err := o.fireReadyWindows(ctx, out); err != nil {
				return err
			}
		}
		// Forward the watermark downstream so chained operators see it.
		return out.Emit(in)
	}

	ts := o.Extractor(in)
	if ts.After(o.maxEventTime) {
		o.maxEventTime = ts
	}

	// Drop too-late records (event-time is before watermark - allowed_late).
	if o.wmInitialized || !o.currentWM.Equal(MinTime) {
		o.wmInitialized = true
		threshold := o.currentWM.Add(-o.AllowedLate)
		if ts.Before(threshold) {
			o.recordsDropped++
			if o.LateFn != nil {
				o.LateFn(in)
			}
			return nil
		}
	}

	w := tumblingWindowFor(ts, o.Size)
	bucket := fmt.Sprintf("%s__w%c", o.OpName, WKTumbling)
	key := encodeWindowKey(w, in.Key)
	cur, _, err := o.Store.GetUint64(bucket, o.Partition, key)
	if err != nil {
		return err
	}
	cur++
	if err := o.Store.PutUint64(bucket, o.Partition, key, cur); err != nil {
		return err
	}
	return nil
}

// fireReadyWindows emits every window whose end + allowed_late <= watermark
// and removes them from state.
func (o *TumblingWindowOp) fireReadyWindows(ctx context.Context, out Emitter) error {
	bucket := fmt.Sprintf("%s__w%c", o.OpName, WKTumbling)
	type pending struct {
		w     Window
		key   []byte
		count uint64
	}
	var ready []pending
	err := o.Store.Iterate(bucket, o.Partition, func(k, v []byte) error {
		w, userKey, ok := decodeWindowKey(k)
		if !ok {
			return nil
		}
		if !w.End.Add(o.AllowedLate).After(o.currentWM) {
			// window_end + allowed_late <= watermark -> fire
			if len(v) != 8 {
				return nil
			}
			count := binary.BigEndian.Uint64(v)
			ready = append(ready, pending{
				w:     w,
				key:   append([]byte(nil), userKey...),
				count: count,
			})
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Sort for deterministic emission order (by window start, then key).
	sort.Slice(ready, func(i, j int) bool {
		if !ready[i].w.Start.Equal(ready[j].w.Start) {
			return ready[i].w.Start.Before(ready[j].w.Start)
		}
		return string(ready[i].key) < string(ready[j].key)
	})
	for _, p := range ready {
		// Emit as Record with Value = "<win_start_ms>\t<win_end_ms>\t<count>"
		payload := fmt.Sprintf("%d\t%d\t%d",
			p.w.Start.UnixMilli(), p.w.End.UnixMilli(), p.count)
		err := out.Emit(Record{
			Key:       p.key,
			Value:     []byte(payload),
			Timestamp: p.w.End,
		})
		if err != nil {
			return err
		}
		if err := o.Store.Delete(bucket, o.Partition,
			encodeWindowKey(p.w, p.key)); err != nil {
			return err
		}
	}
	return nil
}

// Finish fires ALL remaining windows at end-of-stream: we treat EOS as
// watermark = +inf.
func (o *TumblingWindowOp) Finish(ctx context.Context, out Emitter) error {
	o.currentWM = MaxTime
	return o.fireReadyWindows(ctx, out)
}

// RecordsDropped is exported for test/demo visibility.
func (o *TumblingWindowOp) RecordsDropped() uint64 { return o.recordsDropped }

// ----------------------------------------------------------------------------
// Sliding window
// ----------------------------------------------------------------------------

// SlidingWindowOp: fixed-size windows that overlap by (size - slide). A
// record with event-time t belongs to every window [w_start, w_start+size)
// such that t falls within it. Implemented by assigning each record to
// up to ceil(size/slide) windows.
type SlidingWindowOp struct {
	OpName      string
	Store       *state.Store
	Partition   int
	Size        time.Duration
	Slide       time.Duration
	AllowedLate time.Duration
	Extractor   TimestampExtractor
	LateFn      func(Record)
	currentWM   time.Time
}

func NewSlidingWindowOp(name string, store *state.Store, partition int,
	size, slide, allowedLate time.Duration, extractor TimestampExtractor) *SlidingWindowOp {
	if extractor == nil {
		extractor = IngestionTime
	}
	return &SlidingWindowOp{
		OpName: name, Store: store, Partition: partition,
		Size: size, Slide: slide, AllowedLate: allowedLate,
		Extractor: extractor, currentWM: MinTime,
	}
}

func (o *SlidingWindowOp) Name() string { return o.OpName }

func slidingWindowsFor(t time.Time, size, slide time.Duration) []Window {
	ns := t.UnixNano()
	sz := int64(size)
	sl := int64(slide)
	// Windows containing t are those with start in (t-size, t], stepping by slide.
	// First window start = floor((t - size + slide) / slide) * slide, clamped.
	first := ((ns - sz + sl) / sl) * sl
	if (ns-sz+sl) < 0 && (ns-sz+sl)%sl != 0 {
		first -= sl
	}
	var out []Window
	for s := first; s <= ns; s += sl {
		out = append(out, Window{
			Start: time.Unix(0, s),
			End:   time.Unix(0, s+sz),
		})
	}
	return out
}

func (o *SlidingWindowOp) Process(ctx context.Context, in Record, out Emitter) error {
	if IsWatermark(in) {
		newWM := WatermarkOf(in)
		if newWM.After(o.currentWM) {
			o.currentWM = newWM
			if err := o.fireReadyWindows(ctx, out); err != nil {
				return err
			}
		}
		return out.Emit(in)
	}
	ts := o.Extractor(in)
	if !o.currentWM.Equal(MinTime) && ts.Before(o.currentWM.Add(-o.AllowedLate)) {
		if o.LateFn != nil {
			o.LateFn(in)
		}
		return nil
	}
	bucket := fmt.Sprintf("%s__w%c", o.OpName, WKSliding)
	for _, w := range slidingWindowsFor(ts, o.Size, o.Slide) {
		key := encodeWindowKey(w, in.Key)
		cur, _, err := o.Store.GetUint64(bucket, o.Partition, key)
		if err != nil {
			return err
		}
		cur++
		if err := o.Store.PutUint64(bucket, o.Partition, key, cur); err != nil {
			return err
		}
	}
	return nil
}

func (o *SlidingWindowOp) fireReadyWindows(ctx context.Context, out Emitter) error {
	bucket := fmt.Sprintf("%s__w%c", o.OpName, WKSliding)
	type pending struct {
		w     Window
		key   []byte
		count uint64
	}
	var ready []pending
	err := o.Store.Iterate(bucket, o.Partition, func(k, v []byte) error {
		w, userKey, ok := decodeWindowKey(k)
		if !ok {
			return nil
		}
		if !w.End.Add(o.AllowedLate).After(o.currentWM) {
			if len(v) != 8 {
				return nil
			}
			count := binary.BigEndian.Uint64(v)
			ready = append(ready, pending{
				w:     w,
				key:   append([]byte(nil), userKey...),
				count: count,
			})
		}
		return nil
	})
	if err != nil {
		return err
	}
	sort.Slice(ready, func(i, j int) bool {
		if !ready[i].w.Start.Equal(ready[j].w.Start) {
			return ready[i].w.Start.Before(ready[j].w.Start)
		}
		return string(ready[i].key) < string(ready[j].key)
	})
	for _, p := range ready {
		payload := fmt.Sprintf("%d\t%d\t%d",
			p.w.Start.UnixMilli(), p.w.End.UnixMilli(), p.count)
		err := out.Emit(Record{Key: p.key, Value: []byte(payload), Timestamp: p.w.End})
		if err != nil {
			return err
		}
		if err := o.Store.Delete(bucket, o.Partition,
			encodeWindowKey(p.w, p.key)); err != nil {
			return err
		}
	}
	return nil
}

func (o *SlidingWindowOp) Finish(ctx context.Context, out Emitter) error {
	o.currentWM = MaxTime
	return o.fireReadyWindows(ctx, out)
}

// ----------------------------------------------------------------------------
// Session window
//
// A session window for a key grows while records arrive within Gap of each
// other; as soon as the watermark passes (last_event_time + Gap) the session
// closes and is emitted.
//
// State layout per key: one "open session" record per key:
//
//	bucket = op__session__open
//	key    = <user_key>
//	value  = <start_ns:8BE> || <last_ns:8BE> || <count:8BE>
//
// When a new record arrives for that key within Gap of last_ns we extend
// last_ns and increment count. Otherwise (or if watermark has already passed
// last_ns + Gap) we close the old session and open a new one.
// ----------------------------------------------------------------------------

type SessionWindowOp struct {
	OpName      string
	Store       *state.Store
	Partition   int
	Gap         time.Duration
	AllowedLate time.Duration
	Extractor   TimestampExtractor
	LateFn      func(Record)
	currentWM   time.Time
}

func NewSessionWindowOp(name string, store *state.Store, partition int,
	gap, allowedLate time.Duration, extractor TimestampExtractor) *SessionWindowOp {
	if extractor == nil {
		extractor = IngestionTime
	}
	return &SessionWindowOp{
		OpName: name, Store: store, Partition: partition,
		Gap: gap, AllowedLate: allowedLate, Extractor: extractor,
		currentWM: MinTime,
	}
}

func (o *SessionWindowOp) Name() string { return o.OpName }

func encodeSession(start, last time.Time, count uint64) []byte {
	buf := make([]byte, 24)
	binary.BigEndian.PutUint64(buf[0:8], uint64(start.UnixNano()))
	binary.BigEndian.PutUint64(buf[8:16], uint64(last.UnixNano()))
	binary.BigEndian.PutUint64(buf[16:24], count)
	return buf
}

func decodeSession(buf []byte) (start, last time.Time, count uint64, ok bool) {
	if len(buf) != 24 {
		return time.Time{}, time.Time{}, 0, false
	}
	s := int64(binary.BigEndian.Uint64(buf[0:8]))
	l := int64(binary.BigEndian.Uint64(buf[8:16]))
	c := binary.BigEndian.Uint64(buf[16:24])
	return time.Unix(0, s), time.Unix(0, l), c, true
}

func (o *SessionWindowOp) Process(ctx context.Context, in Record, out Emitter) error {
	if IsWatermark(in) {
		newWM := WatermarkOf(in)
		if newWM.After(o.currentWM) {
			o.currentWM = newWM
			if err := o.fireReadySessions(ctx, out); err != nil {
				return err
			}
		}
		return out.Emit(in)
	}
	ts := o.Extractor(in)
	if !o.currentWM.Equal(MinTime) && ts.Before(o.currentWM.Add(-o.AllowedLate)) {
		if o.LateFn != nil {
			o.LateFn(in)
		}
		return nil
	}
	bucket := fmt.Sprintf("%s__session__open", o.OpName)
	cur, ok, err := o.Store.Get(bucket, o.Partition, in.Key)
	if err != nil {
		return err
	}
	if !ok {
		return o.Store.Put(bucket, o.Partition, in.Key,
			encodeSession(ts, ts, 1))
	}
	start, last, count, ok := decodeSession(cur)
	if !ok {
		return o.Store.Put(bucket, o.Partition, in.Key,
			encodeSession(ts, ts, 1))
	}
	if ts.Sub(last) <= o.Gap && ts.After(last.Add(-o.Gap)) {
		// extend session
		if ts.After(last) {
			last = ts
		}
		if ts.Before(start) {
			start = ts
		}
		count++
		return o.Store.Put(bucket, o.Partition, in.Key,
			encodeSession(start, last, count))
	}
	// Out of reach -> close old, open new. (Rare path: late but not too-late.)
	payload := fmt.Sprintf("%d\t%d\t%d",
		start.UnixMilli(), last.Add(o.Gap).UnixMilli(), count)
	if err := out.Emit(Record{
		Key: append([]byte(nil), in.Key...), Value: []byte(payload),
		Timestamp: last.Add(o.Gap),
	}); err != nil {
		return err
	}
	return o.Store.Put(bucket, o.Partition, in.Key, encodeSession(ts, ts, 1))
}

func (o *SessionWindowOp) fireReadySessions(ctx context.Context, out Emitter) error {
	bucket := fmt.Sprintf("%s__session__open", o.OpName)
	type pending struct {
		key   []byte
		start time.Time
		end   time.Time
		count uint64
	}
	var ready []pending
	err := o.Store.Iterate(bucket, o.Partition, func(k, v []byte) error {
		start, last, count, ok := decodeSession(v)
		if !ok {
			return nil
		}
		end := last.Add(o.Gap)
		// close when watermark passes end + allowed_late
		if !end.Add(o.AllowedLate).After(o.currentWM) {
			ready = append(ready, pending{
				key:   append([]byte(nil), k...),
				start: start, end: end, count: count,
			})
		}
		return nil
	})
	if err != nil {
		return err
	}
	sort.Slice(ready, func(i, j int) bool {
		if !ready[i].start.Equal(ready[j].start) {
			return ready[i].start.Before(ready[j].start)
		}
		return string(ready[i].key) < string(ready[j].key)
	})
	for _, p := range ready {
		payload := fmt.Sprintf("%d\t%d\t%d",
			p.start.UnixMilli(), p.end.UnixMilli(), p.count)
		if err := out.Emit(Record{
			Key: p.key, Value: []byte(payload), Timestamp: p.end,
		}); err != nil {
			return err
		}
		if err := o.Store.Delete(bucket, o.Partition, p.key); err != nil {
			return err
		}
	}
	return nil
}

func (o *SessionWindowOp) Finish(ctx context.Context, out Emitter) error {
	o.currentWM = MaxTime
	return o.fireReadySessions(ctx, out)
}

// ----------------------------------------------------------------------------
// Small helper: encode a count as a decimal string payload (used by demos
// that sink window results back to Kafka-compatible topics).
// ----------------------------------------------------------------------------

func FormatWindowPayload(w Window, count uint64) string {
	return strconv.FormatInt(w.Start.UnixMilli(), 10) + "\t" +
		strconv.FormatInt(w.End.UnixMilli(), 10) + "\t" +
		strconv.FormatUint(count, 10)
}
