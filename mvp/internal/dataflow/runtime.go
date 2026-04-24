package dataflow

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ahfoysal/kafka-flink-stream-processor/mvp/internal/state"
)

// Run materializes the graph into one task per input partition and drives
// each task to completion (stops when the source goes idle for idleTimeout).
func (g *StreamGraph) Run(ctx context.Context, idleTimeout time.Duration) (RunStats, error) {
	if err := g.Validate(); err != nil {
		return RunStats{}, err
	}
	bv := NewBrokerView(g.BrokerAddr)
	tm, err := bv.FetchTopic(g.SourceTopic)
	if err != nil {
		return RunStats{}, fmt.Errorf("fetch source meta: %w", err)
	}
	if g.SinkTopic != "" {
		if _, err := bv.FetchTopic(g.SinkTopic); err != nil {
			return RunStats{}, fmt.Errorf("fetch sink meta: %w", err)
		}
	}

	statePath := g.StatePath
	if statePath == "" {
		statePath = fmt.Sprintf("state-%s-%s", g.SourceTopic, g.SourceGroup)
	}

	var wg sync.WaitGroup
	var statsMu sync.Mutex
	stats := RunStats{OperatorCounts: map[string]uint64{}}

	for _, pm := range tm.Partitions {
		wg.Add(1)
		go func(partition int) {
			defer wg.Done()
			taskStats, err := runTask(ctx, g, bv, partition, filepath.Join(statePath,
				fmt.Sprintf("task-p%d.db", partition)), idleTimeout)
			if err != nil {
				log.Printf("[dataflow] task p=%d: %v", partition, err)
			}
			statsMu.Lock()
			stats.RecordsIn += taskStats.RecordsIn
			stats.RecordsOut += taskStats.RecordsOut
			for k, v := range taskStats.OperatorCounts {
				stats.OperatorCounts[k] += v
			}
			statsMu.Unlock()
		}(pm.Partition)
	}
	wg.Wait()
	return stats, nil
}

// RunStats is a small aggregate over all tasks (used by the demo).
type RunStats struct {
	RecordsIn      uint64
	RecordsOut     uint64
	OperatorCounts map[string]uint64
}

// ----------------------------------------------------------------------------
// Per-task execution.
// ----------------------------------------------------------------------------

type chainEmitter struct {
	ops     []Operator
	idx     int
	sinkFn  func(Record) error
	counts  map[string]*uint64
	bufPool *sync.Pool
}

func (c *chainEmitter) Emit(r Record) error {
	next := c.idx + 1
	if next >= len(c.ops) {
		// End of chain -> sink
		return c.sinkFn(r)
	}
	atomic.AddUint64(c.counts[c.ops[next].Name()], 1)
	ctx := context.Background()
	child := &chainEmitter{ops: c.ops, idx: next, sinkFn: c.sinkFn, counts: c.counts}
	return c.ops[next].Process(ctx, r, child)
}

func runTask(ctx context.Context, g *StreamGraph, bv *BrokerView,
	partition int, dbPath string, idleTimeout time.Duration) (RunStats, error) {

	store, err := state.Open(dbPath, g.CheckpointInterval)
	if err != nil {
		return RunStats{}, err
	}
	defer store.Close()

	// Compile opSpecs -> Operators (bind partition + store to stateful ones).
	ops := make([]Operator, 0, len(g.Ops))
	for _, s := range g.Ops {
		switch s.kind {
		case "map":
			ops = append(ops, &MapOp{OpName: s.name, Fn: s.mapFn})
		case "filter":
			ops = append(ops, &FilterOp{OpName: s.name, Fn: s.filt})
		case "flatmap":
			ops = append(ops, &FlatMapOp{OpName: s.name, Fn: s.fm})
		case "groupby":
			ops = append(ops, &GroupByKeyOp{OpName: s.name, KeyFn: s.keyFn})
		case "count":
			ops = append(ops, &CountOp{OpName: s.name, Store: store, Partition: partition})
		case "reduce":
			ops = append(ops, &ReduceOp{
				OpName: s.name, Store: store, Partition: partition,
				Fn: s.redFn, InitFn: s.redIni,
			})
		case "tumbling":
			wop := NewTumblingWindowOp(s.name, store, partition,
				s.winSize, g.AllowedLateness, g.Extractor)
			wop.LateFn = g.LateFn
			ops = append(ops, wop)
		case "sliding":
			wop := NewSlidingWindowOp(s.name, store, partition,
				s.winSize, s.winSlide, g.AllowedLateness, g.Extractor)
			wop.LateFn = g.LateFn
			ops = append(ops, wop)
		case "session":
			wop := NewSessionWindowOp(s.name, store, partition,
				s.winGap, g.AllowedLateness, g.Extractor)
			wop.LateFn = g.LateFn
			ops = append(ops, wop)
		default:
			return RunStats{}, fmt.Errorf("unknown op kind: %s", s.kind)
		}
	}

	counts := map[string]*uint64{}
	for _, o := range ops {
		var c uint64
		counts[o.Name()] = &c
	}

	// Sink fn: if SinkTopic set, produce back to broker; else print.
	// Watermark sentinels are never sunk — they are control records.
	var outCount uint64
	sinkFn := func(r Record) error {
		if IsWatermark(r) {
			return nil
		}
		atomic.AddUint64(&outCount, 1)
		if g.SinkTopic == "" {
			// No sink — for demos that inspect in-memory state.
			return nil
		}
		_, err := bv.Produce(g.SinkTopic, r.Key, r.Value, int(outCount))
		return err
	}

	taskStats := RunStats{OperatorCounts: map[string]uint64{}}
	var inCount uint64
	var idleSince time.Time

	// M4: source-side watermark generator. We use BoundedOutOfOrderness:
	// watermark = max_event_time_seen - MaxOutOfOrderness. Emitted into the
	// op chain every WatermarkInterval and on idle.
	var wmGen *BoundedOutOfOrdernessGenerator
	if g.Extractor != nil {
		wmGen = NewBoundedOutOfOrdernessGenerator(g.MaxOutOfOrderness)
	}
	var lastWMEmit time.Time
	wmInterval := g.WatermarkInterval
	if wmInterval == 0 {
		wmInterval = 200 * time.Millisecond
	}
	emitWatermark := func() error {
		if wmGen == nil || len(ops) == 0 {
			return nil
		}
		cur := wmGen.Current()
		if cur.Equal(MinTime) {
			return nil
		}
		wmRec := NewWatermark(cur)
		em := &chainEmitter{ops: ops, idx: 0, sinkFn: sinkFn, counts: counts}
		return ops[0].Process(ctx, wmRec, em)
	}

	for {
		select {
		case <-ctx.Done():
			taskStats.RecordsIn = inCount
			taskStats.RecordsOut = atomic.LoadUint64(&outCount)
			for k, v := range counts {
				taskStats.OperatorCounts[k] = atomic.LoadUint64(v)
			}
			return taskStats, ctx.Err()
		default:
		}

		recs, next, err := bv.Consume(g.SourceTopic, partition, g.SourceGroup, 200)
		if err != nil {
			taskStats.RecordsIn = inCount
			taskStats.RecordsOut = atomic.LoadUint64(&outCount)
			for k, v := range counts {
				taskStats.OperatorCounts[k] = atomic.LoadUint64(v)
			}
			return taskStats, err
		}
		if len(recs) == 0 {
			if idleSince.IsZero() {
				idleSince = time.Now()
			}
			if time.Since(idleSince) > idleTimeout {
				break
			}
			// On idle, still pulse a watermark so windows can fire.
			if wmGen != nil && time.Since(lastWMEmit) >= wmInterval {
				if err := emitWatermark(); err != nil {
					return taskStats, err
				}
				lastWMEmit = time.Now()
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
		idleSince = time.Time{}

		for _, rec := range recs {
			inCount++
			// Observe event-time for watermark generation (before Process so
			// the generator has seen this record when it computes Current()).
			if wmGen != nil {
				wmGen.Observe(g.Extractor(rec))
			}
			if len(ops) == 0 {
				if err := sinkFn(rec); err != nil {
					taskStats.RecordsIn = inCount
			taskStats.RecordsOut = atomic.LoadUint64(&outCount)
			for k, v := range counts {
				taskStats.OperatorCounts[k] = atomic.LoadUint64(v)
			}
			return taskStats, err
				}
				continue
			}
			atomic.AddUint64(counts[ops[0].Name()], 1)
			em := &chainEmitter{ops: ops, idx: 0, sinkFn: sinkFn, counts: counts}
			if err := ops[0].Process(ctx, rec, em); err != nil {
				taskStats.RecordsIn = inCount
			taskStats.RecordsOut = atomic.LoadUint64(&outCount)
			for k, v := range counts {
				taskStats.OperatorCounts[k] = atomic.LoadUint64(v)
			}
			return taskStats, err
			}
		}
		store.CommitOffset(g.SourceTopic, partition, g.SourceGroup, next)

		// Periodic watermark after every batch.
		if wmGen != nil && time.Since(lastWMEmit) >= wmInterval {
			if err := emitWatermark(); err != nil {
				return taskStats, err
			}
			lastWMEmit = time.Now()
		}
	}

	// Flush operator buffers (no-op for M3 operators; wired up for future windowing).
	em := &chainEmitter{ops: ops, idx: len(ops) - 1, sinkFn: sinkFn, counts: counts}
	for _, o := range ops {
		if err := o.Finish(ctx, em); err != nil {
			taskStats.RecordsIn = inCount
			taskStats.RecordsOut = atomic.LoadUint64(&outCount)
			for k, v := range counts {
				taskStats.OperatorCounts[k] = atomic.LoadUint64(v)
			}
			return taskStats, err
		}
	}
	if err := store.Checkpoint(); err != nil {
		return taskStats, err
	}

	taskStats.RecordsIn = inCount
	taskStats.RecordsOut = atomic.LoadUint64(&outCount)
	for k, v := range counts {
		taskStats.OperatorCounts[k] = atomic.LoadUint64(v)
	}
	return taskStats, nil
}
