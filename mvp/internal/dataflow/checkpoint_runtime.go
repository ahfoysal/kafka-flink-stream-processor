// M5 checkpointed runtime: an alternative to StreamGraph.Run that uses
// aligned-barrier checkpoints with two-phase commit for exactly-once sinks.
//
// The non-checkpointed Run (runtime.go) is kept for the M3/M4 demos. The new
// RunCheckpointed path differs only in how sink records are delivered:
//
//   Run           : every record produced to the broker synchronously.
//   RunCheckpointed: records staged into bbolt __pending_out during an epoch;
//                    at barrier, offsets+state+pending are fsync'd together
//                    (phase 1), then pending records are replayed to the
//                    broker with deterministic dedup ids (phase 2), then
//                    pending rows are deleted and the cp row flipped to
//                    COMMITTED.
//
// KillAfterRecords simulates a worker crash mid-run. The task panics after
// processing N input records. On restart, the runtime discovers pending rows
// from the last PREPARED-but-not-committed epoch, replays them, and continues
// — exactly-once-safe because the sink dedups.
package dataflow

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/ahfoysal/kafka-flink-stream-processor/mvp/internal/checkpoint"
	"github.com/ahfoysal/kafka-flink-stream-processor/mvp/internal/state"
)

// CheckpointOptions configures the exactly-once runtime.
type CheckpointOptions struct {
	// BarrierInterval is the wall-clock cadence at which the runtime cuts a
	// barrier and commits the current epoch.
	BarrierInterval time.Duration
	// KillAfterRecords, when >0, causes the task to panic after processing
	// that many input records. Used by the demo to verify exactly-once
	// behavior under mid-stream failure.
	KillAfterRecords uint64
	// DisableSinkDedup turns off sink-side dedup (to demonstrate what would
	// happen WITHOUT exactly-once guarantees: duplicates after a crash).
	DisableSinkDedup bool
}

// RunCheckpointed is the M5 entrypoint. Same shape as Run.
func (g *StreamGraph) RunCheckpointed(ctx context.Context, idleTimeout time.Duration,
	opts CheckpointOptions) (RunStats, error) {
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
	if opts.BarrierInterval <= 0 {
		opts.BarrierInterval = 500 * time.Millisecond
	}

	var wg sync.WaitGroup
	var statsMu sync.Mutex
	stats := RunStats{OperatorCounts: map[string]uint64{}}

	for _, pm := range tm.Partitions {
		wg.Add(1)
		go func(partition int) {
			defer wg.Done()
			taskStats, err := runCheckpointedTask(ctx, g, bv, partition,
				filepath.Join(statePath, fmt.Sprintf("task-p%d.db", partition)),
				idleTimeout, opts)
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

// pendingSink is the sink closure the runtime passes into the operator chain
// when running in checkpointed mode. Instead of producing to the broker, it
// stages PendingOutput rows into bbolt, assigning sequential dedup ids.
type pendingSink struct {
	store      *state.Store
	currentCP  uint64
	seq        uint64
	taskID     string
	sinkTopic  string
	stagedThis uint64 // staged in current epoch
}

func (p *pendingSink) stage(r Record) error {
	p.seq++
	dedup := checkpoint.DedupID(p.taskID, p.currentCP, p.seq)
	po := checkpoint.PendingOutput{
		Topic:   p.sinkTopic,
		Key:     append([]byte(nil), r.Key...),
		Value:   append([]byte(nil), r.Value...),
		DedupID: dedup,
		Seq:     p.seq,
	}
	err := p.store.DB().Update(func(tx *bolt.Tx) error {
		return checkpoint.StagePendingOutput(tx, p.currentCP, p.seq, po)
	})
	if err == nil {
		p.stagedThis++
	}
	return err
}

// runCheckpointedTask is the per-partition loop.
func runCheckpointedTask(ctx context.Context, g *StreamGraph, bv *BrokerView,
	partition int, dbPath string, idleTimeout time.Duration,
	opts CheckpointOptions) (RunStats, error) {

	store, err := state.Open(dbPath, 0) // we drive checkpointing ourselves
	if err != nil {
		return RunStats{}, err
	}
	defer store.Close()

	// Ensure checkpoint buckets exist up-front.
	err = store.DB().Update(func(tx *bolt.Tx) error {
		return checkpoint.EnsureBuckets(tx)
	})
	if err != nil {
		return RunStats{}, err
	}

	taskID := fmt.Sprintf("p%d", partition)

	// --- RECOVERY ----------------------------------------------------------
	// 1. Reload offsets (already on disk from last COMMITTED checkpoint).
	// 2. Replay any __pending_out rows. Each carries a deterministic
	//    dedup_id, so duplicates are safe.
	if err := store.ReloadOffsets(); err != nil {
		return RunStats{}, err
	}
	if err := replayPending(store, bv, opts.DisableSinkDedup); err != nil {
		return RunStats{}, err
	}

	// Compile ops (same logic as non-checkpointed runtime).
	ops, err := compileOps(g, store, partition)
	if err != nil {
		return RunStats{}, err
	}

	counts := map[string]*uint64{}
	for _, o := range ops {
		var c uint64
		counts[o.Name()] = &c
	}

	coord := checkpoint.NewCoordinator(opts.BarrierInterval)
	// Pick a starting cp id AFTER any existing committed one, so id space is
	// monotonic across restarts.
	var lastCommittedID uint64
	_ = store.DB().View(func(tx *bolt.Tx) error {
		if row, ok, _ := checkpoint.LastCommitted(tx); ok {
			lastCommittedID = row.ID
		}
		return nil
	})
	for i := uint64(0); i < lastCommittedID; i++ {
		coord.NextID() // burn ids up to lastCommitted
	}
	currentCP := coord.NextID() // first epoch id for this run

	ps := &pendingSink{
		store:     store,
		currentCP: currentCP,
		taskID:    taskID,
		sinkTopic: g.SinkTopic,
	}

	// The chain's sinkFn stages into bbolt instead of producing directly.
	var outCount uint64
	sinkFn := func(r Record) error {
		if IsWatermark(r) {
			return nil
		}
		atomic.AddUint64(&outCount, 1)
		if g.SinkTopic == "" {
			return nil
		}
		return ps.stage(r)
	}

	taskStats := RunStats{OperatorCounts: map[string]uint64{}}
	var inCount uint64
	var idleSince time.Time

	// Commit helper: flush window/operator buffers (Finish is idempotent for
	// our window ops on barrier; for the periodic barrier we skip Finish to
	// avoid double-firing sessions, and instead rely on each op's own
	// watermark-driven emission). For M5 the practical trigger is periodic:
	// we snapshot offsets + whatever pending rows were staged this epoch.
	commit := func(final bool) error {
		if final {
			// End-of-stream: ask every op to flush remaining buffers through
			// the sink.
			em := &chainEmitter{ops: ops, idx: len(ops) - 1, sinkFn: sinkFn, counts: counts}
			for _, o := range ops {
				if err := o.Finish(ctx, em); err != nil {
					return err
				}
			}
		}
		// Phase 1 — prepare. Persist offsets + cp row atomically.
		offsetsSnap := store.Offsets()
		row := checkpoint.CheckpointRow{
			ID:      ps.currentCP,
			Offsets: offsetsSnap,
		}
		if err := store.DB().Update(func(tx *bolt.Tx) error {
			return checkpoint.WritePrepared(tx, row, offsetsSnap)
		}); err != nil {
			return err
		}
		// Phase 2 — commit: produce every pending row for this cp, then
		// mark COMMITTED + delete pending rows.
		if err := flushPending(store, bv, ps.currentCP, opts.DisableSinkDedup); err != nil {
			return err
		}
		if err := store.DB().Update(func(tx *bolt.Tx) error {
			return checkpoint.WriteCommitted(tx, ps.currentCP, time.Now())
		}); err != nil {
			return err
		}
		// Start next epoch.
		ps.currentCP = coord.NextID()
		ps.stagedThis = 0
		return nil
	}

	// Watermark setup (same as Run).
	var wmGen *BoundedOutOfOrdernessGenerator
	if g.Extractor != nil {
		wmGen = NewBoundedOutOfOrdernessGenerator(g.MaxOutOfOrderness)
	}
	wmInterval := g.WatermarkInterval
	if wmInterval == 0 {
		wmInterval = 200 * time.Millisecond
	}
	var lastWMEmit time.Time
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
			// Try a final commit so whatever we staged isn't lost.
			_ = commit(true)
			return fillStats(taskStats, inCount, atomic.LoadUint64(&outCount), counts), ctx.Err()
		default:
		}

		recs, next, err := bv.Consume(g.SourceTopic, partition, g.SourceGroup, 200)
		if err != nil {
			return fillStats(taskStats, inCount, atomic.LoadUint64(&outCount), counts), err
		}
		if len(recs) == 0 {
			if idleSince.IsZero() {
				idleSince = time.Now()
			}
			if time.Since(idleSince) > idleTimeout {
				break
			}
			if wmGen != nil && time.Since(lastWMEmit) >= wmInterval {
				if err := emitWatermark(); err != nil {
					return fillStats(taskStats, inCount, atomic.LoadUint64(&outCount), counts), err
				}
				lastWMEmit = time.Now()
			}
			if coord.ShouldCut(time.Now()) {
				if err := commit(false); err != nil {
					return fillStats(taskStats, inCount, atomic.LoadUint64(&outCount), counts), err
				}
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
		idleSince = time.Time{}

		for _, rec := range recs {
			inCount++

			// Kill-after-N: simulate worker crash after N records. The
			// process dies BEFORE commit, so any pending rows + pre-crash
			// offsets remain on disk for recovery.
			if opts.KillAfterRecords > 0 && inCount == opts.KillAfterRecords {
				log.Printf("[dataflow] SIMULATED CRASH: task p=%d after %d records, cp=%d staged=%d",
					partition, inCount, ps.currentCP, ps.stagedThis)
				// bbolt has already durably written every staged row; the
				// PREPARED row for this epoch may or may not exist. Exiting
				// with a non-zero code so the demo script knows to restart.
				panic(fmt.Sprintf("simulated-crash-after-%d", inCount))
			}

			if wmGen != nil {
				wmGen.Observe(g.Extractor(rec))
			}
			if len(ops) == 0 {
				if err := sinkFn(rec); err != nil {
					return fillStats(taskStats, inCount, atomic.LoadUint64(&outCount), counts), err
				}
				continue
			}
			atomic.AddUint64(counts[ops[0].Name()], 1)
			em := &chainEmitter{ops: ops, idx: 0, sinkFn: sinkFn, counts: counts}
			if err := ops[0].Process(ctx, rec, em); err != nil {
				return fillStats(taskStats, inCount, atomic.LoadUint64(&outCount), counts), err
			}
		}
		store.CommitOffset(g.SourceTopic, partition, g.SourceGroup, next)

		if wmGen != nil && time.Since(lastWMEmit) >= wmInterval {
			if err := emitWatermark(); err != nil {
				return fillStats(taskStats, inCount, atomic.LoadUint64(&outCount), counts), err
			}
			lastWMEmit = time.Now()
		}

		if coord.ShouldCut(time.Now()) {
			if err := commit(false); err != nil {
				return fillStats(taskStats, inCount, atomic.LoadUint64(&outCount), counts), err
			}
		}
	}

	// Final barrier at EOS.
	if err := commit(true); err != nil {
		return fillStats(taskStats, inCount, atomic.LoadUint64(&outCount), counts), err
	}
	return fillStats(taskStats, inCount, atomic.LoadUint64(&outCount), counts), nil
}

func fillStats(s RunStats, in, out uint64, counts map[string]*uint64) RunStats {
	s.RecordsIn = in
	s.RecordsOut = out
	if s.OperatorCounts == nil {
		s.OperatorCounts = map[string]uint64{}
	}
	for k, v := range counts {
		s.OperatorCounts[k] = atomic.LoadUint64(v)
	}
	return s
}

// compileOps is runtime.go's op compilation, factored out so the checkpointed
// path can reuse it. Kept identical to avoid divergence bugs.
func compileOps(g *StreamGraph, store *state.Store, partition int) ([]Operator, error) {
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
			return nil, fmt.Errorf("unknown op kind: %s", s.kind)
		}
	}
	return ops, nil
}

// flushPending reads every pending-output row for cpID (and earlier, in case
// some survived a crash) and produces them to the broker. Dedup is enforced
// client-side by prefixing the payload with the dedup id and having the sink
// consumer skip seen ids — but since we only own the processor here, we also
// stamp a local "seen" mark so the same task-id never re-emits a row it
// already sent in a previous run.
func flushPending(store *state.Store, bv *BrokerView, cpID uint64, disableDedup bool) error {
	type row struct {
		cp, seq uint64
		po      checkpoint.PendingOutput
	}
	var rows []row
	err := store.DB().View(func(tx *bolt.Tx) error {
		return checkpoint.IterPending(tx, func(cp, seq uint64, po checkpoint.PendingOutput) error {
			if cp > cpID {
				return nil
			}
			rows = append(rows, row{cp, seq, po})
			return nil
		})
	})
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := producePending(store, bv, r.po, disableDedup); err != nil {
			return err
		}
	}
	return nil
}

// replayPending is called on startup: pending rows from a previous run are
// re-produced. Sink dedup (or local seen marks) turns replays into no-ops.
func replayPending(store *state.Store, bv *BrokerView, disableDedup bool) error {
	var rows []checkpoint.PendingOutput
	err := store.DB().View(func(tx *bolt.Tx) error {
		return checkpoint.IterPending(tx, func(cp, seq uint64, po checkpoint.PendingOutput) error {
			rows = append(rows, po)
			return nil
		})
	})
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	log.Printf("[dataflow] RECOVERY: replaying %d pending output(s) from previous run", len(rows))
	for _, po := range rows {
		if err := producePending(store, bv, po, disableDedup); err != nil {
			return err
		}
	}
	return nil
}

// producePending is the single-row producer + dedup gate.
func producePending(store *state.Store, bv *BrokerView,
	po checkpoint.PendingOutput, disableDedup bool) error {
	if !disableDedup {
		var seen bool
		_ = store.DB().View(func(tx *bolt.Tx) error {
			seen, _ = checkpoint.SeenDedup(tx, po.Topic, po.DedupID)
			return nil
		})
		if seen {
			return nil // idempotent skip
		}
	}
	// Stamp dedup_id into the payload as a leading header so downstream
	// consumers CAN see it even without a Kafka headers API. Format:
	// "__DEDUP__<id>\t<original_value>". Demos that want raw payloads can
	// strip the prefix; the demo's count verifier does.
	stamped := []byte("__DEDUP__" + po.DedupID + "\t")
	stamped = append(stamped, po.Value...)
	if _, err := bv.Produce(po.Topic, po.Key, stamped, 0); err != nil {
		return err
	}
	if !disableDedup {
		if err := store.DB().Update(func(tx *bolt.Tx) error {
			return checkpoint.MarkDedup(tx, po.Topic, po.DedupID)
		}); err != nil {
			return err
		}
	}
	return nil
}
