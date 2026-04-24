// sql-job: M5 demo binary.
//
// Parses a streaming-SQL query, lowers it to a StreamGraph, and runs it with
// the exactly-once checkpointed runtime. Supports a --kill-after-records
// switch that panics mid-stream so the demo can verify exactly-once on
// restart (no duplicates in the sink topic).
//
// Usage:
//   sql-job -broker ... -group g1 -state ./state \
//           -sql "SELECT word, COUNT(*) FROM et_lines \
//                 GROUP BY word, TUMBLE(event_time, INTERVAL '5' SECOND) \
//                 EMIT ON WATERMARK INTO et_counts"
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ahfoysal/kafka-flink-stream-processor/mvp/internal/dataflow"
	"github.com/ahfoysal/kafka-flink-stream-processor/mvp/internal/sql"
)

func main() {
	broker := flag.String("broker", "http://127.0.0.1:49092", "any-broker URL")
	group := flag.String("group", "sql-job", "consumer group")
	statePath := flag.String("state", "", "directory for bbolt state files (required)")
	idleMs := flag.Int("idle-ms", 3500, "stop after this many ms with no new input")
	killAfter := flag.Uint64("kill-after-records", 0,
		"simulate a worker crash after processing this many records (0 = off)")
	disableDedup := flag.Bool("disable-dedup", false,
		"turn OFF sink-side dedup so you can SEE duplicates after a crash (demo)")
	barrierMs := flag.Int("barrier-ms", 300, "checkpoint barrier cadence")
	query := flag.String("sql", "", "SQL query (see package doc for grammar)")
	flag.Parse()

	if *query == "" || *statePath == "" {
		log.Fatalf("usage: sql-job -broker ... -state DIR -sql \"...\"")
	}

	q, err := sql.Parse(*query)
	if err != nil {
		log.Fatalf("sql: %v", err)
	}
	fmt.Printf(">>> parsed SQL: SELECT %s, COUNT(*) FROM %s GROUP BY %s, TUMBLE(%s, %s) EMIT ON WATERMARK",
		q.KeyCol, q.SourceTopic, q.KeyCol, q.TsCol, q.WindowSize)
	if q.SinkTopic != "" {
		fmt.Printf(" INTO %s", q.SinkTopic)
	}
	fmt.Println()

	g := q.Plan(*broker, *group).WithState(*statePath)

	fmt.Printf(">>> running checkpointed DAG (barrier=%dms, kill-after=%d, dedup=%v)\n",
		*barrierMs, *killAfter, !*disableDedup)
	start := time.Now()
	stats, err := g.RunCheckpointed(context.Background(),
		time.Duration(*idleMs)*time.Millisecond,
		dataflow.CheckpointOptions{
			BarrierInterval:  time.Duration(*barrierMs) * time.Millisecond,
			KillAfterRecords: *killAfter,
			DisableSinkDedup: *disableDedup,
		})
	if err != nil {
		log.Fatalf("run: %v", err)
	}
	elapsed := time.Since(start)
	fmt.Printf(">>> DAG finished in %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("    records in:  %d\n", stats.RecordsIn)
	fmt.Printf("    records out: %d\n", stats.RecordsOut)

	// Tiny helper so the demo script can grep a deterministic marker.
	_ = os.Stdout.Sync()
	fmt.Printf(">>> SQL_JOB_DONE in=%d out=%d elapsed_ms=%d\n",
		stats.RecordsIn, stats.RecordsOut, elapsed.Milliseconds())
}

