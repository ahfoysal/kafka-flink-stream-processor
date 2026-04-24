package sql

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ahfoysal/kafka-flink-stream-processor/mvp/internal/dataflow"
)

// Plan lowers a parsed Query into a StreamGraph.
//
// Wire payload convention (matches event_time_wordcount):
//
//   "<event_ms>\t<key>[\t<...rest>]"
//
// The tokenizer splits on the first tab: the prefix is the event-time, the
// suffix is the key (and optional trailing data we ignore for COUNT).
//
// Lowering:
//   SELECT k, COUNT(*) FROM t
//    GROUP BY k, TUMBLE(ts, INTERVAL 'n' SECOND) EMIT ON WATERMARK
//      [INTO sink]
//
//   -> Source(t)
//        .WithTimestamps(extractor, maxOOO)
//        .Map(parseKey)
//        .GroupByKey()
//        .TumblingWindow(n)
//        .Map(encode "k\tws\twe\tcount")
//        [.Sink(sink)]
//
// maxOOO defaults to 1s; allowed_late defaults to 0 (= EMIT ON WATERMARK
// fires windows as soon as the watermark crosses window_end).
func (q *Query) Plan(brokerAddr, group string) *dataflow.StreamGraph {
	extractor := func(r dataflow.Record) time.Time {
		s := string(r.Value)
		tab := strings.IndexByte(s, '\t')
		if tab <= 0 {
			return r.Timestamp
		}
		ms, err := strconv.ParseInt(s[:tab], 10, 64)
		if err != nil {
			return r.Timestamp
		}
		return time.UnixMilli(ms)
	}

	g := dataflow.Source(brokerAddr, q.SourceTopic, group).
		WithCheckpointInterval(500 * time.Millisecond).
		WithTimestamps(extractor, 1*time.Second).
		WithAllowedLateness(0).
		Map("parse-key", func(r dataflow.Record) (dataflow.Record, error) {
			// Watermarks flow through untouched — they're control records,
			// not data.
			if dataflow.IsWatermark(r) {
				return r, nil
			}
			// Extract the key column; here the "key column" is whatever
			// token immediately follows the event-time prefix. We preserve
			// the full payload as Value so the window extractor still works.
			s := string(r.Value)
			tab := strings.IndexByte(s, '\t')
			if tab <= 0 {
				return r, fmt.Errorf("sql: expected '<ms>\\t<key>', got %q", s)
			}
			rest := s[tab+1:]
			// Take first whitespace-delimited token of rest as the key.
			keyEnd := len(rest)
			for i := 0; i < len(rest); i++ {
				if rest[i] == '\t' || rest[i] == ' ' {
					keyEnd = i
					break
				}
			}
			key := rest[:keyEnd]
			return dataflow.Record{
				Key:       []byte(key),
				Value:     r.Value, // keep full payload for downstream extractor
				Timestamp: r.Timestamp,
			}, nil
		}).
		GroupByKey("group-by-key", nil).
		TumblingWindow("tumble", q.WindowSize).
		Map("encode", func(r dataflow.Record) (dataflow.Record, error) {
			if dataflow.IsWatermark(r) {
				return r, nil
			}
			// Window op emits value="<start_ms>\t<end_ms>\t<count>"; prepend
			// the key so downstream SELECT-style consumers can read a single
			// "key\tstart\tend\tcount" row.
			return dataflow.Record{
				Key:       r.Key,
				Value:     []byte(fmt.Sprintf("%s\t%s", r.Key, r.Value)),
				Timestamp: r.Timestamp,
			}, nil
		})
	if q.SinkTopic != "" {
		g.Sink(q.SinkTopic)
	}
	return g
}
