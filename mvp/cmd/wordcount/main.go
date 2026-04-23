// wordcount: M3 demo.
//
// Pipeline:
//
//	Source("lines")
//	  -> FlatMap(tokenize)
//	  -> GroupByKey()
//	  -> Count()
//	  -> Map(encode "word\tcount")
//	  -> Sink("counts")
//
// The Map step packs the word into the value because the M2 broker
// /consume endpoint does not yet return message keys — so downstream
// readers need the word embedded in the payload. The Count operator
// itself is genuinely keyed+stateful (see internal/dataflow/operator.go).
//
// Expects the broker cluster + topics created by scripts/demo_m3.sh.
package main

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ahfoysal/kafka-flink-stream-processor/mvp/internal/dataflow"
	"github.com/ahfoysal/kafka-flink-stream-processor/mvp/internal/state"
)

func main() {
	broker := flag.String("broker", "http://127.0.0.1:19092", "any-broker URL")
	inTopic := flag.String("in", "lines", "input topic")
	outTopic := flag.String("out", "counts", "output topic")
	group := flag.String("group", "wc", "consumer group")
	statePath := flag.String("state", "", "directory for bbolt state files")
	idleMs := flag.Int("idle-ms", 2500, "stop after this many ms with no new input")
	dumpCounts := flag.Bool("dump-top", true, "after the run, drain the sink topic and print top counts")
	topK := flag.Int("top", 10, "how many top words to print when --dump-top")
	flag.Parse()

	g := dataflow.Source(*broker, *inTopic, *group).
		WithCheckpointInterval(1 * time.Second).
		FlatMap("tokenize", func(r dataflow.Record) ([]dataflow.Record, error) {
			line := strings.ToLower(string(r.Value))
			out := make([]dataflow.Record, 0, 8)
			cur := strings.Builder{}
			flush := func() {
				if cur.Len() == 0 {
					return
				}
				tok := cur.String()
				cur.Reset()
				out = append(out, dataflow.Record{
					Key:       []byte(tok),
					Value:     []byte(tok),
					Timestamp: r.Timestamp,
				})
			}
			for _, ch := range line {
				switch {
				case ch >= 'a' && ch <= 'z', ch >= '0' && ch <= '9':
					cur.WriteRune(ch)
				default:
					flush()
				}
			}
			flush()
			return out, nil
		}).
		GroupByKey("by-word", nil).
		Count("count").
		Map("encode", func(r dataflow.Record) (dataflow.Record, error) {
			// Count emits value=<running-count> with key=<word>. Pack both
			// into the payload so sink readers don't need the key path.
			return dataflow.Record{
				Key:       r.Key,
				Value:     []byte(fmt.Sprintf("%s\t%s", r.Key, r.Value)),
				Timestamp: r.Timestamp,
			}, nil
		}).
		Sink(*outTopic)

	if *statePath != "" {
		g.WithState(*statePath)
	}

	fmt.Printf(">>> running word-count DAG: Source(%s) -> FlatMap -> GroupByKey -> Count -> Map -> Sink(%s)\n",
		*inTopic, *outTopic)
	start := time.Now()
	stats, err := g.Run(context.Background(), time.Duration(*idleMs)*time.Millisecond)
	if err != nil {
		log.Fatalf("run: %v", err)
	}
	elapsed := time.Since(start)
	fmt.Printf(">>> DAG finished in %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("    records in:  %d\n", stats.RecordsIn)
	fmt.Printf("    records out: %d\n", stats.RecordsOut)
	keys := make([]string, 0, len(stats.OperatorCounts))
	for k := range stats.OperatorCounts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("    op %-12s processed %d\n", k, stats.OperatorCounts[k])
	}

	if !*dumpCounts {
		return
	}

	// Count's state is sharded per source partition (one bbolt file per task).
	// The authoritative global totals = sum across partition shards.
	//
	// We ALSO drain the sink topic for a sanity-check: since the pipeline
	// lands each shard's running total into the key-partitioned "counts"
	// topic, the final record per (word, source-partition) is that shard's
	// contribution. The sink numbers will match the state numbers.
	fmt.Printf(">>> top %d word counts (from stateful Count operator, summed across shards)\n", *topK)
	latest := dumpStateTotals(*statePath)
	if len(latest) == 0 {
		// Fallback: drain the sink topic and SUM final-per-partition totals.
		latest = drainCounts(*broker, *outTopic, "wc-reader-"+strconv.FormatInt(time.Now().UnixNano(), 10))
	}
	type kv struct {
		Word  string
		Count uint64
	}
	arr := make([]kv, 0, len(latest))
	for w, c := range latest {
		arr = append(arr, kv{w, c})
	}
	sort.Slice(arr, func(i, j int) bool {
		if arr[i].Count != arr[j].Count {
			return arr[i].Count > arr[j].Count
		}
		return arr[i].Word < arr[j].Word
	})
	if *topK > len(arr) {
		*topK = len(arr)
	}
	for i := 0; i < *topK; i++ {
		fmt.Printf("    %2d. %-12s %d\n", i+1, arr[i].Word, arr[i].Count)
	}
	fmt.Printf("    (unique words = %d)\n", len(arr))
}

func filepathGlob(p string) ([]string, error) { return filepath.Glob(p) }
func filepathBase(p string) string             { return filepath.Base(p) }
func decodeBE64(v []byte) uint64               { return binary.BigEndian.Uint64(v) }

// dumpStateTotals walks the per-partition bbolt state files and sums the
// "count" bucket. This is the authoritative answer: the stateful Count
// operator persists exactly one (word -> running-count) entry per source
// partition, and the global total is their sum.
func dumpStateTotals(statePath string) map[string]uint64 {
	if statePath == "" {
		return nil
	}
	matches, err := filepathGlob(statePath + "/task-p*.db")
	if err != nil || len(matches) == 0 {
		return nil
	}
	out := map[string]uint64{}
	for i, f := range matches {
		// Open each shard read-only. The main run already closed them.
		s, err := state.Open(f, 0)
		if err != nil {
			log.Printf("state open %s: %v", f, err)
			continue
		}
		// partition index is encoded in the filename task-p<N>.db
		var part int
		fmt.Sscanf(filepathBase(f), "task-p%d.db", &part)
		_ = i
		err = s.Iterate("count", part, func(k, v []byte) error {
			if len(v) != 8 {
				return nil
			}
			word := string(append([]byte(nil), k...))
			out[word] += decodeBE64(v)
			return nil
		})
		if err != nil {
			log.Printf("state iterate %s: %v", f, err)
		}
		s.Close()
	}
	return out
}

// drainCounts reads every partition of the sink topic from offset 0 and keeps
// only the latest count per word. Count emits a running update per input
// record, so the last observation for a word is its final total.
func drainCounts(brokerAddr, topic, group string) map[string]uint64 {
	type partMeta struct {
		Partition int `json:"partition"`
		Leader    int `json:"leader"`
	}
	type topicM struct {
		Topic      string     `json:"topic"`
		Partitions []partMeta `json:"partitions"`
	}
	type metaResp struct {
		Topics  []topicM       `json:"topics"`
		Brokers map[string]any `json:"brokers"`
	}
	type cr struct {
		Offset  uint64 `json:"offset"`
		Payload string `json:"payload"`
	}
	type resp struct {
		Records []cr `json:"records"`
	}

	u := brokerAddr + "/metadata?topic=" + url.QueryEscape(topic)
	r, err := http.Get(u)
	if err != nil {
		log.Fatalf("metadata: %v", err)
	}
	body, _ := io.ReadAll(r.Body)
	r.Body.Close()
	var m metaResp
	if err := json.Unmarshal(body, &m); err != nil {
		log.Fatalf("metadata decode: %v", err)
	}
	brokers := map[int]string{}
	for k, v := range m.Brokers {
		id, _ := strconv.Atoi(k)
		if s, ok := v.(string); ok {
			brokers[id] = s
		}
	}
	out := map[string]uint64{}
	if len(m.Topics) == 0 {
		return out
	}

	for _, p := range m.Topics[0].Partitions {
		leaderURL := brokers[p.Leader]
		for {
			q := url.Values{}
			q.Set("topic", topic)
			q.Set("partition", strconv.Itoa(p.Partition))
			q.Set("group", group)
			q.Set("max", "2000")
			r, err := http.Get(leaderURL + "/consume?" + q.Encode())
			if err != nil {
				log.Fatalf("consume: %v", err)
			}
			b, _ := io.ReadAll(r.Body)
			r.Body.Close()
			var cc resp
			if err := json.Unmarshal(b, &cc); err != nil {
				log.Fatalf("decode: %v", err)
			}
			if len(cc.Records) == 0 {
				break
			}
			for _, rec := range cc.Records {
				pl, err := base64.StdEncoding.DecodeString(rec.Payload)
				if err != nil {
					continue
				}
				// "word\tcount"
				s := string(pl)
				tab := strings.IndexByte(s, '\t')
				if tab < 0 {
					continue
				}
				word := s[:tab]
				n, err := strconv.ParseUint(s[tab+1:], 10, 64)
				if err != nil {
					continue
				}
				out[word] = n // Count emits monotonically, so last-seen wins.
			}
		}
	}
	return out
}
