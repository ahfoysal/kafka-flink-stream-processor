// event_time_wordcount: M4 demo.
//
// Pipeline:
//
//	Source("et_lines")  (event-time extracted from payload)
//	  -> FlatMap(tokenize; preserve per-token event-time)
//	  -> GroupByKey(by-word)
//	  -> TumblingWindow(5s)    [keyed + stateful + watermark-driven]
//	  -> Map(encode "word\tws_ms\twe_ms\tcount")
//	  -> Sink("et_counts")
//
// Payload format on the input topic is: "<event_ms>\t<sentence>". The
// extractor parses the prefix; the tokenizer drops the prefix and attaches
// the event-time to every emitted token record. Source-side watermarks
// (bounded out-of-orderness) drive window firing downstream.
//
// The demo input intentionally contains out-of-order records AND one late
// record that arrives after the watermark has passed its window. We verify:
//   - per-window counts match the expected tally
//   - the late record is either dropped (too late) or included (if within
//     allowed_lateness).
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ahfoysal/kafka-flink-stream-processor/mvp/internal/dataflow"
)

// eventTimeExtractor parses "<event_ms>\t..." and returns the time component.
// Falls back to ingestion time if the payload doesn't start with a decimal.
func eventTimeExtractor(r dataflow.Record) time.Time {
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

func main() {
	broker := flag.String("broker", "http://127.0.0.1:39092", "any-broker URL")
	inTopic := flag.String("in", "et_lines", "input topic")
	outTopic := flag.String("out", "et_counts", "output topic")
	group := flag.String("group", "et-wc", "consumer group")
	statePath := flag.String("state", "", "directory for bbolt state files")
	idleMs := flag.Int("idle-ms", 3500, "stop after this many ms with no new input")
	windowSec := flag.Int("window", 5, "tumbling window size in seconds")
	outOfOrderMs := flag.Int("out-of-order", 1000, "max out-of-orderness in ms (watermark lag)")
	lateMs := flag.Int("late", 500, "allowed lateness after window end in ms")
	produceInput := flag.Bool("produce", false, "produce a fixed event-time demo dataset and exit")
	seed := flag.Int64("seed", 42, "shuffle seed when -produce is set")
	flag.Parse()

	if *produceInput {
		produceDemoData(*broker, *inTopic, *seed)
		return
	}

	var dropped []string
	g := dataflow.Source(*broker, *inTopic, *group).
		WithCheckpointInterval(500 * time.Millisecond).
		WithTimestamps(eventTimeExtractor, time.Duration(*outOfOrderMs)*time.Millisecond).
		WithAllowedLateness(time.Duration(*lateMs) * time.Millisecond).
		OnLate(func(r dataflow.Record) {
			dropped = append(dropped, string(r.Value))
		}).
		FlatMap("tokenize", func(r dataflow.Record) ([]dataflow.Record, error) {
			// Split "<ms>\tsentence" and preserve event-time on each token.
			// We stash the original Value (with event_ms prefix) so the
			// window extractor can re-read it on tokens too.
			s := string(r.Value)
			tab := strings.IndexByte(s, '\t')
			if tab <= 0 {
				return nil, nil
			}
			evMs := s[:tab]
			body := strings.ToLower(s[tab+1:])
			out := make([]dataflow.Record, 0, 8)
			cur := strings.Builder{}
			flush := func() {
				if cur.Len() == 0 {
					return
				}
				tok := cur.String()
				cur.Reset()
				// Re-prefix with event_ms so the extractor still works on
				// downstream records; the window op uses in.Key for keying.
				out = append(out, dataflow.Record{
					Key:       []byte(tok),
					Value:     []byte(evMs + "\t" + tok),
					Timestamp: r.Timestamp,
				})
			}
			for _, ch := range body {
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
		TumblingWindow("tumble", time.Duration(*windowSec)*time.Second).
		Map("encode", func(r dataflow.Record) (dataflow.Record, error) {
			// TumblingWindow emits Value="<start_ms>\t<end_ms>\t<count>".
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

	fmt.Printf(">>> running event-time wordcount DAG: Source(%s) -> FlatMap -> GroupByKey -> TumblingWindow(%ds) -> Sink(%s)\n",
		*inTopic, *windowSec, *outTopic)
	fmt.Printf("    watermark strategy: max_event_time - %dms; allowed_lateness=%dms\n",
		*outOfOrderMs, *lateMs)
	start := time.Now()
	stats, err := g.Run(context.Background(), time.Duration(*idleMs)*time.Millisecond)
	if err != nil {
		log.Fatalf("run: %v", err)
	}
	elapsed := time.Since(start)
	fmt.Printf(">>> DAG finished in %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("    records in:  %d\n", stats.RecordsIn)
	fmt.Printf("    records out: %d  (= unique (word, window) emissions)\n", stats.RecordsOut)
	if len(dropped) > 0 {
		fmt.Printf("    late-dropped: %d\n", len(dropped))
		for _, d := range dropped {
			fmt.Printf("      -> %q\n", d)
		}
	}

	// Drain the sink topic and print per-window totals.
	printWindowedCounts(*broker, *outTopic, "et-reader-"+strconv.FormatInt(time.Now().UnixNano(), 10))
}

// produceDemoData posts a deterministic set of event-time records to the
// input topic. Event times span three contiguous 5s windows; record arrival
// order is shuffled (out-of-order), and we include one deliberately-late
// record outside the allowed-lateness bound to prove that path works.
func produceDemoData(brokerAddr, topic string, seed int64) {
	// Base time aligned to a 5s boundary so windows are predictable.
	base := time.UnixMilli((time.Now().UnixMilli() / 5000) * 5000)

	type ev struct {
		EventMs int64
		Line    string
	}

	// Three windows: W1 [base, base+5s), W2 [+5s, +10s), W3 [+10s, +15s).
	events := []ev{
		// --- W1 ---
		{base.Add(500 * time.Millisecond).UnixMilli(), "the quick brown fox"},
		{base.Add(1500 * time.Millisecond).UnixMilli(), "the lazy dog"},
		{base.Add(2500 * time.Millisecond).UnixMilli(), "the quick fox"},
		{base.Add(4500 * time.Millisecond).UnixMilli(), "fox fox fox"},
		// --- W2 ---
		{base.Add(5500 * time.Millisecond).UnixMilli(), "the dog"},
		{base.Add(6500 * time.Millisecond).UnixMilli(), "the quick brown dog"},
		{base.Add(7500 * time.Millisecond).UnixMilli(), "fox and dog"},
		{base.Add(9000 * time.Millisecond).UnixMilli(), "the the the"},
		// --- W3 ---
		{base.Add(10500 * time.Millisecond).UnixMilli(), "stream processing is fun"},
		{base.Add(12000 * time.Millisecond).UnixMilli(), "fox jumps over"},
		{base.Add(13500 * time.Millisecond).UnixMilli(), "the quick"},
		{base.Add(14500 * time.Millisecond).UnixMilli(), "stream stream stream"},
		// Late but inside allowed-lateness (500ms): fits if watermark hasn't
		// yet crossed window_end + 500ms.
		{base.Add(3000 * time.Millisecond).UnixMilli(), "late fox"},
	}

	// Shuffle to simulate out-of-order arrival.
	rng := rand.New(rand.NewSource(seed))
	rng.Shuffle(len(events), func(i, j int) { events[i], events[j] = events[j], events[i] })

	// Append one EXTRA-late record at the very end so its event-time is far
	// behind the current watermark and it must be dropped.
	extraLate := ev{
		EventMs: base.Add(100 * time.Millisecond).UnixMilli(),
		Line:    "droppedlate word",
	}

	fmt.Printf(">>> producing %d event-time records (shuffled, seed=%d)\n", len(events)+1, seed)
	fmt.Printf("    base window boundary = %s (ms=%d)\n",
		base.Format("15:04:05.000"), base.UnixMilli())
	for _, e := range events {
		payload := fmt.Sprintf("%d\t%s", e.EventMs, e.Line)
		if err := httpProduce(brokerAddr, topic, payload); err != nil {
			log.Fatalf("produce: %v", err)
		}
	}
	// Give the pipeline time to advance its watermark past W1/W2 before we
	// send the too-late record. The demo script waits before launching the
	// processor so this just guarantees we arrive chronologically last.
	time.Sleep(50 * time.Millisecond)
	if err := httpProduce(brokerAddr, topic,
		fmt.Sprintf("%d\t%s", extraLate.EventMs, extraLate.Line)); err != nil {
		log.Fatalf("produce late: %v", err)
	}
	fmt.Printf("    produced (last record is intentionally too-late at ms=%d)\n",
		extraLate.EventMs)
}

func httpProduce(brokerAddr, topic, payload string) error {
	u := brokerAddr + "/produce?topic=" + url.QueryEscape(topic)
	resp, err := http.Post(u, "application/octet-stream", strings.NewReader(payload))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		// Follow 307 redirect to partition leader.
		loc := resp.Header.Get("Location")
		if loc == "" {
			return fmt.Errorf("redirect with empty Location")
		}
		resp2, err := http.Post(loc, "application/octet-stream", strings.NewReader(payload))
		if err != nil {
			return err
		}
		defer resp2.Body.Close()
		if resp2.StatusCode != 200 {
			body, _ := io.ReadAll(resp2.Body)
			return fmt.Errorf("produce: %d %s", resp2.StatusCode, body)
		}
		return nil
	}
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("produce: %d %s", resp.StatusCode, body)
	}
	return nil
}

type windowKey struct {
	word    string
	startMs int64
	endMs   int64
}

// printWindowedCounts drains the sink topic and prints (word, window, count).
func printWindowedCounts(brokerAddr, topic, group string) {
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
	r, err := http.Get(brokerAddr + "/metadata?topic=" + url.QueryEscape(topic))
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
	if len(m.Topics) == 0 {
		return
	}
	// Aggregate across partitions: windowKey -> total count. TumblingWindow
	// emits one final count per (key, window) per partition shard; global
	// answer is the sum.
	agg := map[windowKey]uint64{}
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
				// Payload: "word\tstart_ms\tend_ms\tcount"
				parts := strings.Split(string(pl), "\t")
				if len(parts) != 4 {
					continue
				}
				s, _ := strconv.ParseInt(parts[1], 10, 64)
				e, _ := strconv.ParseInt(parts[2], 10, 64)
				c, _ := strconv.ParseUint(parts[3], 10, 64)
				agg[windowKey{parts[0], s, e}] += c
			}
		}
	}
	// Group by window, then word within window, sorted.
	type winGroup struct {
		startMs, endMs int64
	}
	winWords := map[winGroup][]struct {
		word  string
		count uint64
	}{}
	for k, c := range agg {
		g := winGroup{k.startMs, k.endMs}
		winWords[g] = append(winWords[g], struct {
			word  string
			count uint64
		}{k.word, c})
	}
	var wins []winGroup
	for w := range winWords {
		wins = append(wins, w)
	}
	sort.Slice(wins, func(i, j int) bool {
		if wins[i].startMs != wins[j].startMs {
			return wins[i].startMs < wins[j].startMs
		}
		return wins[i].endMs < wins[j].endMs
	})
	fmt.Printf(">>> windowed counts (per 5s tumbling window)\n")
	for _, w := range wins {
		words := winWords[w]
		sort.Slice(words, func(i, j int) bool {
			if words[i].count != words[j].count {
				return words[i].count > words[j].count
			}
			return words[i].word < words[j].word
		})
		fmt.Printf("  window [%d, %d)  (%s - %s)\n",
			w.startMs, w.endMs,
			time.UnixMilli(w.startMs).Format("15:04:05"),
			time.UnixMilli(w.endMs).Format("15:04:05"))
		for _, kv := range words {
			fmt.Printf("    %-14s %d\n", kv.word, kv.count)
		}
	}
}
