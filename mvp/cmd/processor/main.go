// processor: minimal "stream processor" DAG. M2-aware.
// Reads integers from --in topic as group --group across ALL partitions,
// doubles them, writes to --out topic (producer-side hash partitioning).
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type partitionMeta struct {
	Partition int   `json:"partition"`
	Leader    int   `json:"leader"`
	Followers []int `json:"followers"`
}
type topicMeta struct {
	Topic      string          `json:"topic"`
	Partitions []partitionMeta `json:"partitions"`
}
type metadataResp struct {
	Topics  []topicMeta    `json:"topics"`
	Brokers map[string]any `json:"brokers"`
}

type consumeRecord struct {
	Offset    uint64 `json:"offset"`
	Partition int    `json:"partition"`
	Payload   string `json:"payload"`
}
type consumeResp struct {
	Records []consumeRecord `json:"records"`
	Next    uint64          `json:"next"`
}

func fetchMeta(broker, topic string) (topicMeta, map[int]string) {
	u := broker + "/metadata?topic=" + url.QueryEscape(topic)
	resp, err := http.Get(u)
	if err != nil {
		log.Fatalf("metadata %s: %v", topic, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		log.Fatalf("metadata %s status=%d body=%s", topic, resp.StatusCode, body)
	}
	var m metadataResp
	if err := json.Unmarshal(body, &m); err != nil {
		log.Fatalf("metadata decode: %v", err)
	}
	brokers := make(map[int]string)
	for k, v := range m.Brokers {
		id, err := strconv.Atoi(k)
		if err != nil {
			continue
		}
		if s, ok := v.(string); ok {
			brokers[id] = s
		}
	}
	return m.Topics[0], brokers
}

func main() {
	addr := flag.String("broker", "http://127.0.0.1:9092", "broker URL")
	inTopic := flag.String("in", "numbers", "input topic")
	outTopic := flag.String("out", "doubled", "output topic")
	group := flag.String("group", "doubler", "consumer group for the input")
	idleMs := flag.Int("idle-ms", 1500, "stop after this many ms with no new input")
	flag.Parse()

	inMeta, brokers := fetchMeta(*addr, *inTopic)
	outMeta, outBrokers := fetchMeta(*addr, *outTopic)

	var processed int
	var idleSince time.Time
	for {
		got := 0
		for _, p := range inMeta.Partitions {
			leaderURL := brokers[p.Leader]
			u := fmt.Sprintf("%s/consume?topic=%s&group=%s&partition=%d&max=100",
				leaderURL, url.QueryEscape(*inTopic), url.QueryEscape(*group), p.Partition)
			resp, err := http.Get(u)
			if err != nil {
				log.Fatalf("consume: %v", err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				log.Fatalf("consume status=%d body=%s", resp.StatusCode, string(body))
			}
			var cr consumeResp
			if err := json.Unmarshal(body, &cr); err != nil {
				log.Fatalf("decode: %v", err)
			}
			for _, r := range cr.Records {
				pl, err := base64.StdEncoding.DecodeString(r.Payload)
				if err != nil {
					log.Fatalf("payload decode: %v", err)
				}
				n, err := strconv.ParseInt(string(pl), 10, 64)
				if err != nil {
					log.Printf("skip non-integer off=%d: %q", r.Offset, string(pl))
					continue
				}
				doubled := []byte(strconv.FormatInt(n*2, 10))
				// hash-partition on the original value so outputs fan out
				key := []byte(strconv.FormatInt(n, 10))
				if err := produceOut(outMeta, outBrokers, *outTopic, key, doubled); err != nil {
					log.Fatalf("produce: %v", err)
				}
				processed++
				got++
			}
		}
		if got == 0 {
			if idleSince.IsZero() {
				idleSince = time.Now()
			}
			if time.Since(idleSince) > time.Duration(*idleMs)*time.Millisecond {
				break
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		idleSince = time.Time{}
	}
	fmt.Printf("processor done: processed=%d in=%q -> out=%q\n", processed, *inTopic, *outTopic)
}

func produceOut(meta topicMeta, brokers map[int]string, topic string, key, payload []byte) error {
	n := len(meta.Partitions)
	h := fnv.New32a()
	h.Write(key)
	part := int(h.Sum32() % uint32(n))
	leader := meta.Partitions[part].Leader
	u := fmt.Sprintf("%s/produce?topic=%s&partition=%d&key=%s",
		brokers[leader], url.QueryEscape(topic), part, url.QueryEscape(string(key)))
	resp, err := http.Post(u, "application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, string(b))
	}
	return nil
}
