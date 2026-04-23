// producer: sends N messages to a topic. M2: partition-aware.
//
// The producer first fetches /metadata?topic=X from any broker to learn the
// partition layout (leader + followers per partition), then routes each
// record directly to its partition leader. If -key is set, the key is
// hash(key) %% N; otherwise it's round-robin.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
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
	Brokers map[string]any `json:"brokers"` // json decodes int keys to strings
}

type produceResp struct {
	Partition int    `json:"partition"`
	Offset    uint64 `json:"offset"`
	Leader    int    `json:"leader"`
}

func fetchMetadata(broker, topic string) (topicMeta, map[int]string, error) {
	u := broker + "/metadata?topic=" + url.QueryEscape(topic)
	resp, err := http.Get(u)
	if err != nil {
		return topicMeta{}, nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return topicMeta{}, nil, fmt.Errorf("metadata %d: %s", resp.StatusCode, string(body))
	}
	var m metadataResp
	if err := json.Unmarshal(body, &m); err != nil {
		return topicMeta{}, nil, err
	}
	if len(m.Topics) == 0 {
		return topicMeta{}, nil, fmt.Errorf("no metadata for topic %q", topic)
	}
	brokers := make(map[int]string)
	for k, v := range m.Brokers {
		id, err := strconv.Atoi(k)
		if err != nil {
			continue
		}
		s, ok := v.(string)
		if !ok {
			continue
		}
		brokers[id] = s
	}
	return m.Topics[0], brokers, nil
}

func partitionFor(key []byte, n int, rr int) int {
	if n <= 0 {
		return 0
	}
	if len(key) == 0 {
		return rr % n
	}
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32() % uint32(n))
}

func main() {
	addr := flag.String("broker", "http://127.0.0.1:9092", "broker URL (any member)")
	topic := flag.String("topic", "demo", "topic name")
	count := flag.Int("count", 100, "number of messages")
	prefix := flag.String("prefix", "msg", "message body prefix")
	keyed := flag.Bool("keyed", false, "if true, use msg index as key (partition by key hash)")
	flag.Parse()

	meta, brokers, err := fetchMetadata(*addr, *topic)
	if err != nil {
		log.Fatalf("metadata: %v", err)
	}
	n := len(meta.Partitions)
	if n == 0 {
		log.Fatalf("topic %q has 0 partitions", *topic)
	}

	client := &http.Client{}
	perPartition := make([]int, n)

	for i := 0; i < *count; i++ {
		body := fmt.Sprintf("%s-%d", *prefix, i)
		var key []byte
		if *keyed {
			// key = "k<i>" so each message has a distinct key spread across partitions.
			key = []byte(fmt.Sprintf("k%d", i))
		}
		part := partitionFor(key, n, i)
		leader := meta.Partitions[part].Leader
		leaderURL, ok := brokers[leader]
		if !ok {
			log.Fatalf("unknown leader %d for partition %d", leader, part)
		}
		u := fmt.Sprintf("%s/produce?topic=%s&partition=%d&key=%s",
			leaderURL, url.QueryEscape(*topic), part, url.QueryEscape(string(key)))
		resp, err := client.Post(u, "application/octet-stream", bytes.NewReader([]byte(body)))
		if err != nil {
			log.Fatalf("produce %d: %v", i, err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			log.Fatalf("produce %d: status=%d body=%s", i, resp.StatusCode, string(b))
		}
		var pr produceResp
		_ = json.Unmarshal(b, &pr)
		perPartition[pr.Partition]++
	}
	fmt.Printf("produced %d messages to topic=%q across %d partitions\n", *count, *topic, n)
	for i, c := range perPartition {
		fmt.Printf("  partition %d: %d msgs (leader=b%d)\n", i, c, meta.Partitions[i].Leader)
	}
}
