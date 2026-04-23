// consumer: M2 partition-aware consumer. Supports consumer groups with a
// simple range assignment driven by -members/-member-id flags (no on-broker
// coordinator yet; the members agree on the layout locally because the
// partition list from /metadata is deterministic).
package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
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
	Records   []consumeRecord `json:"records"`
	Next      uint64          `json:"next"`
	Partition int             `json:"partition"`
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

// rangeAssign returns the partitions assigned to memberIdx out of members
// using Kafka's range-style assignment: first (n/members) + (1 if idx<n%members else 0) partitions.
func rangeAssign(numPartitions, members, memberIdx int) []int {
	if members <= 0 {
		members = 1
	}
	parts := make([]int, 0)
	// simple contiguous block
	base := numPartitions / members
	rem := numPartitions % members
	start := memberIdx*base + min(memberIdx, rem)
	size := base
	if memberIdx < rem {
		size++
	}
	for i := 0; i < size; i++ {
		parts = append(parts, start+i)
	}
	return parts
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	addr := flag.String("broker", "http://127.0.0.1:9092", "broker URL (any member)")
	topic := flag.String("topic", "demo", "topic")
	group := flag.String("group", "g1", "consumer group")
	expect := flag.Int("expect", 0, "expected record count (0 = drain until idle)")
	printBodies := flag.Bool("print", false, "print each payload body")
	members := flag.Int("members", 1, "consumer group size")
	memberIdx := flag.Int("member-id", 0, "this consumer's index in the group (0-based)")
	partitionsFlag := flag.String("partitions", "", "explicit comma-separated partition list (overrides range assignment)")
	tag := flag.String("tag", "", "label used in log output (defaults to m<idx>)")
	flag.Parse()

	if *tag == "" {
		*tag = fmt.Sprintf("m%d", *memberIdx)
	}

	meta, brokers, err := fetchMetadata(*addr, *topic)
	if err != nil {
		log.Fatalf("metadata: %v", err)
	}

	var myParts []int
	if *partitionsFlag != "" {
		for _, s := range splitCSV(*partitionsFlag) {
			v, err := strconv.Atoi(s)
			if err != nil {
				log.Fatalf("bad -partitions: %v", err)
			}
			myParts = append(myParts, v)
		}
	} else {
		myParts = rangeAssign(len(meta.Partitions), *members, *memberIdx)
	}
	sort.Ints(myParts)
	fmt.Printf("[%s] assigned partitions=%v (group=%s, members=%d)\n", *tag, myParts, *group, *members)

	// per-partition state: highest offset seen (for ordering check)
	lastOff := make(map[int]int64)
	for _, p := range myParts {
		lastOff[p] = -1
	}

	total := 0
	idleSince := time.Time{}

	for {
		got := 0
		for _, p := range myParts {
			leader := meta.Partitions[p].Leader
			leaderURL := brokers[leader]
			u, _ := url.Parse(leaderURL + "/consume")
			q := u.Query()
			q.Set("topic", *topic)
			q.Set("group", *group)
			q.Set("partition", strconv.Itoa(p))
			q.Set("max", "100")
			u.RawQuery = q.Encode()

			resp, err := http.Get(u.String())
			if err != nil {
				log.Fatalf("consume p=%d: %v", p, err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				log.Fatalf("consume p=%d status=%d body=%s", p, resp.StatusCode, string(body))
			}
			var cr consumeResp
			if err := json.Unmarshal(body, &cr); err != nil {
				log.Fatalf("decode: %v", err)
			}
			for _, r := range cr.Records {
				if int64(r.Offset) != lastOff[p]+1 {
					log.Fatalf("[%s] ORDER VIOLATION p=%d: got offset %d after %d",
						*tag, p, r.Offset, lastOff[p])
				}
				lastOff[p] = int64(r.Offset)
				total++
				got++
				if *printBodies {
					pl, err := base64.StdEncoding.DecodeString(r.Payload)
					if err != nil {
						log.Fatalf("payload decode: %v", err)
					}
					fmt.Printf("  [%s] p=%d off=%d payload=%s\n", *tag, p, r.Offset, string(pl))
				}
			}
		}
		if got == 0 {
			if idleSince.IsZero() {
				idleSince = time.Now()
			}
			if *expect > 0 && total >= *expect {
				break
			}
			if time.Since(idleSince) > 1500*time.Millisecond {
				break
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
		idleSince = time.Time{}
		if *expect > 0 && total >= *expect {
			break
		}
	}
	fmt.Printf("[%s] consumed %d records from topic=%q group=%q partitions=%v\n",
		*tag, total, *topic, *group, myParts)
}

func splitCSV(s string) []string {
	out := []string{}
	cur := ""
	for _, r := range s {
		if r == ',' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
