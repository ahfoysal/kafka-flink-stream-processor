package dataflow

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// Minimal HTTP client for the M2 broker. We keep it private to the dataflow
// package; cmd/wordcount never touches it directly.

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
	// NOTE: the M2 broker does not yet return message keys on /consume
	// (see internal/broker/broker.go handleConsume). Sources that need a key
	// derive it from the payload via a Map step.
}
type consumeResp struct {
	Records   []consumeRecord `json:"records"`
	Next      uint64          `json:"next"`
	Partition int             `json:"partition"`
}

// BrokerView is a small, cached view of cluster metadata. It is NOT
// auto-refreshed during a run — M3 pipelines finish fast enough that
// leader changes are out of scope (M4 will add failure detection anyway).
type BrokerView struct {
	AnyURL  string
	Client  *http.Client
	Topics  map[string]topicMeta
	Brokers map[int]string

	// Per-(topic, partition) produce mutex. The M2 broker's synchronous
	// replication is not concurrency-safe on a single partition: if two
	// produce requests to the same partition race, the leader may assign
	// offsets N and N+1 but the follower can see the /replicate calls in
	// reverse, creating a gap and a 500. We serialize per-partition on the
	// client side until M4 adds in-flight ordering to the broker.
	produceMu sync.Map // key: "topic|partition" -> *sync.Mutex
}

func NewBrokerView(anyBrokerURL string) *BrokerView {
	return &BrokerView{
		AnyURL: anyBrokerURL,
		Client: &http.Client{Timeout: 10 * time.Second},
		Topics: map[string]topicMeta{},
	}
}

func (b *BrokerView) partLock(topic string, partition int) *sync.Mutex {
	k := fmt.Sprintf("%s|%d", topic, partition)
	v, _ := b.produceMu.LoadOrStore(k, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func (b *BrokerView) FetchTopic(topic string) (topicMeta, error) {
	if m, ok := b.Topics[topic]; ok {
		return m, nil
	}
	u := b.AnyURL + "/metadata?topic=" + url.QueryEscape(topic)
	resp, err := b.Client.Get(u)
	if err != nil {
		return topicMeta{}, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return topicMeta{}, fmt.Errorf("metadata %s: %d %s", topic, resp.StatusCode, body)
	}
	var m metadataResp
	if err := json.Unmarshal(body, &m); err != nil {
		return topicMeta{}, err
	}
	if len(m.Topics) == 0 {
		return topicMeta{}, fmt.Errorf("no metadata for topic %q", topic)
	}
	if b.Brokers == nil || len(b.Brokers) == 0 {
		b.Brokers = map[int]string{}
		for k, v := range m.Brokers {
			id, err := strconv.Atoi(k)
			if err != nil {
				continue
			}
			if s, ok := v.(string); ok {
				b.Brokers[id] = s
			}
		}
	}
	b.Topics[topic] = m.Topics[0]
	return m.Topics[0], nil
}

// Consume pulls a batch of records from (topic, partition) via that
// partition's leader, using the given consumer group for offset tracking.
// The broker commits the group offset server-side on a successful read.
func (b *BrokerView) Consume(topic string, partition int, group string, max int) ([]Record, uint64, error) {
	tm, ok := b.Topics[topic]
	if !ok {
		return nil, 0, fmt.Errorf("no metadata for %q", topic)
	}
	leader := tm.Partitions[partition].Leader
	leaderURL, ok := b.Brokers[leader]
	if !ok {
		return nil, 0, fmt.Errorf("no broker addr for leader id %d", leader)
	}
	u, _ := url.Parse(leaderURL + "/consume")
	q := u.Query()
	q.Set("topic", topic)
	q.Set("partition", strconv.Itoa(partition))
	q.Set("group", group)
	q.Set("max", strconv.Itoa(max))
	u.RawQuery = q.Encode()
	resp, err := b.Client.Get(u.String())
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, 0, fmt.Errorf("consume %s p=%d: %d %s", topic, partition, resp.StatusCode, body)
	}
	var cr consumeResp
	if err := json.Unmarshal(body, &cr); err != nil {
		return nil, 0, err
	}
	out := make([]Record, 0, len(cr.Records))
	for _, r := range cr.Records {
		pl, err := base64.StdEncoding.DecodeString(r.Payload)
		if err != nil {
			return nil, 0, fmt.Errorf("payload decode: %w", err)
		}
		out = append(out, Record{
			Key:       nil,
			Value:     pl,
			Timestamp: time.Now(),
			Topic:     topic,
			Partition: r.Partition,
			Offset:    r.Offset,
		})
	}
	return out, cr.Next, nil
}

// Produce routes a record to its partition leader. If key is empty we
// fall back to round-robin via the caller-supplied rr counter.
//
// Retries up to 5x with a short backoff on transient failures (connection
// errors, 5xx). The M2 broker occasionally returns 502 under bursty
// concurrent produce because ISR replication is synchronous; the retry
// makes the M3 demo resilient to that.
func (b *BrokerView) Produce(topic string, key, payload []byte, rr int) (int, error) {
	tm, err := b.FetchTopic(topic)
	if err != nil {
		return 0, err
	}
	n := len(tm.Partitions)
	if n == 0 {
		return 0, fmt.Errorf("topic %q has 0 partitions", topic)
	}
	part := partitionOf(key, n)
	if len(key) == 0 {
		part = rr % n
	}
	leader := tm.Partitions[part].Leader
	leaderURL := b.Brokers[leader]
	u := fmt.Sprintf("%s/produce?topic=%s&partition=%d&key=%s",
		leaderURL, url.QueryEscape(topic), part, url.QueryEscape(string(key)))

	// Serialize per-partition to work around M2 broker replica-ordering race.
	mu := b.partLock(topic, part)
	mu.Lock()
	defer mu.Unlock()

	const maxAttempts = 5
	backoff := 20 * time.Millisecond
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		resp, err := b.Client.Post(u, "application/octet-stream", bytes.NewReader(payload))
		if err != nil {
			lastErr = err
		} else {
			if resp.StatusCode == 200 {
				resp.Body.Close()
				return part, nil
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("produce %s p=%d: %d %s", topic, part, resp.StatusCode, body)
			// 4xx client errors shouldn't be retried — fail fast.
			if resp.StatusCode >= 400 && resp.StatusCode < 500 {
				return part, lastErr
			}
		}
		time.Sleep(backoff)
		backoff *= 2
	}
	return part, lastErr
}
