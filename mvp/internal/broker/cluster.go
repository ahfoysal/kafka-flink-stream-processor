// Cluster metadata + partition assignment for the M2 multi-broker broker.
//
// Each broker is started with:
//   -id <int>          : unique broker id in the cluster
//   -peers <csv>       : id=url pairs, e.g. "0=http://127.0.0.1:19092,1=http://127.0.0.1:19093,2=..."
//                        (including this broker itself)
//
// Topics are created via POST /admin/create_topic?topic=X&partitions=N
// on *any* broker. That broker computes the assignment deterministically
// (round-robin leaders; 2 followers per partition) and broadcasts it to
// every peer via POST /admin/assign.
//
// The assignment + cluster map is held in memory (M2 is a single process-lifetime
// demo, no ZK/etcd yet). On restart the disk segments are still there, but the
// cluster must be recreated by re-running create_topic. A persistent metadata
// log is on the M3+ backlog.
package broker

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// PartitionAssignment is the ISR layout for one partition.
type PartitionAssignment struct {
	Partition int   `json:"partition"`
	Leader    int   `json:"leader"`
	Followers []int `json:"followers"` // replicas other than leader
}

// ISR returns leader + followers (static ISR in M2).
func (p PartitionAssignment) ISR() []int {
	out := make([]int, 0, 1+len(p.Followers))
	out = append(out, p.Leader)
	out = append(out, p.Followers...)
	return out
}

// TopicMetadata is the full partition map for a topic.
type TopicMetadata struct {
	Topic      string                `json:"topic"`
	Partitions []PartitionAssignment `json:"partitions"`
}

// PartitionForKey returns the partition id for a given message key.
// Empty key => round-robin via counter argument; callers pass the current
// counter value.
func PartitionForKey(key []byte, numPartitions int, rrCounter uint64) int {
	if numPartitions <= 0 {
		return 0
	}
	if len(key) == 0 {
		return int(rrCounter % uint64(numPartitions))
	}
	h := fnv.New32a()
	_, _ = h.Write(key)
	return int(h.Sum32() % uint32(numPartitions))
}

// ClusterConfig is the in-memory view of peers.
type ClusterConfig struct {
	SelfID int
	Peers  map[int]string // id -> base URL (e.g. http://127.0.0.1:19092)
}

// ParsePeers parses "0=url,1=url,..." into a map.
func ParsePeers(spec string) (map[int]string, error) {
	out := make(map[int]string)
	if strings.TrimSpace(spec) == "" {
		return out, nil
	}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		eq := strings.IndexByte(part, '=')
		if eq <= 0 {
			return nil, fmt.Errorf("bad peer %q (want id=url)", part)
		}
		id, err := strconv.Atoi(part[:eq])
		if err != nil {
			return nil, fmt.Errorf("bad peer id %q: %v", part[:eq], err)
		}
		url := strings.TrimRight(part[eq+1:], "/")
		out[id] = url
	}
	return out, nil
}

// AssignPartitions builds a deterministic assignment: partition i's leader is
// broker sortedIDs[i % N]; followers are the next replicationFactor-1 brokers
// in the sorted ring.
func AssignPartitions(topic string, numPartitions, replicationFactor int, brokerIDs []int) (TopicMetadata, error) {
	if numPartitions <= 0 {
		return TopicMetadata{}, fmt.Errorf("partitions must be > 0")
	}
	if replicationFactor <= 0 || replicationFactor > len(brokerIDs) {
		return TopicMetadata{}, fmt.Errorf("replication factor %d invalid for %d brokers", replicationFactor, len(brokerIDs))
	}
	ids := append([]int(nil), brokerIDs...)
	sort.Ints(ids)

	meta := TopicMetadata{Topic: topic}
	for p := 0; p < numPartitions; p++ {
		leaderIdx := p % len(ids)
		leader := ids[leaderIdx]
		var followers []int
		for f := 1; f < replicationFactor; f++ {
			followers = append(followers, ids[(leaderIdx+f)%len(ids)])
		}
		meta.Partitions = append(meta.Partitions, PartitionAssignment{
			Partition: p,
			Leader:    leader,
			Followers: followers,
		})
	}
	return meta, nil
}

// metaStore holds all known topic metadata on a broker.
type metaStore struct {
	mu     sync.RWMutex
	topics map[string]TopicMetadata
}

func newMetaStore() *metaStore {
	return &metaStore{topics: make(map[string]TopicMetadata)}
}

func (m *metaStore) put(meta TopicMetadata) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// copy to avoid aliasing
	c := TopicMetadata{Topic: meta.Topic, Partitions: append([]PartitionAssignment(nil), meta.Partitions...)}
	m.topics[meta.Topic] = c
}

func (m *metaStore) get(topic string) (TopicMetadata, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.topics[topic]
	return t, ok
}

func (m *metaStore) all() []TopicMetadata {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]TopicMetadata, 0, len(m.topics))
	for _, t := range m.topics {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Topic < out[j].Topic })
	return out
}

// encodeJSON is a small helper used by handlers.
func encodeJSON(v any) ([]byte, error) { return json.Marshal(v) }
