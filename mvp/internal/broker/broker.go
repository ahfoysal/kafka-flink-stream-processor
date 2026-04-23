// Package broker is the M2 multi-broker HTTP broker.
//
// Endpoints:
//
//	POST /admin/create_topic?topic=X&partitions=N[&replication=R]
//	        pick this broker as controller; build assignment; fan out to peers.
//	POST /admin/assign                                    (peer -> peer)
//	        body = TopicMetadata JSON; store locally and open the partitions
//	        this broker is responsible for.
//	GET  /metadata[?topic=X]                              -> {topics:[TopicMetadata,...], brokers:{id:url}}
//
//	POST /produce?topic=X[&key=K]                         client-facing
//	        partition = hash(key) % N (or round-robin if no key). If this broker
//	        is not the leader, returns 307 with Location: <leader>/produce...
//	        Otherwise append locally, replicate to all followers synchronously,
//	        return {partition, offset} only once every ISR member has acked.
//
//	POST /replicate?topic=X&partition=P&offset=O          (leader -> follower)
//	        body = raw payload. Follower writes at exact offset.
//
//	GET  /consume?topic=X&partition=P&group=G&max=N       partition-scoped read
//	        offsets committed as (topic, group, partition) triplets.
//	GET  /offset?topic=X&partition=P&group=G              inspect committed offset
//	GET  /health                                          -> ok
//
// This is deliberately simple: static ISR (leader + all followers must ack),
// no failure detection, no leader election, no metadata persistence across
// restarts. M3+ replaces these.
package broker

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	stdlog "log"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	mvplog "github.com/ahfoysal/kafka-flink-stream-processor/mvp/internal/log"
)

// Broker is one node in the cluster.
type Broker struct {
	dataDir string
	id      int
	peers   map[int]string // id -> url, includes self

	mu sync.Mutex
	// partitions keyed by (topic, partition) -> local log replica, for every
	// partition (leader or follower) this broker owns.
	partitions map[partKey]*mvplog.Topic
	offs       *mvplog.OffsetStore

	meta   *metaStore
	client *http.Client

	// global round-robin counter for keyless produces
	rrCounter atomic.Uint64
}

type partKey struct {
	topic     string
	partition int
}

// Config drives New().
type Config struct {
	DataDir string
	ID      int
	Peers   map[int]string // id -> base URL (must include self)
}

// New returns an M2 broker.
func New(cfg Config) (*Broker, error) {
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("DataDir required")
	}
	if _, ok := cfg.Peers[cfg.ID]; !ok {
		return nil, fmt.Errorf("peer map missing self id=%d", cfg.ID)
	}
	// Per-broker on-disk root so multiple brokers can share one -data parent.
	brokerRoot := filepath.Join(cfg.DataDir, fmt.Sprintf("b%d", cfg.ID))
	offs, err := mvplog.NewOffsetStore(brokerRoot)
	if err != nil {
		return nil, err
	}
	return &Broker{
		dataDir:    brokerRoot,
		id:         cfg.ID,
		peers:      cfg.Peers,
		partitions: make(map[partKey]*mvplog.Topic),
		offs:       offs,
		meta:       newMetaStore(),
		client:     &http.Client{Timeout: 10 * time.Second},
	}, nil
}

// partitionDir returns the per-replica data dir for a (topic,partition).
func (b *Broker) partitionLog(topic string, partition int) (*mvplog.Topic, error) {
	k := partKey{topic, partition}
	b.mu.Lock()
	defer b.mu.Unlock()
	if p, ok := b.partitions[k]; ok {
		return p, nil
	}
	// data dir: <brokerRoot>/<topic>/p<N>
	// OpenTopic(dir, name) creates <dir>/<name>/ -> pass topicDir, fmt("p%d",N)
	topicDir := filepath.Join(b.dataDir, topic)
	p, err := mvplog.OpenTopic(topicDir, fmt.Sprintf("p%d", partition))
	if err != nil {
		return nil, err
	}
	b.partitions[k] = p
	return p, nil
}

// Handler returns the HTTP handler.
func (b *Broker) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok\n") })
	mux.HandleFunc("/admin/create_topic", b.handleCreateTopic)
	mux.HandleFunc("/admin/assign", b.handleAssign)
	mux.HandleFunc("/metadata", b.handleMetadata)
	mux.HandleFunc("/produce", b.handleProduce)
	mux.HandleFunc("/replicate", b.handleReplicate)
	mux.HandleFunc("/consume", b.handleConsume)
	mux.HandleFunc("/offset", b.handleOffset)
	return mux
}

// Close shuts down every owned partition log.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var firstErr error
	for _, p := range b.partitions {
		if err := p.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Listen serves on addr.
func (b *Broker) Listen(addr string) error {
	stdlog.Printf("broker id=%d listening on %s (data=%s, peers=%v)", b.id, addr, b.dataDir, b.peers)
	s := &http.Server{Addr: addr, Handler: b.Handler()}
	return s.ListenAndServe()
}

// --- admin -----------------------------------------------------------------

func (b *Broker) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "missing topic", http.StatusBadRequest)
		return
	}
	nStr := r.URL.Query().Get("partitions")
	n, err := strconv.Atoi(nStr)
	if err != nil || n <= 0 {
		http.Error(w, "partitions must be positive int", http.StatusBadRequest)
		return
	}
	rf := 3
	if s := r.URL.Query().Get("replication"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			rf = v
		}
	}
	if rf > len(b.peers) {
		rf = len(b.peers)
	}

	brokerIDs := make([]int, 0, len(b.peers))
	for id := range b.peers {
		brokerIDs = append(brokerIDs, id)
	}
	meta, err := AssignPartitions(topic, n, rf, brokerIDs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Fan out the assignment to every broker (including self) so each opens
	// its replicas.
	body, _ := encodeJSON(meta)
	for id, url := range b.peers {
		if id == b.id {
			if err := b.applyAssignment(meta); err != nil {
				http.Error(w, fmt.Sprintf("self apply: %v", err), http.StatusInternalServerError)
				return
			}
			continue
		}
		req, _ := http.NewRequest(http.MethodPost, url+"/admin/assign", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := b.client.Do(req)
		if err != nil {
			http.Error(w, fmt.Sprintf("peer %d %s: %v", id, url, err), http.StatusBadGateway)
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			http.Error(w, fmt.Sprintf("peer %d status %d", id, resp.StatusCode), http.StatusBadGateway)
			return
		}
	}
	writeJSON(w, meta)
}

func (b *Broker) handleAssign(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var meta TopicMetadata
	if err := json.NewDecoder(r.Body).Decode(&meta); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := b.applyAssignment(meta); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

// applyAssignment stores metadata and pre-opens every partition replica this
// broker owns so the first produce doesn't race.
func (b *Broker) applyAssignment(meta TopicMetadata) error {
	b.meta.put(meta)
	for _, p := range meta.Partitions {
		owned := false
		for _, rid := range p.ISR() {
			if rid == b.id {
				owned = true
				break
			}
		}
		if !owned {
			continue
		}
		if _, err := b.partitionLog(meta.Topic, p.Partition); err != nil {
			return err
		}
	}
	return nil
}

func (b *Broker) handleMetadata(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic != "" {
		m, ok := b.meta.get(topic)
		if !ok {
			http.Error(w, "unknown topic", http.StatusNotFound)
			return
		}
		writeJSON(w, map[string]any{"topics": []TopicMetadata{m}, "brokers": b.peers})
		return
	}
	writeJSON(w, map[string]any{"topics": b.meta.all(), "brokers": b.peers})
}

// --- produce ---------------------------------------------------------------

func (b *Broker) handleProduce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "missing topic", http.StatusBadRequest)
		return
	}
	key := []byte(r.URL.Query().Get("key"))

	meta, ok := b.meta.get(topic)
	if !ok {
		http.Error(w, "unknown topic; create via /admin/create_topic", http.StatusNotFound)
		return
	}

	// Allow clients to pin the partition (the producer does this after its own
	// hashing so the wire format is explicit and debuggable).
	var partID int
	if pStr := r.URL.Query().Get("partition"); pStr != "" {
		v, err := strconv.Atoi(pStr)
		if err != nil || v < 0 || v >= len(meta.Partitions) {
			http.Error(w, "bad partition", http.StatusBadRequest)
			return
		}
		partID = v
	} else {
		partID = PartitionForKey(key, len(meta.Partitions), b.rrCounter.Add(1)-1)
	}
	pa := meta.Partitions[partID]

	// Not leader -> tell the client where to go.
	if pa.Leader != b.id {
		leaderURL := b.peers[pa.Leader]
		q := r.URL.Query()
		q.Set("partition", strconv.Itoa(partID))
		http.Redirect(w, r, leaderURL+"/produce?"+q.Encode(), http.StatusTemporaryRedirect)
		return
	}

	payload, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	log, err := b.partitionLog(topic, partID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	off, err := log.Append(payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Synchronous replication to every follower (static ISR).
	for _, fid := range pa.Followers {
		url := b.peers[fid]
		req, _ := http.NewRequest(http.MethodPost,
			fmt.Sprintf("%s/replicate?topic=%s&partition=%d&offset=%d", url, topic, partID, off),
			bytes.NewReader(payload))
		resp, err := b.client.Do(req)
		if err != nil {
			http.Error(w, fmt.Sprintf("replicate to %d: %v", fid, err), http.StatusBadGateway)
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			http.Error(w, fmt.Sprintf("replicate to %d: status %d", fid, resp.StatusCode), http.StatusBadGateway)
			return
		}
	}
	writeJSON(w, map[string]any{"partition": partID, "offset": off, "leader": b.id})
}

func (b *Broker) handleReplicate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	topic := r.URL.Query().Get("topic")
	pStr := r.URL.Query().Get("partition")
	oStr := r.URL.Query().Get("offset")
	if topic == "" || pStr == "" || oStr == "" {
		http.Error(w, "missing topic/partition/offset", http.StatusBadRequest)
		return
	}
	partID, err := strconv.Atoi(pStr)
	if err != nil {
		http.Error(w, "bad partition", http.StatusBadRequest)
		return
	}
	off, err := strconv.ParseUint(oStr, 10, 64)
	if err != nil {
		http.Error(w, "bad offset", http.StatusBadRequest)
		return
	}
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	log, err := b.partitionLog(topic, partID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := log.AppendAt(off, payload); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"ok": true, "follower": b.id, "offset": off})
}

// --- consume ---------------------------------------------------------------

type consumeRecord struct {
	Offset    uint64 `json:"offset"`
	Partition int    `json:"partition"`
	Payload   string `json:"payload"` // base64
}

func (b *Broker) handleConsume(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	group := r.URL.Query().Get("group")
	pStr := r.URL.Query().Get("partition")
	if topic == "" || group == "" || pStr == "" {
		http.Error(w, "missing topic/group/partition", http.StatusBadRequest)
		return
	}
	partID, err := strconv.Atoi(pStr)
	if err != nil {
		http.Error(w, "bad partition", http.StatusBadRequest)
		return
	}
	maxStr := r.URL.Query().Get("max")
	maxRecords := 100
	if maxStr != "" {
		if n, err := strconv.Atoi(maxStr); err == nil && n > 0 {
			maxRecords = n
		}
	}

	// Redirect to leader if we aren't one (consumers only read from leaders).
	if meta, ok := b.meta.get(topic); ok {
		if partID < 0 || partID >= len(meta.Partitions) {
			http.Error(w, "bad partition", http.StatusBadRequest)
			return
		}
		if meta.Partitions[partID].Leader != b.id {
			leaderURL := b.peers[meta.Partitions[partID].Leader]
			http.Redirect(w, r, leaderURL+"/consume?"+r.URL.Query().Encode(), http.StatusTemporaryRedirect)
			return
		}
	}

	log, err := b.partitionLog(topic, partID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	offKey := fmt.Sprintf("%s__p%d", topic, partID)
	start, err := b.offs.Get(offKey, group)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	recs, next, err := log.Read(start, maxRecords)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if next > start {
		if err := b.offs.Commit(offKey, group, next); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	out := make([]consumeRecord, 0, len(recs))
	for _, rec := range recs {
		out = append(out, consumeRecord{
			Offset:    rec.Offset,
			Partition: partID,
			Payload:   base64.StdEncoding.EncodeToString(rec.Payload),
		})
	}
	writeJSON(w, map[string]any{"records": out, "next": next, "partition": partID})
}

func (b *Broker) handleOffset(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	group := r.URL.Query().Get("group")
	pStr := r.URL.Query().Get("partition")
	if topic == "" || group == "" || pStr == "" {
		http.Error(w, "missing topic/group/partition", http.StatusBadRequest)
		return
	}
	partID, err := strconv.Atoi(pStr)
	if err != nil {
		http.Error(w, "bad partition", http.StatusBadRequest)
		return
	}
	offKey := fmt.Sprintf("%s__p%d", topic, partID)
	off, err := b.offs.Get(offKey, group)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"offset": off, "partition": partID})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		stdlog.Printf("writeJSON: %v", err)
	}
}

// SanityError helps main surface config problems.
func SanityError(msg string) error { return fmt.Errorf("broker: %s", msg) }
