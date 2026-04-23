// Package log implements a simple append-only, length-prefixed segment log.
//
// On-disk layout per topic:
//
//	<dataDir>/<topic>/000000000000.log   (active segment; rolled when > maxSegmentBytes)
//	<dataDir>/<topic>/000000000123.log   (older segment; first record has offset 123)
//
// Record framing (big-endian):
//
//	[ 8 bytes offset ][ 4 bytes payload length ][ payload bytes ]
//
// Offsets are monotonic per topic and start at 0. Consumer group offsets are
// stored as plain JSON in <dataDir>/_offsets/<topic>__<group>.json.
package log

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	recordHeaderSize  = 12 // 8 offset + 4 length
	defaultSegmentMax = 64 * 1024 * 1024
)

// Record is a single log entry returned to consumers.
type Record struct {
	Offset  uint64 `json:"offset"`
	Payload []byte `json:"payload"`
}

// Topic is an append-only log for a single topic name.
type Topic struct {
	mu             sync.Mutex
	name           string
	dir            string
	maxSegmentSize int64

	// segments sorted by base offset ascending; last element is the active one.
	segments  []*segment
	nextOff   uint64
}

type segment struct {
	baseOffset uint64
	path       string
	file       *os.File
	size       int64
}

// OpenTopic opens or creates the topic directory and recovers any existing
// segments. nextOffset is derived by scanning the last segment.
func OpenTopic(dataDir, name string) (*Topic, error) {
	dir := filepath.Join(dataDir, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	t := &Topic{name: name, dir: dir, maxSegmentSize: defaultSegmentMax}
	if err := t.loadSegments(); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *Topic) loadSegments() error {
	entries, err := os.ReadDir(t.dir)
	if err != nil {
		return err
	}
	var bases []uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		baseStr := strings.TrimSuffix(e.Name(), ".log")
		base, err := strconv.ParseUint(baseStr, 10, 64)
		if err != nil {
			continue
		}
		bases = append(bases, base)
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })

	if len(bases) == 0 {
		// create first segment at offset 0
		seg, err := createSegment(t.dir, 0)
		if err != nil {
			return err
		}
		t.segments = []*segment{seg}
		t.nextOff = 0
		return nil
	}

	for _, b := range bases {
		seg, err := openSegment(t.dir, b)
		if err != nil {
			return err
		}
		t.segments = append(t.segments, seg)
	}
	// derive nextOff by scanning the last segment
	last := t.segments[len(t.segments)-1]
	nextOff, err := scanLastOffset(last)
	if err != nil {
		return err
	}
	t.nextOff = nextOff
	return nil
}

func segmentPath(dir string, baseOffset uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%012d.log", baseOffset))
}

func createSegment(dir string, baseOffset uint64) (*segment, error) {
	p := segmentPath(dir, baseOffset)
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &segment{baseOffset: baseOffset, path: p, file: f, size: fi.Size()}, nil
}

func openSegment(dir string, baseOffset uint64) (*segment, error) {
	return createSegment(dir, baseOffset)
}

// scanLastOffset walks the segment to find the offset of the last record and
// returns nextOffset = lastOffset + 1 (or baseOffset if empty).
func scanLastOffset(seg *segment) (uint64, error) {
	f, err := os.Open(seg.path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var (
		hdr      [recordHeaderSize]byte
		lastOff  uint64
		sawAny   bool
		pos      int64
	)
	for {
		n, err := io.ReadFull(f, hdr[:])
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// truncated header: stop; in a real system we'd truncate here.
			break
		}
		_ = n
		off := binary.BigEndian.Uint64(hdr[0:8])
		plen := binary.BigEndian.Uint32(hdr[8:12])
		if _, err := f.Seek(int64(plen), io.SeekCurrent); err != nil {
			break
		}
		lastOff = off
		sawAny = true
		pos += int64(recordHeaderSize) + int64(plen)
	}
	if !sawAny {
		return seg.baseOffset, nil
	}
	return lastOff + 1, nil
}

// Append writes a single record and returns its offset.
func (t *Topic) Append(payload []byte) (uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	active := t.segments[len(t.segments)-1]
	// Roll if the active segment is too big.
	if active.size >= t.maxSegmentSize {
		seg, err := createSegment(t.dir, t.nextOff)
		if err != nil {
			return 0, err
		}
		t.segments = append(t.segments, seg)
		active = seg
	}

	off := t.nextOff
	var hdr [recordHeaderSize]byte
	binary.BigEndian.PutUint64(hdr[0:8], off)
	binary.BigEndian.PutUint32(hdr[8:12], uint32(len(payload)))

	if _, err := active.file.Write(hdr[:]); err != nil {
		return 0, err
	}
	if _, err := active.file.Write(payload); err != nil {
		return 0, err
	}
	if err := active.file.Sync(); err != nil {
		return 0, err
	}
	active.size += int64(recordHeaderSize + len(payload))
	t.nextOff++
	return off, nil
}

// NextOffset returns the offset that will be assigned to the next appended record.
func (t *Topic) NextOffset() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.nextOff
}

// AppendAt writes a record with an explicit, caller-chosen offset. It is used
// by followers during replication: the leader picks the offset, followers
// must store the record at that exact offset so every replica stays
// byte-identical.
//
// The offset must equal the topic's current NextOffset (no gaps, no rewrites).
// On success, nextOffset advances by one.
func (t *Topic) AppendAt(offset uint64, payload []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if offset != t.nextOff {
		// Idempotency: silently accept a re-send of an offset already stored.
		if offset < t.nextOff {
			return nil
		}
		return fmt.Errorf("replication gap: got offset %d, want %d", offset, t.nextOff)
	}

	active := t.segments[len(t.segments)-1]
	if active.size >= t.maxSegmentSize {
		seg, err := createSegment(t.dir, t.nextOff)
		if err != nil {
			return err
		}
		t.segments = append(t.segments, seg)
		active = seg
	}

	var hdr [recordHeaderSize]byte
	binary.BigEndian.PutUint64(hdr[0:8], offset)
	binary.BigEndian.PutUint32(hdr[8:12], uint32(len(payload)))
	if _, err := active.file.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := active.file.Write(payload); err != nil {
		return err
	}
	if err := active.file.Sync(); err != nil {
		return err
	}
	active.size += int64(recordHeaderSize + len(payload))
	t.nextOff++
	return nil
}

// Read reads up to maxRecords records starting at fromOffset (inclusive).
// Returns the records and the next offset to read from.
func (t *Topic) Read(fromOffset uint64, maxRecords int) ([]Record, uint64, error) {
	t.mu.Lock()
	segs := append([]*segment(nil), t.segments...)
	nextOff := t.nextOff
	t.mu.Unlock()

	if fromOffset >= nextOff {
		return nil, fromOffset, nil
	}
	// find segment whose baseOffset <= fromOffset < nextBase
	segIdx := 0
	for i, s := range segs {
		if s.baseOffset <= fromOffset {
			segIdx = i
		} else {
			break
		}
	}

	var out []Record
	cur := fromOffset
	for segIdx < len(segs) && len(out) < maxRecords {
		seg := segs[segIdx]
		f, err := os.Open(seg.path)
		if err != nil {
			return nil, cur, err
		}
		var hdr [recordHeaderSize]byte
		for len(out) < maxRecords {
			if _, err := io.ReadFull(f, hdr[:]); err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					break
				}
				f.Close()
				return nil, cur, err
			}
			off := binary.BigEndian.Uint64(hdr[0:8])
			plen := binary.BigEndian.Uint32(hdr[8:12])
			payload := make([]byte, plen)
			if _, err := io.ReadFull(f, payload); err != nil {
				f.Close()
				return nil, cur, err
			}
			if off < fromOffset {
				continue
			}
			out = append(out, Record{Offset: off, Payload: payload})
			cur = off + 1
		}
		f.Close()
		segIdx++
	}
	return out, cur, nil
}

// Close flushes and closes all segment files.
func (t *Topic) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	var firstErr error
	for _, s := range t.segments {
		if err := s.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// --- consumer group offsets -------------------------------------------------

// OffsetStore persists committed offsets per (topic, group) as JSON files.
type OffsetStore struct {
	mu  sync.Mutex
	dir string
}

func NewOffsetStore(dataDir string) (*OffsetStore, error) {
	dir := filepath.Join(dataDir, "_offsets")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &OffsetStore{dir: dir}, nil
}

type offsetFile struct {
	Topic  string `json:"topic"`
	Group  string `json:"group"`
	Offset uint64 `json:"offset"`
}

func (s *OffsetStore) path(topic, group string) string {
	safeTopic := strings.ReplaceAll(topic, string(filepath.Separator), "_")
	safeGroup := strings.ReplaceAll(group, string(filepath.Separator), "_")
	return filepath.Join(s.dir, fmt.Sprintf("%s__%s.json", safeTopic, safeGroup))
}

// Get returns the committed offset for (topic, group), or 0 if none.
func (s *OffsetStore) Get(topic, group string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := os.ReadFile(s.path(topic, group))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var of offsetFile
	if err := json.Unmarshal(b, &of); err != nil {
		return 0, err
	}
	return of.Offset, nil
}

// Commit atomically writes the new offset for (topic, group).
func (s *OffsetStore) Commit(topic, group string, offset uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	of := offsetFile{Topic: topic, Group: group, Offset: offset}
	b, err := json.Marshal(of)
	if err != nil {
		return err
	}
	tmp := s.path(topic, group) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path(topic, group))
}
