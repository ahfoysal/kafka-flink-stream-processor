// processor: minimal "stream processor" DAG.
// Reads integers from --in topic as group --group, doubles them, writes
// to --out topic. Demonstrates the (source -> map -> sink) pattern that a
// real Flink-style engine generalizes.
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type consumeRecord struct {
	Offset  uint64 `json:"offset"`
	Payload string `json:"payload"`
}
type consumeResp struct {
	Records []consumeRecord `json:"records"`
	Next    uint64          `json:"next"`
}

func main() {
	addr := flag.String("broker", "http://127.0.0.1:9092", "broker URL")
	inTopic := flag.String("in", "numbers", "input topic")
	outTopic := flag.String("out", "doubled", "output topic")
	group := flag.String("group", "doubler", "consumer group for the input")
	idleMs := flag.Int("idle-ms", 1500, "stop after this many ms with no new input")
	flag.Parse()

	base, err := url.Parse(*addr)
	if err != nil {
		log.Fatalf("bad broker url: %v", err)
	}

	var (
		processed int
		idleSince time.Time
	)
	for {
		u := *base
		u.Path = "/consume"
		q := u.Query()
		q.Set("topic", *inTopic)
		q.Set("group", *group)
		q.Set("max", "100")
		u.RawQuery = q.Encode()

		resp, err := http.Get(u.String())
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
		if len(cr.Records) == 0 {
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
		for _, r := range cr.Records {
			p, err := base64.StdEncoding.DecodeString(r.Payload)
			if err != nil {
				log.Fatalf("payload decode: %v", err)
			}
			n, err := strconv.ParseInt(string(p), 10, 64)
			if err != nil {
				log.Printf("skip non-integer payload off=%d: %q", r.Offset, string(p))
				continue
			}
			doubled := []byte(strconv.FormatInt(n*2, 10))
			if err := produce(base, *outTopic, doubled); err != nil {
				log.Fatalf("produce: %v", err)
			}
			processed++
		}
	}
	fmt.Printf("processor done: processed=%d in=%q -> out=%q\n", processed, *inTopic, *outTopic)
}

func produce(base *url.URL, topic string, payload []byte) error {
	u := *base
	u.Path = "/produce"
	q := u.Query()
	q.Set("topic", topic)
	u.RawQuery = q.Encode()
	resp, err := http.Post(u.String(), "application/octet-stream", bytes.NewReader(payload))
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
