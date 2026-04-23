// consumer: polls /consume for a group until expected messages are drained
// or a quiescence window passes, then prints the list.
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
	topic := flag.String("topic", "demo", "topic")
	group := flag.String("group", "g1", "consumer group")
	expect := flag.Int("expect", 0, "expected record count (0 = drain until idle)")
	printBodies := flag.Bool("print", false, "print each payload body")
	flag.Parse()

	u, err := url.Parse(*addr)
	if err != nil {
		log.Fatalf("bad broker url: %v", err)
	}
	u.Path = "/consume"

	var (
		total     int
		lastOff   int64 = -1
		idleSince time.Time
	)
	for {
		q := u.Query()
		q.Set("topic", *topic)
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
			// exit when we've either reached expected count or stayed idle.
			if *expect > 0 && total >= *expect {
				break
			}
			if time.Since(idleSince) > 1500*time.Millisecond {
				break
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		idleSince = time.Time{}
		for _, r := range cr.Records {
			if int64(r.Offset) != lastOff+1 {
				log.Fatalf("ORDER VIOLATION: got offset %d after %d", r.Offset, lastOff)
			}
			lastOff = int64(r.Offset)
			total++
			if *printBodies {
				p, err := base64.StdEncoding.DecodeString(r.Payload)
				if err != nil {
					log.Fatalf("payload decode: %v", err)
				}
				fmt.Printf("  off=%d payload=%s\n", r.Offset, string(p))
			}
		}
		if *expect > 0 && total >= *expect {
			break
		}
	}
	fmt.Printf("consumed %d records from topic=%q group=%q (last offset=%d)\n",
		total, *topic, *group, lastOff)
}
