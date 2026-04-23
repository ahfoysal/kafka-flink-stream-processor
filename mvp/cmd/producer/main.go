// producer: sends N messages of form "msg-<i>" to a topic.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
)

func main() {
	addr := flag.String("broker", "http://127.0.0.1:9092", "broker URL")
	topic := flag.String("topic", "demo", "topic name")
	count := flag.Int("count", 100, "number of messages")
	prefix := flag.String("prefix", "msg", "message body prefix")
	flag.Parse()

	u, err := url.Parse(*addr)
	if err != nil {
		log.Fatalf("bad broker url: %v", err)
	}
	u.Path = "/produce"
	q := u.Query()
	q.Set("topic", *topic)
	u.RawQuery = q.Encode()

	for i := 0; i < *count; i++ {
		body := fmt.Sprintf("%s-%d", *prefix, i)
		resp, err := http.Post(u.String(), "application/octet-stream", bytes.NewReader([]byte(body)))
		if err != nil {
			log.Fatalf("produce %d: %v", i, err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			log.Fatalf("produce %d: status=%d body=%s", i, resp.StatusCode, string(b))
		}
	}
	fmt.Printf("produced %d messages to topic %q\n", *count, *topic)
}
