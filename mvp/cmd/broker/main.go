package main

import (
	"flag"
	"log"

	"github.com/ahfoysal/kafka-flink-stream-processor/mvp/internal/broker"
)

func main() {
	addr := flag.String("addr", ":9092", "HTTP listen address")
	dataDir := flag.String("data", "./data", "data directory for log segments")
	flag.Parse()

	b, err := broker.New(*dataDir)
	if err != nil {
		log.Fatalf("broker init: %v", err)
	}
	defer b.Close()
	if err := b.Listen(*addr); err != nil {
		log.Fatalf("listen: %v", err)
	}
}
