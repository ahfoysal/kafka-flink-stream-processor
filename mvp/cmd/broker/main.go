package main

import (
	"flag"
	"log"

	"github.com/ahfoysal/kafka-flink-stream-processor/mvp/internal/broker"
)

func main() {
	addr := flag.String("addr", ":9092", "HTTP listen address")
	dataDir := flag.String("data", "./data", "data directory (each broker writes under <data>/b<id>/)")
	id := flag.Int("id", 0, "broker id (unique within cluster)")
	peers := flag.String("peers", "0=http://127.0.0.1:9092", "cluster peer list (id=url,...) including self")
	flag.Parse()

	peerMap, err := broker.ParsePeers(*peers)
	if err != nil {
		log.Fatalf("parse peers: %v", err)
	}
	b, err := broker.New(broker.Config{
		DataDir: *dataDir,
		ID:      *id,
		Peers:   peerMap,
	})
	if err != nil {
		log.Fatalf("broker init: %v", err)
	}
	defer b.Close()
	if err := b.Listen(*addr); err != nil {
		log.Fatalf("listen: %v", err)
	}
}
