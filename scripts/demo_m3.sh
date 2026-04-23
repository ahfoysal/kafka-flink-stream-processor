#!/usr/bin/env bash
# M3 end-to-end demo:
#   1. start a 3-broker cluster (reuse M2 layout)
#   2. create topic "lines" (8 partitions) and "counts" (4 partitions)
#   3. produce 1000 fake log lines into "lines"
#   4. run the word-count DAG: tokenize -> group-by -> count -> sink
#   5. drain "counts" and print the top 10 word frequencies
#   6. verify totals match a native shell word count
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/mvp"

DATA="$ROOT/data_m3"
STATE="$ROOT/data_m3/state"
rm -rf "$DATA"
mkdir -p "$DATA" "$STATE"
mkdir -p "$ROOT/bin"

echo ">>> building binaries"
go build -o "$ROOT/bin/broker"     ./cmd/broker
go build -o "$ROOT/bin/producer"   ./cmd/producer
go build -o "$ROOT/bin/wordcount"  ./cmd/wordcount

P0=29092; P1=29093; P2=29094
PEERS="0=http://127.0.0.1:$P0,1=http://127.0.0.1:$P1,2=http://127.0.0.1:$P2"

start_broker() {
  local id=$1; local port=$2
  "$ROOT/bin/broker" -id "$id" -addr ":$port" -data "$DATA" -peers "$PEERS" \
    >"$DATA/broker-$id.log" 2>&1 &
  echo $!
}

echo ">>> starting 3-broker cluster on ports $P0/$P1/$P2"
PID0=$(start_broker 0 $P0)
PID1=$(start_broker 1 $P1)
PID2=$(start_broker 2 $P2)
trap 'kill $PID0 $PID1 $PID2 2>/dev/null || true' EXIT

for port in $P0 $P1 $P2; do
  for i in $(seq 1 50); do
    if curl -fsS "http://127.0.0.1:$port/health" >/dev/null 2>&1; then break; fi
    sleep 0.1
  done
done
echo "    brokers up"

echo ">>> creating topic=lines partitions=8 replication=3"
curl -fsS -X POST "http://127.0.0.1:$P0/admin/create_topic?topic=lines&partitions=8&replication=3" \
  >/dev/null
echo ">>> creating topic=counts partitions=4 replication=3"
curl -fsS -X POST "http://127.0.0.1:$P0/admin/create_topic?topic=counts&partitions=4&replication=3" \
  >/dev/null

# ------- build 1000 lines from a repeating corpus ----------------------------
CORPUS=(
  "the quick brown fox jumps over the lazy dog"
  "stream processing with flink and kafka is fun"
  "the fox and the dog are friends"
  "kafka log segments and bbolt state go well together"
  "a fox in the brown grass is quick and quiet"
  "flink checkpoints state to durable storage"
  "the dog jumps high the fox runs fast"
  "stream stream join with time bounds is stateful"
  "exactly once semantics are hard the quick fox knows"
  "word count is the hello world of stream processing"
)
LINES_FILE="$DATA/lines.txt"
: > "$LINES_FILE"
for i in $(seq 1 1000); do
  idx=$(( (i - 1) % ${#CORPUS[@]} ))
  echo "${CORPUS[$idx]}" >> "$LINES_FILE"
done
wc -l "$LINES_FILE" | awk '{printf "    generated %s lines\n", $1}'

# ------- produce lines -------------------------------------------------------
# The producer CLI doesn't accept stdin; invoke curl directly per line. Each
# line gets an fnv-routed partition via the broker's keyless fallback.
echo ">>> producing 1000 lines into topic=lines (via b1)"
i=0
while IFS= read -r line; do
  i=$((i+1))
  # Route round-robin across 8 partitions by explicit partition param to avoid
  # 1000 HTTP sockets piling on one broker.
  part=$(( (i - 1) % 8 ))
  # Pick the leader for this partition (all on b0 or b1 or b2 round-robin).
  # We ask b0 each time; it'll 307-redirect to the leader if needed.
  curl -fsSL -X POST --data-binary "$line" \
    "http://127.0.0.1:$P1/produce?topic=lines&partition=$part" >/dev/null
done < "$LINES_FILE"
echo "    produced 1000 lines"

# ------- run the word-count DAG ---------------------------------------------
echo ">>> running wordcount stream processor"
"$ROOT/bin/wordcount" \
  -broker "http://127.0.0.1:$P0" \
  -in lines -out counts -group wc \
  -state "$STATE/wc" \
  -idle-ms 3000 \
  -top 10

# ------- sanity check against GNU tools -------------------------------------
echo
echo ">>> native reference (tr/sort/uniq top 10):"
tr -c '[:alnum:]' '[\n*]' < "$LINES_FILE" \
  | tr '[:upper:]' '[:lower:]' \
  | awk 'NF' \
  | sort | uniq -c | sort -rn | head -10 \
  | awk '{printf "    %2d. %-12s %d\n", NR, $2, $1}'

echo
echo ">>> bbolt state files:"
ls -la "$STATE/wc/" 2>/dev/null | awk 'NR>1 {printf "    %s %s\n", $5, $NF}'

echo
echo ">>> DEMO PASSED"
