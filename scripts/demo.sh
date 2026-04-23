#!/usr/bin/env bash
# End-to-end MVP demo:
#   1. start broker (fresh data dir)
#   2. produce 100 messages to topic "demo"
#   3. consume with group "g1" and verify count + order
#   4. produce integers 1..10 to "numbers"
#   5. run the doubling processor -> topic "doubled"
#   6. verify "doubled" values are 2..20 in order
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/mvp"

DATA="$ROOT/data"
rm -rf "$DATA"
mkdir -p "$DATA"

echo ">>> building binaries"
go build -o "$ROOT/bin/broker"    ./cmd/broker
go build -o "$ROOT/bin/producer"  ./cmd/producer
go build -o "$ROOT/bin/consumer"  ./cmd/consumer
go build -o "$ROOT/bin/processor" ./cmd/processor

PORT=19092
BROKER_URL="http://127.0.0.1:$PORT"

echo ">>> starting broker on $PORT"
"$ROOT/bin/broker" -addr ":$PORT" -data "$DATA" >"$DATA/broker.log" 2>&1 &
BROKER_PID=$!
trap 'kill $BROKER_PID 2>/dev/null || true' EXIT

# wait for /health
for i in $(seq 1 50); do
  if curl -fsS "$BROKER_URL/health" >/dev/null 2>&1; then break; fi
  sleep 0.1
done

echo ">>> producing 100 messages to topic=demo"
"$ROOT/bin/producer" -broker "$BROKER_URL" -topic demo -count 100

echo ">>> consuming with group=g1 (expect=100)"
CONSUME_OUT=$("$ROOT/bin/consumer" -broker "$BROKER_URL" -topic demo -group g1 -expect 100)
echo "$CONSUME_OUT"
COUNT=$(echo "$CONSUME_OUT" | grep -Eo 'consumed [0-9]+' | awk '{print $2}')
if [[ "$COUNT" != "100" ]]; then
  echo "FAIL: expected 100 records, got $COUNT"; exit 1
fi

echo ">>> verifying at-least-once: second consume with same group returns 0"
CONSUME_OUT2=$("$ROOT/bin/consumer" -broker "$BROKER_URL" -topic demo -group g1)
echo "$CONSUME_OUT2"
COUNT2=$(echo "$CONSUME_OUT2" | grep -Eo 'consumed [0-9]+' | awk '{print $2}')
if [[ "$COUNT2" != "0" ]]; then
  echo "FAIL: offsets not committed; got $COUNT2 instead of 0"; exit 1
fi

echo ">>> producing integers 1..10 to topic=numbers"
for i in $(seq 1 10); do
  curl -fsS -X POST --data-binary "$i" "$BROKER_URL/produce?topic=numbers" >/dev/null
done

echo ">>> running processor numbers -> doubled"
"$ROOT/bin/processor" -broker "$BROKER_URL" -in numbers -out doubled -group doubler

echo ">>> consuming 'doubled' and verifying values"
DOUBLED=$("$ROOT/bin/consumer" -broker "$BROKER_URL" -topic doubled -group verify -expect 10 -print)
echo "$DOUBLED"
EXPECTED="2 4 6 8 10 12 14 16 18 20"
GOT=$(echo "$DOUBLED" | grep -Eo 'payload=[0-9]+' | cut -d= -f2 | tr '\n' ' ' | sed 's/ $//')
if [[ "$GOT" != "$EXPECTED" ]]; then
  echo "FAIL: doubled values mismatch. got=[$GOT] expected=[$EXPECTED]"; exit 1
fi

echo ">>> DEMO PASSED"
