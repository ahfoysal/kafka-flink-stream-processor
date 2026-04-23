#!/usr/bin/env bash
# M2 end-to-end demo:
#   1. start a 3-broker cluster (ids 0,1,2 on ports 19092/19093/19094)
#   2. create topic "events" with 4 partitions, replication factor 3
#   3. produce 1000 keyed messages from any broker (producer hash-routes)
#   4. run 2 consumers in the same group (g1) concurrently
#      - expect the 1000 records split ~500/500 across members
#      - verify no duplicates, no missing
#   5. verify every follower replica has the same byte count as its leader (ISR)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/mvp"

DATA="$ROOT/data_m2"
rm -rf "$DATA"
mkdir -p "$DATA"
mkdir -p "$ROOT/bin"

echo ">>> building binaries"
go build -o "$ROOT/bin/broker"    ./cmd/broker
go build -o "$ROOT/bin/producer"  ./cmd/producer
go build -o "$ROOT/bin/consumer"  ./cmd/consumer

P0=19092; P1=19093; P2=19094
PEERS="0=http://127.0.0.1:$P0,1=http://127.0.0.1:$P1,2=http://127.0.0.1:$P2"

start_broker() {
  local id=$1; local port=$2
  "$ROOT/bin/broker" -id "$id" -addr ":$port" -data "$DATA" -peers "$PEERS" \
    >"$DATA/broker-$id.log" 2>&1 &
  echo $!
}

echo ">>> starting 3-broker cluster"
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
echo "    brokers up: b0=$P0 b1=$P1 b2=$P2"

echo ">>> creating topic=events partitions=4 replication=3 (via b0)"
curl -fsS -X POST "http://127.0.0.1:$P0/admin/create_topic?topic=events&partitions=4&replication=3" \
  | sed 's/^/    /'

echo
echo ">>> metadata (as seen by b2 — confirms all brokers agree)"
curl -fsS "http://127.0.0.1:$P2/metadata?topic=events" | sed 's/^/    /'

echo
echo ">>> producing 1000 keyed messages via b1"
"$ROOT/bin/producer" \
  -broker "http://127.0.0.1:$P1" \
  -topic events -count 1000 -keyed -prefix ev \
  | sed 's/^/    /'

echo
echo ">>> consuming with 2-member group g1 (member 0 and member 1 concurrently)"
C0_OUT="$DATA/consumer0.out"
C1_OUT="$DATA/consumer1.out"
"$ROOT/bin/consumer" \
  -broker "http://127.0.0.1:$P0" \
  -topic events -group g1 \
  -members 2 -member-id 0 -tag m0 \
  -expect 0 > "$C0_OUT" 2>&1 &
CPID0=$!
"$ROOT/bin/consumer" \
  -broker "http://127.0.0.1:$P2" \
  -topic events -group g1 \
  -members 2 -member-id 1 -tag m1 \
  -expect 0 > "$C1_OUT" 2>&1 &
CPID1=$!
wait $CPID0 $CPID1
sed 's/^/    /' "$C0_OUT"
sed 's/^/    /' "$C1_OUT"

N0=$(grep -Eo 'consumed [0-9]+' "$C0_OUT" | awk '{print $2}' | tail -1)
N1=$(grep -Eo 'consumed [0-9]+' "$C1_OUT" | awk '{print $2}' | tail -1)
TOTAL=$(( N0 + N1 ))
echo
echo ">>> tally: m0=$N0  m1=$N1  total=$TOTAL"
if [[ "$TOTAL" != "1000" ]]; then
  echo "FAIL: expected 1000 total, got $TOTAL"; exit 1
fi
# each member should get a non-trivial share (at least one partition's worth ~= 250).
if [[ "$N0" -lt 100 || "$N1" -lt 100 ]]; then
  echo "FAIL: uneven distribution m0=$N0 m1=$N1"; exit 1
fi

echo
echo ">>> verifying replicas: segment sizes should match across b0/b1/b2 per partition"
PASS=1
for p in 0 1 2 3; do
  SIZES=()
  for b in 0 1 2; do
    f="$DATA/b$b/events/p$p/000000000000.log"
    if [[ -f "$f" ]]; then
      SZ=$(wc -c < "$f" | tr -d ' ')
    else
      SZ="missing"
    fi
    SIZES+=("b$b=$SZ")
  done
  echo "    partition $p: ${SIZES[*]}"
  # all three should be equal numbers
  UNIQ=$(printf "%s\n" "${SIZES[@]}" | awk -F= '{print $2}' | sort -u | wc -l | tr -d ' ')
  if [[ "$UNIQ" != "1" ]]; then
    echo "    FAIL: replicas diverge for partition $p"
    PASS=0
  fi
done
if [[ "$PASS" != "1" ]]; then
  exit 1
fi

echo
echo ">>> DEMO PASSED"
