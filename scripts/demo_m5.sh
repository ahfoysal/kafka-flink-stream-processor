#!/usr/bin/env bash
# M5 end-to-end demo: exactly-once via 2PC checkpoints + streaming SQL frontend.
#
#   1. Start 3-broker cluster (reuse M2 layout).
#   2. Create topics: sql_in (4 partitions), sql_out (2 partitions).
#   3. Produce ~40 event-time-stamped records into sql_in.
#   4. Run the SQL job with --kill-after-records=15 — the job panics mid-run.
#      Nothing should appear in sql_out yet (or at most what the first barrier
#      committed before the crash), and pending outputs will sit staged in
#      the bbolt state files.
#   5. Restart the SQL job (no --kill flag). It replays pending outputs
#      (idempotent thanks to dedup), finishes the stream, and commits.
#   6. Drain sql_out and verify:
#       - expected (word, window) counts MATCH the oracle produced from the
#         raw input, AND
#       - no dedup_id appears more than once in sql_out (exactly-once).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/mvp"

DATA="$ROOT/data_m5"
STATE="$ROOT/data_m5/state"
rm -rf "$DATA"
mkdir -p "$DATA" "$STATE"
mkdir -p "$ROOT/bin"

echo ">>> building binaries"
go build -o "$ROOT/bin/broker"  ./cmd/broker
go build -o "$ROOT/bin/sql-job" ./cmd/sql-job

P0=49092; P1=49093; P2=49094
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

echo ">>> creating topic=sql_in  partitions=4 replication=3"
curl -fsS -X POST "http://127.0.0.1:$P0/admin/create_topic?topic=sql_in&partitions=4&replication=3"  >/dev/null
echo ">>> creating topic=sql_out partitions=2 replication=3"
curl -fsS -X POST "http://127.0.0.1:$P0/admin/create_topic?topic=sql_out&partitions=2&replication=3" >/dev/null

# ------- produce a deterministic event-time dataset --------------------------
# Three 5s windows, words "apple"/"banana"/"cherry" at various offsets.
# base aligned to 5s so window bounds are predictable.
NOW_MS=$(python3 -c 'import time; print(int(time.time()*1000))')
BASE_MS=$(( (NOW_MS / 5000) * 5000 ))
echo ">>> producing events (base_ms=$BASE_MS)"
produce() {
  # $1 = event-time (ms), $2 = word
  local payload="$1	$2"
  curl -fsSL -X POST --data-binary "$payload" \
    "http://127.0.0.1:$P0/produce?topic=sql_in" >/dev/null
}

# W1 [base, base+5s)
for i in 1 2 3 4 5; do produce $((BASE_MS +  500*i)) apple; done
for i in 1 2 3;       do produce $((BASE_MS +  700*i)) banana; done
# W2 [base+5s, base+10s)
for i in 1 2 3 4;     do produce $((BASE_MS + 5000 + 800*i)) apple;  done
for i in 1 2 3 4 5 6; do produce $((BASE_MS + 5000 + 400*i)) cherry; done
# W3 [base+10s, base+15s)
for i in 1 2;         do produce $((BASE_MS + 10000 + 800*i)) banana; done
for i in 1 2 3;       do produce $((BASE_MS + 10000 + 300*i)) cherry; done

TOTAL_EVENTS=23
echo "    produced $TOTAL_EVENTS events across 3 five-second windows"

# Oracle tally (what exactly-once output MUST equal):
#   W1 apple=5 banana=3
#   W2 apple=4 cherry=6
#   W3 banana=2 cherry=3
echo ">>> oracle totals:"
echo "     W1 apple=5  banana=3"
echo "     W2 apple=4  cherry=6"
echo "     W3 banana=2 cherry=3"

SQL="SELECT word, COUNT(*) FROM sql_in GROUP BY word, TUMBLE(event_time, INTERVAL '5' SECOND) EMIT ON WATERMARK INTO sql_out"

# ------- FIRST RUN: crashes mid-stream ---------------------------------------
echo
echo "=============================================================="
echo ">>> RUN 1: sql-job with --kill-after-records=8 (simulated crash)"
echo "=============================================================="
set +e
"$ROOT/bin/sql-job" \
  -broker "http://127.0.0.1:$P0" \
  -group sql-demo -state "$STATE" \
  -idle-ms 4000 -barrier-ms 200 \
  -kill-after-records 8 \
  -sql "$SQL" 2>&1 | tee "$DATA/run1.log" || true
set -e
grep -q "SIMULATED CRASH" "$DATA/run1.log" && echo "    run1 crashed as expected" || {
  echo "    WARN: run1 did not crash (it may have finished before hitting 8 records on some partition; continuing)"
}

# Inspect bbolt: should contain pending_out rows from the crashed epoch.
echo ">>> post-crash bbolt state files:"
ls -la "$STATE"/ | awk 'NR>1 {printf "    %s %s\n", $5, $NF}'

# ------- SECOND RUN: recovery + finish ---------------------------------------
echo
echo "=============================================================="
echo ">>> RUN 2: sql-job (recovery mode, no kill flag)"
echo "=============================================================="
"$ROOT/bin/sql-job" \
  -broker "http://127.0.0.1:$P0" \
  -group sql-demo -state "$STATE" \
  -idle-ms 4000 -barrier-ms 200 \
  -sql "$SQL" 2>&1 | tee "$DATA/run2.log"
grep -q "RECOVERY: replaying" "$DATA/run2.log" \
  && echo "    run2 replayed pending outputs from the crashed epoch" \
  || echo "    (no pending outputs needed replay — crash landed before any barrier)"

# ------- verify sql_out: per-window totals + no duplicate dedup ids ---------
echo
echo ">>> draining sql_out and checking exactly-once"

python3 - "$BASE_MS" <<'PY'
import base64, json, sys, urllib.request, collections

base_ms = int(sys.argv[1])
broker = "http://127.0.0.1:49092"

def get(u):
    with urllib.request.urlopen(u) as r:
        return json.loads(r.read())

meta = get(f"{broker}/metadata?topic=sql_out")
brokers = {int(k): v for k, v in meta["brokers"].items()}

dedup_ids = collections.Counter()
# Per-window aggregation.
final_counts = {}  # (word, start, end) -> count (last-observed value)

for p in meta["topics"][0]["partitions"]:
    leader = brokers[p["leader"]]
    pid = p["partition"]
    while True:
        resp = get(f"{leader}/consume?topic=sql_out&partition={pid}&group=verify&max=2000")
        if not resp["records"]:
            break
        for rec in resp["records"]:
            pl = base64.b64decode(rec["payload"]).decode()
            # "__DEDUP__<id>\t<word>\t<start_ms>\t<end_ms>\t<count>"
            assert pl.startswith("__DEDUP__"), f"missing dedup prefix: {pl!r}"
            rest = pl[len("__DEDUP__"):]
            dedup, _, body = rest.partition("\t")
            dedup_ids[dedup] += 1
            parts = body.split("\t")
            if len(parts) != 4:
                continue
            w, s, e, c = parts[0], int(parts[1]), int(parts[2]), int(parts[3])
            # TumblingWindow emits once per (key, window) per partition shard
            # with the final count; last-observed wins (they should all match).
            final_counts[(w, s, e)] = c

# Exactly-once check: every dedup id appears at most once.
dups = {d: c for d, c in dedup_ids.items() if c > 1}
if dups:
    print(f"    FAIL: {len(dups)} dedup_ids appeared more than once")
    for d, c in list(dups.items())[:5]:
        print(f"         {d} x {c}")
    sys.exit(1)
print(f"    OK: {sum(dedup_ids.values())} sink records, {len(dedup_ids)} unique dedup_ids  (no duplicates)")

# Sum across partition shards per window.
totals = collections.Counter()
for (w, s, e), c in final_counts.items():
    totals[(w, s, e)] += c

want = {
    ("apple",   base_ms +      0, base_ms +  5000): 5,
    ("banana",  base_ms +      0, base_ms +  5000): 3,
    ("apple",   base_ms +   5000, base_ms + 10000): 4,
    ("cherry",  base_ms +   5000, base_ms + 10000): 6,
    ("banana",  base_ms +  10000, base_ms + 15000): 2,
    ("cherry",  base_ms +  10000, base_ms + 15000): 3,
}

ok = True
for k, v in want.items():
    got = totals.get(k, 0)
    w, s, e = k
    window_idx = (s - base_ms) // 5000 + 1
    status = "OK" if got == v else "MISMATCH"
    print(f"    W{window_idx} {w:<8s} want={v} got={got}  [{status}]")
    if got != v:
        ok = False

if not ok:
    print(">>> DEMO FAILED: per-window counts diverge from oracle")
    sys.exit(1)
print(">>> exactly-once verified: dedup clean, per-window counts match oracle")
PY

echo
echo ">>> DEMO PASSED"
