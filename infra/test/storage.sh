#!/bin/sh
# Phase 2 "done when": one writer's sustained throughput, and redelivery adds no rows.
#   1. With the writer stopped, ingest N log events (+1,000 metrics) so JetStream holds a backlog.
#   2. Start one writer and time how long it takes to drain the backlog into ClickHouse.
#   3. Restart the writer with fault injection (every 3rd inserted batch is NAKed, so it is
#      redelivered and inserted again), ingest M more, SIGKILL the writer mid-way, restart it.
#   4. Exact counts, per level, after everything drains and after a forced merge.
# Usage: sh infra/test/storage.sh [N] [M]   (CI: 500000 200000)
# Brings the stack up and tears it down (down -v). HTTP_PORT and COMPOSE_PROJECT_NAME are honoured.
set -eu
N=${1:-500000}
M=${2:-200000}
BATCH=10000
[ $((N % BATCH)) = 0 ] && [ $((M % (2 * BATCH))) = 0 ] || { echo "N must be a multiple of $BATCH, M of $((2 * BATCH))" >&2; exit 2; }
base="http://localhost:${HTTP_PORT:-8080}"
compose="docker compose -f compose.yaml"
tmp=$(mktemp -d)

cleanup() {
  [ "${KEEP:-}" = 1 ] || $compose down -v --remove-orphans >/dev/null 2>&1
  rm -rf "$tmp"
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; $compose logs --tail 50 writer clickhouse >&2 || true; exit 1; }
ch() { $compose exec -T clickhouse clickhouse-client --password "$(grep ^CLICKHOUSE_PASSWORD= .env | cut -d= -f2-)" -q "$1"; }
now() { python3 -c 'import time; print(time.time())'; }

# 10,000 events per batch, levels 60% info, 20% warn, 10% error, 10% debug.
awk 'BEGIN { split("info info info info info info warn warn error debug", lv, " ");
  for (i = 0; i < 10000; i++) printf "{\"service\":\"svc%d\",\"level\":\"%s\",\"message\":\"event %d\",\"attrs\":{\"i\":\"%d\"}}\n", i % 5, lv[i % 10 + 1], i, i }' \
  | gzip > "$tmp/logs.ndjson.gz"
awk 'BEGIN { for (i = 0; i < 1000; i++) printf "{\"service\":\"api\",\"name\":\"req_ms\",\"value\":%d,\"labels\":{\"route\":\"/r%d\"}}\n", i % 100, i % 3 }' \
  > "$tmp/metrics.ndjson"

post() { # path file [gzip]
  enc=""; [ "${3:-}" ] && enc="-H Content-Encoding:gzip"
  code=$(curl -s -o "$tmp/res" -w '%{http_code}' "$base$1" -H 'Authorization: Bearer obs_demo_local_dev_only' $enc --data-binary @"$2")
  [ "$code" = 202 ] && ! grep -q '"rejected_count":[1-9]' "$tmp/res" || fail "POST $1 -> $code $(cat "$tmp/res")"
}
post_logs() { i=0; while [ $i -lt "$1" ]; do post /v1/logs "$tmp/logs.ndjson.gz" gzip; i=$((i + BATCH)); done; }
wait_rows() { # expected timeout_s
  deadline=$(( $(date +%s) + $2 ))
  while [ "$(ch 'SELECT count() FROM logs FINAL')" != "$1" ]; do
    [ "$(date +%s)" -lt "$deadline" ] || fail "logs has $(ch 'SELECT count() FROM logs FINAL') rows after $2 s, want $1"
    sleep 0.2
  done
}
stream_msgs() { $compose exec -T nats wget -qO- 'http://localhost:8222/jsz' | sed -n 's/^ *"messages": *\([0-9]*\).*/\1/p' | head -1; }

echo "== stack up"
$compose up -d --wait --build || { $compose logs; exit 1; }
$compose run --rm seed >/dev/null

echo "== 1. backlog: $N logs + 1,000 metrics with the writer stopped"
$compose stop writer
post_logs "$N"
post /v1/metrics "$tmp/metrics.ndjson"
[ "$(stream_msgs)" = $((N + 1000)) ] || fail "stream holds $(stream_msgs) messages, want $((N + 1000))"

echo "== 2. one writer drains it"
start=$(now)
$compose start writer
wait_rows "$N" 600
end=$(now)
# Sustained rate: first insert's start to last insert's end, from ClickHouse's own query log,
# so container start and this script's polling don't count. Wall clock is printed too.
ch 'SYSTEM FLUSH LOGS'
ms=$(ch "SELECT dateDiff('millisecond', min(query_start_time_microseconds), max(event_time_microseconds))
  FROM system.query_log WHERE type = 'QueryFinish' AND query_kind = 'Insert' AND has(tables, 'default.logs')")
python3 -c "import sys; n, ms, s, e = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4]); print(f'RESULT: one writer, {n:,} events: {n / ms * 1000:,.0f} events/s sustained ({ms / 1000:.1f} s first to last insert), {e - s:.1f} s wall clock from writer start')" "$N" "$ms" "$start" "$end"
[ "$(ch 'SELECT count() FROM metrics FINAL')" = 1000 ] || fail "metrics rows: $(ch 'SELECT count() FROM metrics FINAL')"
[ "$(ch 'SELECT sum(count), sum(sum) FROM metrics_1m' | tr '\t' ' ')" = "1000 49500" ] || fail "rollup: $(ch 'SELECT sum(count), sum(sum) FROM metrics_1m')"

echo "== 3. redelivery: every 3rd batch inserted then NAKed, plus a SIGKILL mid-way"
WRITER_FAULT_NAK_EVERY=3 $compose up -d --wait writer
post_logs $((M / 2))
$compose kill -s SIGKILL writer
post_logs $((M / 2))
WRITER_FAULT_NAK_EVERY=3 $compose up -d --wait writer
total=$((N + M))
wait_rows "$total" 600
deadline=$(( $(date +%s) + 60 ))
until [ "$(stream_msgs)" = 0 ]; do [ "$(date +%s)" -lt "$deadline" ] || fail "stream not drained: $(stream_msgs)"; sleep 0.5; done
naks=$($compose logs writer | grep -c 'fault: inserted batch NAKed' || true)
[ "$naks" -gt 0 ] || fail "no fault was injected"

echo "== 4. exact counts"
raw=$(ch 'SELECT count() FROM logs')
echo "$naks batches redelivered after insert; physical rows before merge: $raw, distinct: $total"
[ "$(ch 'SELECT uniqExact(id) FROM logs')" = "$total" ] || fail "distinct ids: $(ch 'SELECT uniqExact(id) FROM logs')"
want="debug $((total / 10))
error $((total / 10))
info $((total * 6 / 10))
warn $((total * 2 / 10))"
got=$(ch 'SELECT level, count() FROM logs FINAL GROUP BY level ORDER BY level' | tr '\t' ' ')
[ "$got" = "$want" ] || fail "per-level counts: got
$got
want
$want"
ch 'OPTIMIZE TABLE logs FINAL'
[ "$(ch 'SELECT count() FROM logs')" = "$total" ] || fail "after merge: $(ch 'SELECT count() FROM logs') rows, want $total"
echo "PASS: $total events stored exactly once (per-level counts match, no rows lost or duplicated)"
