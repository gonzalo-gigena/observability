# Logs and metrics

A self-hosted log and metrics pipeline: ingest, store, search, and alert.

The step-by-step build plan is in [PLAN.md](PLAN.md).

> **Status:** phases 1–2 are built: logs and metrics go in over HTTP, through JetStream, into ClickHouse.
> Search and alerts are the next phases.

## Run it

```bash
git clone --recurse-submodules https://github.com/gonzalo-gigena/observability.git
cd observability
cp .env.example .env
docker compose up --watch
```

Open http://localhost:8080. Needs only Docker with Compose v2. Code changes reload
automatically (Compose Watch). On Windows, clone inside the WSL filesystem, not `/mnt/c`.

Cloned without `--recurse-submodules`? Run `git submodule update --init`.

## Send logs and metrics

```bash
docker compose run --rm seed   # demo project + public demo key, never seed production
cat > events.ndjson <<'NDJSON'
{"service":"checkout","level":"error","message":"payment failed","attrs":{"status":502}}
{"service":"checkout","message":"order placed"}
not json
NDJSON
curl -i http://localhost:8080/v1/logs \
  -H 'Authorization: Bearer obs_demo_local_dev_only' \
  --data-binary @events.ndjson
# HTTP/1.1 202 Accepted
# {"accepted":2,"rejected":[{"line":3,"error":"invalid JSON object"}],"rejected_count":1}

# A second later the writer has moved them into ClickHouse
docker compose exec clickhouse clickhouse-client --password local-dev-only \
  -q 'SELECT ts, service, level, message, attrs FROM logs FINAL ORDER BY ts DESC LIMIT 5'
```

To watch them sit in JetStream instead, `docker compose stop writer` first, then check
`docker compose exec nats wget -qO- 'http://localhost:8222/jsz?streams=true' | grep '"messages"'`.

### The API

`POST /v1/logs` and `POST /v1/metrics` take NDJSON (one JSON object per line), optionally with
`Content-Encoding: gzip`.

| Field | Logs | Metrics |
|---|---|---|
| `service` | required, up to 100 bytes | required, up to 100 bytes |
| `ts` (or `timestamp`) | RFC 3339, or unix s / ms / µs / ns. Default: now. Up to 30 days old, 1 h ahead | same |
| `message` (or `msg`) | required, cut at 32 KB | |
| `level` | `trace` `debug` `info` `warn` `error` `fatal`, common aliases accepted. Default: `info` | |
| `trace_id` | optional, hex | |
| `attrs` | object, up to 64 keys. Values become strings, cut at 4 KB | |
| `name` | | required, `[a-zA-Z_:][a-zA-Z0-9_:.]*` |
| `value` | | required, a finite number |
| `labels` | | same rules as `attrs` |

| Status | Meaning | Client should |
|---|---|---|
| `202` | JetStream acknowledged every valid event. Bad lines are in `rejected` (first 100) and `rejected_count` | Drop the batch |
| `400` | Empty or unreadable body, or no valid event | Fix it, don't retry |
| `401` | Missing, unknown or revoked key | Fix the key |
| `413` | Over 5 MB sent, 10 MB after gunzip, or 10,000 events | Split the batch |
| `415` | An encoding other than gzip | Fix it |
| `503` + `Retry-After` | Stream full, NATS or Postgres unreachable, or this replica has too many bytes in flight | Retry the same batch, with backoff and jitter |

A single event can be up to 256 KB. A larger one is rejected as a bad line.

Send an `Idempotency-Key` (up to 128 bytes, a new one for each batch) to make retries safe. A `503`
can leave part of a batch stored, and a retry with the same key within 2 minutes stores only the rest.
Without a key, or after 2 minutes, that part is stored twice. Reusing a key for a different batch drops events.

### What "no loss" means

A `202` is sent only after JetStream acknowledged every valid event in the batch, so acknowledged events
survive a crash or `SIGKILL` of the api or NATS process. **It does not cover power loss or a kernel crash:**
JetStream fsyncs every 2 minutes by default, so the host can lose up to that much acknowledged data.
Covering that needs `sync_interval: always` (much slower) or a replicated NATS cluster.

When the stream reaches `STREAM_MAX_BYTES` (1 GB by default) new events get `503` and stored ones are kept.
The writer acks a message only after the batch holding it is in ClickHouse (a synchronous insert),
so an event leaves JetStream only once it's stored. A writer that dies mid-batch leaves its messages
unacked, and JetStream redelivers them after 30 s.

## Storage

| Table | Engine | Sort key | What |
|---|---|---|---|
| `logs` | `ReplacingMergeTree` | `(project_id, service, ts, id)` | One row per log event. `id` is the JetStream stream sequence |
| `metrics` | `ReplacingMergeTree` | `(project_id, name, service, ts, id)` | One row per data point |
| `metrics_1m` | `AggregatingMergeTree`, fed by `metrics_1m_mv` | `(project_id, name, service, sipHash64(labels), minute)` | Per-minute count, sum, min, max for charts. avg = `sum(sum) / sum(count)` |

All are partitioned by day with a 30-day TTL. The schema is in
[`infra/clickhouse/migrations`](infra/clickhouse/migrations), applied by the one-shot `migrate-clickhouse` service.

**Read `logs` and `metrics` with `FINAL`** (or count `uniqExact(id)`). A redelivered message is stored a
second time with the same `id`, and the copy only disappears when ClickHouse merges the parts, which can be
minutes later. `FINAL` merges at read time, so counts are exact either way.

The writer pulls up to 5,000 messages (or 32 MB, or whatever arrived within 1 s), turns them into one
`INSERT … FORMAT JSONEachRow` over ClickHouse's HTTP interface, and acks each message after the insert
returns. A failed insert NAKs the batch, which comes back a second later. Run more with `--scale writer=N`.
They share one durable pull consumer per table.

### Create an API key

Only the SHA-256 and the first 12 characters (the visible prefix) are stored. This prints the key once.
Revoking (`UPDATE api_keys SET revoked_at = now() …`) takes up to 60 s to apply, because of the cache.

```bash
docker compose exec -T db psql -U app app -qtA <<'SQL'
INSERT INTO projects (name) VALUES ('my-project') ON CONFLICT (name) DO NOTHING;
WITH k AS (SELECT 'obs_' || replace(gen_random_uuid()::text, '-', '') AS key),
     saved AS (
       INSERT INTO api_keys (project_id, prefix, key_hash)
       SELECT p.id, left(k.key, 12), sha256(k.key::bytea) FROM projects p, k WHERE p.name = 'my-project'
     )
SELECT key FROM k;
SQL
```

## Everyday commands

| What | Command |
|---|---|
| Seed demo data | `docker compose run --rm seed` |
| SQL shell | `docker compose exec db psql -U app app` |
| ClickHouse shell | `docker compose exec clickhouse clickhouse-client --password local-dev-only` |
| Storage test (writer throughput + no duplicates) | `sh infra/test/storage.sh 1000000 200000` (brings up its own stack, then `down -v`) |
| Scale the api | `docker compose up -d --scale api=3` |
| Prove scaling works | `sh common/scripts/smoke.sh 3` |
| Run exactly what CI runs | `sh common/scripts/ci-stack.sh 3` |
| Load test at 1, 2, 4 replicas | `sh common/scripts/scale.sh` |
| Metrics (Prometheus) | `docker compose --profile observability up -d`, then http://localhost:9090 |
| Migration lint | `sh common/scripts/check-migrations.sh` |
| Unit tests | `docker run --rm -v "$PWD/apps/api:/src" -w /src golang:1.27.4-bookworm sh -c "go vet ./... && go test ./..."` |
| New migration | add `infra/db/migrations/00000N_name.up.sql` + `.down.sql` (ClickHouse: `infra/clickhouse/migrations`), restart |
| Update shared code | `git submodule update --remote common` |
| Reset everything | `docker compose down -v` |

## Fakes (from common)

These start in dev and CI (`COMPOSE_PROFILES=fakes`), never in production.
Full reference: [`common/README.md`](common/README.md).

| Service | What |
|---|---|
| `webhook-sink` | Records webhooks sent to `http://webhook-sink:8091/hooks/<name>`. View at http://localhost:8091. |
| `mailpit` | Catches all email. Send to `smtp://mailpit:1025`, read at http://localhost:8025. |

## Architecture

```mermaid
flowchart LR
  services["Your services<br/>SDK or OTLP"]
  browser["Browser<br/>search, live tail, alerts"]
  subgraph stack["docker compose"]
    caddy["Caddy :8080"]
    nats[("NATS JetStream<br/>durable buffer")]
    writer["writer ×N"]
    ch[("ClickHouse<br/>logs, metrics")]
    api["api ×N<br/>ingest today, query later"]
    alerter["alerter ×2<br/>one leader"]
    db[("Postgres<br/>projects, API keys, alert rules")]
    sink["webhook-sink<br/>(fake Slack)"]
    mail["Mailpit<br/>(fake email)"]
  end
  services -->|"POST /v1/logs, /v1/metrics"| caddy
  browser --> caddy
  caddy --> api
  api -->|"publish, wait for ack"| nats
  nats -->|"5,000 msgs or 1 s"| writer
  writer -->|"one INSERT per batch, then ack"| ch
  api -.->|"parameterized SQL"| ch
  nats -.->|"live tail"| api
  api --> db
  alerter -.-> ch
  alerter -.->|"rule state"| db
  alerter -.-> sink
  alerter -.-> mail
  class alerter planned
  classDef planned stroke-dasharray: 5 5
```

Solid boxes and arrows run today. Dashed ones are planned in [PLAN.md](PLAN.md).

Ingest never touches ClickHouse. JetStream absorbs bursts, and `writer` drains it in large batches at the rate ClickHouse handles well.

## Stack

- **api:** Go 1.27 (`net/http` + `pgx` + Prometheus client), distroless runtime image. Stateless, so it scales with `--scale api=N`.
  The plan's `ingest` service lives in this binary, so there is one image and one thing to scale.
- **NATS JetStream 2.15:** the `TELEMETRY` stream, subjects `logs.<project_id>` and `metrics.<project_id>`,
  one message per event.
- **Caddy:** the only service with a host port. It load-balances `/api/*` and `/v1/*` across every
  replica (`import lb api 8080`, from common) and serves `apps/web`.
- **writer:** the same Go binary, run as `api writer`, so there is still one image. JetStream → ClickHouse
  over ClickHouse's HTTP interface with the standard library, no ClickHouse driver.
- **ClickHouse 26.8 (LTS):** `logs`, `metrics` and per-minute rollups. `max_server_memory_usage` is set below
  the container's 2 GB limit, `nofile` is raised to 262,144, and the default user has a password (`CLICKHOUSE_PASSWORD`).
- **Postgres 18:** migrations run once before the api starts (`golang-migrate`, which also migrates ClickHouse).
- **common/:** git submodule with the shared fakes, scripts, Caddy snippet and Prometheus.
  See [portfolio-common](common/README.md).

## Deploy (one VM)

```bash
docker compose -f compose.yaml -f compose.prod.yaml up -d
```

The server's `.env` needs `SITE_ADDRESS`, `IMAGE_REPO`, `TAG`, a strong password, and no `fakes`
profile. Caddy gets TLS automatically. CI pushes multi-arch images to GHCR on every merge to `main`.

## Decisions & tradeoffs

| Decision | Chosen | Rejected | Why, and what it costs |
|---|---|---|---|
| Buffer between ingest and storage | NATS JetStream | Kafka, or writing straight to ClickHouse | Decouples burst rate from insert rate, with one small binary instead of a Kafka cluster. Cost: one more stateful service, and JetStream fsyncs periodically, so "no loss" covers process crashes, not power loss. |
| Backpressure | Stream `max_bytes` with `discard: new`. ingest answers 503 with `Retry-After`, and the SDK retries from a bounded buffer | The default, `discard: old` | `discard: old` silently deletes the oldest data when the stream is full. Rejecting new data is visible and retryable. |
| Stream retention | Work queue: a message is deleted once its consumer acks it | The default limits retention | With `discard: new`, limits retention never frees space, so a full stream would stay full after the writer caught up. Cost: no replay from JetStream, and only one consumer per subject. Live tail uses plain NATS subscriptions, which still work. Retention can't be changed without recreating the stream. |
| Ingest binary | Routes in the existing `api` | A separate `ingest` service | Smallest change, and the shared scripts already scale `api`. Cost: query and ingest will compete for the same CPU and memory until they're split. |
| Overload | Each replica caps decoded bytes in flight at 48 MB, then 503 | No limit | Without it, eight 400 KB gzip requests OOM-killed the 256 MB container. Cost: one busy project can use up the budget for everyone until there are per-project quotas. |
| API keys | SHA-256, hits and misses cached for 60 s per replica | bcrypt, or a lookup per request | Keys are random, so a slow hash adds nothing. Cached keys keep working while Postgres is down. Cost: revocation and newly created keys can lag by 60 s. |
| Storage | ClickHouse, `ORDER BY (project_id, service, ts, id)`, daily partitions, 30-day TTL | Postgres, Elasticsearch | Columnar and compressed, built for append-heavy, scan-heavy data. `project_id` first keeps each tenant's queries inside its own ranges. Cost: it wants big batches, and updates or deletes are expensive. |
| Inserts | Batches of 5,000 messages, 32 MB or 1 s. Synchronous insert (`async_insert=0`), then ack each message | Row-by-row inserts, async inserts | Each insert creates a part, and many small inserts end in "too many parts". An async insert returns before the rows are written, so acking after it could lose data. A failed insert is NAKed and redelivered. |
| Duplicates on redelivery | `ReplacingMergeTree` with `id` (the stream sequence) last in `ORDER BY`, and `FINAL` at read time | Deterministic batches from sequence ranges with an `insert_deduplication_token` | Writers share one pull consumer, so a redelivered message lands in whatever batch some writer fetches next, mixed with new ones. Rebuilding the identical batch would need one writer per sequence range and no 1 s partial batches. With `id` in the key, any redelivery collapses, however it's batched. Cost: reads need `FINAL` (or `uniqExact(id)`) until merges run, and the `metrics_1m` rollup counts a redelivered batch twice, because a materialized view sees every insert. Raw `metrics` stay exact. `non_replicated_deduplication_window` stays on and drops a byte-identical retried block for free. |
| ClickHouse client | Its HTTP interface with `net/http`, `INSERT … SELECT … FROM input() FORMAT JSONEachRow` | `clickhouse-go` | The JetStream payload is already JSON. The writer splices `project_id` and `id` in front without decoding it, and ClickHouse converts unix ms with `fromUnixTimestamp64Milli`. No new dependency. Cost: JSON is slower to parse than the native format, which matters past one writer's throughput below. |
| Writer binary | `api writer`, same image | A separate Go module and image | One image to build, push and version. Cost: the writer image carries the api's code. |
| Query language | Hand-written tokenizer → AST → parameterized SQL. Every query must include `project_id` and a time range | Accepting raw SQL, or building SQL strings | No injection by construction, and one tenant can't scan everyone's data or the whole table. |
| Alerting | Evaluator runs only on the leader (`pg_try_advisory_lock`). State machine `ok → pending → firing → resolved` in Postgres. Notify on transitions, retry through an outbox | Notifying on every evaluation | Transitions dedupe alerts for free, and the outbox survives a crash between changing state and sending. |
| Alert targets in dev and CI | `webhook-sink` and Mailpit from `common/` | A real Slack workspace | Runs offline, and tests can read exactly what was sent. |
| Load balancing | Caddy `dynamic a` upstreams (the `lb` snippet in `common/`) | `reverse_proxy api:8080`, Traefik | Caddy re-reads Docker DNS every 5 s, so `--scale api=N` needs no config change and no Docker socket mount. A plain `reverse_proxy api:8080` is a single upstream to Caddy and doesn't reliably spread load. |
| Migrations | A one-shot `migrate` service that finishes before the api starts | Each replica migrating on boot | Replicas starting together would race on the same migration. |

Rows for parts that aren't built yet record the plan's choice. Each one gets confirmed or revised when that phase is built.

## Scaling

### Measured

Ingest (phase 1): k6 with 8 virtual users for 10 s, posting gzipped 500-event batches (about 240 bytes
stored per event) through Caddy, three runs per replica count. Apple M1, 8 GB given to Docker, with
other Compose stacks busy on the same machine (host load average 6 to 15), so treat it as a noisy baseline:

| api replicas | events/s (each run) | p95 latency | failed |
|---|---|---|---|
| 1 | 93,666 / 97,809 / 38,921 | 64 / 94 / 190 ms | 0.00% |
| 3 | 92,995 / 116,999 / 135,324 | 100 / 35 / 27 ms | 0.00% |

In all six runs the stream held exactly as many messages as k6 saw accepted. The spread between runs is
as large as the gap between replica counts, so this doesn't show whether replicas help. Phase 7 measures that.

Writer (phase 2): `sh infra/test/storage.sh 1000000 200000`. 1M log events (about 150 bytes each) wait
in JetStream, then one writer drains them. "Sustained" is from the first insert's start to the last one's
end, taken from ClickHouse's `query_log`. Wall clock also counts the container starting. Same M1, with
other Compose stacks busy on the machine:

| Run | Sustained, one writer | Wall clock | Redelivered after insert (fault test) | Rows after merge |
|---|---|---|---|---|
| 1 | 64,821 events/s | 18.7 s | 19 batches, 5,000 extra physical rows | exactly 1,200,000 |
| 2 | 54,392 events/s | 23.6 s | 20 batches, 6,204 extra | exactly 1,200,000 |
| 3 | 55,599 events/s | 22.8 s | 20 batches, 8,241 extra | exactly 1,200,000 |

A 5,000-event batch takes about 160 ms, half of it the insert and half fetching and acking, done one
after the other. Overlapping the fetch of the next batch with the current insert, or a second writer,
is the next step if this becomes the bottleneck. Ingest measured 90k+ events/s above, so one writer is
slower than one api; phase 7 measures writers at 1, 2 and 4 replicas. CI runs the same test with 500k + 200k.

The skeleton's baseline: k6 with 20 virtual users for 20 s against `GET /api/hello`, which reads one row from Postgres through Caddy. Run on an Apple M1 laptop with 8 CPUs and 8 GB given to Docker, with the dev override left out:

```bash
COMPOSE_FILE=compose.yaml sh common/scripts/scale.sh
```

| api replicas | req/s | p95 latency | failed |
|---|---|---|---|
| 1 | 17,689 | 2.22 ms | 0.00% |
| 2 | 16,353 | 2.56 ms | 0.00% |
| 4 | 14,937 | 2.96 ms | 0.00% |

4 replicas served 0.8× the requests of 1. One Go process already spreads across every core, and the run shares 8 CPUs with Caddy, Postgres and k6, so on one machine extra api replicas buy redundancy, not throughput. Replica counts matter for `ingest` and `writer`, which phase 7 measures.

### How each part scales

| Part | Scales by | Limit |
|---|---|---|
| ingest | `--scale api=N` (stateless) | No database on the hot path. API keys are cached for 60 s. |
| `writer` | `--scale writer=N`, one shared pull consumer | ClickHouse insert rate |
| `query` | `--scale query=N` | ClickHouse CPU, bounded by `max_execution_time` and `max_rows_to_read` |
| `alerter` | 2 replicas, one leader | Only the leader evaluates rules |
| JetStream | One node | Disk, capped by `max_bytes` |
| ClickHouse | One node | Insert and scan throughput of one machine |

### Where it breaks next

- **ClickHouse on one node.** Next: a cluster with Keeper, for sharding and replication.
- **JetStream on one node.** Next: a 3-node NATS cluster with replicated streams.
- **Disk.** Move old partitions to S3 with tiered storage.

Phase 7 adds the real test: events/s at 1, 2 and 4 replicas of `api` and `writer` together, emit-to-searchable latency, and the JetStream backlog during a 2× spike, which should fill and then drain.
