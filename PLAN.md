# 04 — Self-hosted log and metrics tool

> `webhook-sink`, `mailpit` come from the shared `common` submodule, already wired into `compose.yaml`.

An SDK sends logs and metrics to an ingest API. You store them in a column store,
search and chart them, and fire alerts to Slack. Follows the shared baseline
(`common/docs/baseline.md`).

**What it proves:** high-throughput ingestion, backpressure, OLAP schema design,
safe query parsing, alerting state machines.

## Stack

| Service | Tech | Scales? |
|---|---|---|
| `ingest` | Go: validates, authenticates, publishes to NATS | yes, stateless |
| `nats` | NATS JetStream: durable buffer between ingest and storage | single (cluster is stretch) |
| `writer` | Go: pulls from JetStream in batches, inserts into ClickHouse | yes (shared pull consumer) |
| `clickhouse` | ClickHouse: logs + metrics | single node (sharding is stretch) |
| `query` | Go: query language → ClickHouse SQL, live tail | yes |
| `alerter` | Go: evaluates rules, sends notifications | 2 replicas, one leader |
| `postgres` | projects, API keys, users, alert rules and alert state | single |
| `web` | React: search, live tail, dashboards, alerts | static |
| `demo-app` | a fake service that emits realistic logs/metrics using the SDK | — |
| `mailpit` | email alerts locally | — |
| `webhook-sink` | tiny HTTP server that logs incoming Slack/webhook payloads. The default alert target locally, so no Slack account is needed | — |

Docker needs about 8 GB of RAM for this project. Set ClickHouse's `max_server_memory_usage`,
`ulimits: { nofile: 262144 }`, and a password (`CLICKHOUSE_PASSWORD`), because the default
user has none.

**Scope:** phases 0–3 plus phase 5 with one notification channel make the portfolio
version. Dashboards, saved searches, the metrics explorer and OTLP are stretch goals.

## Steps

### Phase 0 — Scaffold
1. Baseline. Confirm `clickhouse/clickhouse-server` and `nats` images run on arm64 + amd64.
- **Done when:** `docker compose up` and all services pass their health checks.

### Phase 1 — Ingest path
1. `POST /v1/logs`, `POST /v1/metrics`: NDJSON, gzip allowed, max body 5 MB,
   max 10k events per request.
2. Auth via project API key (`Authorization: Bearer`). Store only a SHA-256 hash and
   show a visible prefix. Cache key lookups in memory for 60 s.
3. Validate and normalize (timestamp, level, service, message, attributes map).
   Reject bad events per item; don't fail the whole batch.
4. Publish to JetStream subject `logs.<project_id>` and wait for the publish ack
   before returning `202 Accepted`.
5. **Backpressure:** create the stream with `max_bytes` and `discard: new`. The
   default, `discard: old`, silently deletes the oldest data when the stream is full
   instead of rejecting new messages. When a publish is rejected or JetStream is
   unreachable, return `503` with `Retry-After`. The SDK retries with backoff and a
   bounded local buffer. JetStream fsyncs periodically by default, so "no loss"
   covers process crashes, not power loss. Say so in the README.
- **Done when:** `curl` an NDJSON file in and see it in JetStream.

### Phase 2 — Storage
1. ClickHouse `logs` table:
   ```sql
   CREATE TABLE logs (
     project_id UInt32, ts DateTime64(3),
     id UInt64,  -- JetStream stream sequence: unique, used for dedup and paging
     service LowCardinality(String),
     level LowCardinality(String), message String, trace_id String,
     attrs Map(LowCardinality(String), String),
     INDEX msg_idx message TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 4
   ) ENGINE = MergeTree
   PARTITION BY toDate(ts)
   ORDER BY (project_id, service, ts)
   TTL toDateTime(ts) + INTERVAL 30 DAY
   SETTINGS non_replicated_deduplication_window = 1000;
   ```
2. `metrics` table (name, labels, value, ts) + a materialized view into
   per-minute rollups (`AggregatingMergeTree`) for fast charts.
3. `writer`: pull 5,000 messages or 1 s (whichever first) → one batch INSERT →
   ack. Never insert row by row, because ClickHouse wants big batches. If an insert
   fails, don't ack; the messages are redelivered.
4. Duplicates: ClickHouse deduplicates repeated inserts into a plain (non-replicated)
   `MergeTree` only when `non_replicated_deduplication_window` is set (above). A
   redelivered batch may not hold exactly the same messages as the first attempt, so
   a random batch ID as `insert_deduplication_token` isn't enough. Either (a) build
   batches from deterministic sequence ranges, so a retry rebuilds the identical batch
   and token, or (b) use `ReplacingMergeTree` with `id` in the `ORDER BY` and
   deduplicate at read time. Pick one and explain why in the README.
- **Done when:** sustained throughput with one writer is measured and published (expect
  tens of thousands of events/sec on a laptop), and duplicate delivery does not
  duplicate rows.

### Phase 3 — Query
1. Query language: `service:api level:error "timeout" status>=500 @attr.region:eu`.
2. Hand-written parser (tokenizer → AST) → **parameterized** ClickHouse SQL.
   Never concatenate user input into SQL. Every query is forced to include
   `project_id` and a time range (max 7 days unless explicitly allowed).
3. Query limits: `max_execution_time`, `max_rows_to_read`, and results paginated by `(ts, id)` cursor.
4. Histogram endpoint (counts per bucket) for the timeline chart.
5. Live tail: `query` subscribes to the NATS subject and streams matches to the browser via SSE.
- **Done when:** table-driven parser tests (including SQL-injection attempts) pass,
  and a search over 10M rows (made by a data generator script) returns the first page
  in under 1 s on a laptop.

### Phase 4 — UI
1. Search bar, time picker, results with expandable attributes, histogram.
2. Live tail toggle, saved searches.
3. Metrics explorer: pick a metric, group by label, line chart.
4. Dashboards: a grid of saved charts stored as JSON in Postgres.

### Phase 5 — Alerts
1. Rule: `query + condition (count > N | metric avg > X) + window + for duration`.
2. Evaluator runs every 30 s on the leader only (`pg_try_advisory_lock`).
3. State machine per rule: `ok → pending → firing → resolved`, stored in Postgres.
   Notify only on transitions, which deduplicates alerts automatically.
4. Channels: Slack incoming webhook, email (Mailpit locally), generic webhook
   (HMAC-signed). Locally all of them point at `webhook-sink` or Mailpit by default.
   Retry notifications through a simple outbox table.
- **Done when:** `demo-app` has an "incident" mode that triggers an error spike → one
  notification on firing and one on resolve.

### Phase 6 — SDK
1. TypeScript SDK (`@yourname/logs`): `logger.info()`, batching every 1 s or 500 events,
   gzip, retry with backoff, bounded buffer, flush on process exit.
2. Stretch: accept **OTLP/HTTP** at `/v1/otlp/logs` so any OpenTelemetry SDK can send data.
   This makes the tool look much more real.
- **Done when:** `demo-app` uses the published SDK only.

### Phase 7 — Scale and measure
1. A Go load generator (or k6) at increasing rates, at 1, 2 and 4 replicas of
   `ingest` and `writer` (`common/scripts/scale.sh`, one `--scale` flag per service).
2. Chart events/sec vs replicas, end-to-end latency (emit → searchable), and
   JetStream backlog during a 2× spike (it should absorb the spike, then drain).
3. Document the next steps: ClickHouse cluster with Keeper (sharding + replication),
   a NATS cluster, tiered storage to S3 for old partitions.

## Headline test
End-to-end: the SDK sends 1M events with a known distribution → after the backlog
drains, `SELECT count()` and per-level counts in ClickHouse match exactly
(no loss, no duplicates), including a run where `writer` is killed mid-way.

## Scaling notes
- `ingest` doesn't touch the database on the hot path. JetStream decouples
  burst rate from ClickHouse insert rate. That decoupling is the main idea of the project.
- Multi-tenancy: `project_id` first in `ORDER BY` keeps tenant queries fast and isolated.
