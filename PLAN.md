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

## Beyond the plan

Build these after the phases above. Each starts from a problem real systems hit, and
each should add a row to the README's Decisions & tradeoffs table. If one wouldn't,
skip it. B1 is the one to build first.

### B1 — Dogfood it
**Why:** an observability tool's first customer is its own team. The other five
portfolio projects already produce load and failures, so send their telemetry here.
1. Add OpenTelemetry export to each project, off by default and turned on by
   `OTEL_EXPORTER_OTLP_ENDPOINT` (needs the OTLP endpoint from phase 6).
2. Both stacks run on one machine: join them with a shared external Docker network, or
   send to `host.docker.internal:8080`.
3. One project here per repo, with its own API key, and a few alert rules each
   (5xx rate, p95 latency).
- **Done when:** a load test in another repo (e.g. ticket-booking's `spike_onsale`) fires
  and resolves an alert here, with a screenshot in the README.

### B2 — Traces
**Why:** "the request was slow, but where?" Logs already have a `trace_id` column and
nothing to open it with.
1. Accept OTLP/HTTP traces at `/v1/otlp/traces`, into a ClickHouse `spans` table ordered
   by `(project_id, trace_id, start_ts)`, with a skip index for search by service and duration.
2. A waterfall view per trace. Clicking a log line's `trace_id` opens its trace.
3. Spans of one trace arrive out of order and across writers, so the view renders
   partial traces and fills in as spans arrive.
4. Tail-based sampling: keep every trace that has an error or is slower than X, plus 5% of
   the rest. That needs the whole trace before deciding, so buffer spans by `trace_id`
   with a decision timeout (e.g. 10 s), and route all spans of one trace to the same
   sampler (hash `trace_id` into the JetStream subject).
- **Done when:** sampling keeps 100% of error traces while storing about 5% of all traces,
  and the README states what the timeout loses (spans that arrive after it).

### B3 — Cardinality limits
**Why:** someone adds `user_id` as a metric label. The series count explodes, queries
slow down for everyone, and in a hosted product it's the classic surprise bill.
1. Track distinct series per metric per project over a rolling window. An approximate
   count (HyperLogLog) is enough. Each `ingest` replica sees only part of the traffic:
   either merge sketches across replicas (they merge by design), or enforce in `writer`.
   Pick one and say why.
2. Past a per-project limit, drop new series for that metric (existing series keep
   working) and count what was dropped.
3. Alert the project owner, and show the label that grew the most.
- **Done when:** a `demo-app` bug mode that adds `request_id` as a label gets capped and
  alerted within a minute, and other projects' ingest and query latency don't change.

### B4 — SLOs with burn-rate alerts
**Why:** threshold alerts page on every blip and miss slow leaks. SRE teams alert on how
fast the error budget is burning.
1. An SLO is a good-events query, a total-events query, a target (e.g. 99.9%) and a
   window (30 days). Show the error budget left.
2. Multi-window, multi-burn-rate alerts from the Google SRE workbook: page at a burn rate
   of 14.4 over 1 h *and* 5 min, or 6 over 6 h *and* 30 min; open a ticket at 1 over
   3 days *and* 6 h. Reuse the phase 5 state machine.
3. Precompute per-minute good/total counts with a materialized view, so evaluation
   doesn't scan raw logs every 30 s.
- **Done when:** a 1-minute blip of 10% errors doesn't page. Sustained 10% errors against
  a 99.9% SLO page after about 9 minutes (60 min × 14.4 ÷ 100), and the alert resolves
  about 5 minutes after the errors stop.

### B5 — PII scrubbing and deletion requests
**Why:** a developer logs a whole request body with emails and card numbers. Later, a
user asks for their data to be deleted (GDPR).
1. Redact in `ingest`, before anything reaches JetStream: built-in rules for emails, card
   numbers (regex plus a Luhn check, which cuts false positives) and `Bearer …` tokens,
   plus custom rules per project. Count redactions per rule.
2. Deletion requests remove every log with a given attribute value (e.g. `user_id`).
   That's expensive in ClickHouse: compare lightweight `DELETE`, a mutation and masking
   at read time on 10M rows, and record the choice.
3. The 30-day TTL already limits how long data lives. Say so in the README, because it
   is part of the answer.
- **Done when:** card numbers sent in a test never reach ClickHouse, and a deletion
  request removes one user's logs, with its measured cost in the README.
