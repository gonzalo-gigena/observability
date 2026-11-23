-- ReplacingMergeTree with `id` (the JetStream stream sequence) last in ORDER BY: a redelivered
-- message has the same id and sort key, so merges keep one copy, and reads use FINAL to be exact
-- before the merge runs. See README, "Duplicates on redelivery".
CREATE TABLE IF NOT EXISTS logs (
  project_id UInt32,
  ts DateTime64(3),
  id UInt64, -- JetStream stream sequence: unique, used for dedup and paging
  service LowCardinality(String),
  level LowCardinality(String),
  message String,
  trace_id String,
  attrs Map(LowCardinality(String), String),
  INDEX msg_idx message TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 4
) ENGINE = ReplacingMergeTree
PARTITION BY toDate(ts)
ORDER BY (project_id, service, ts, id)
TTL toDateTime(ts) + INTERVAL 30 DAY
SETTINGS non_replicated_deduplication_window = 1000;

CREATE TABLE IF NOT EXISTS metrics (
  project_id UInt32,
  ts DateTime64(3),
  id UInt64,
  service LowCardinality(String),
  name LowCardinality(String),
  labels Map(LowCardinality(String), String),
  value Float64
) ENGINE = ReplacingMergeTree
PARTITION BY toDate(ts)
ORDER BY (project_id, name, service, ts, id)
TTL toDateTime(ts) + INTERVAL 30 DAY
SETTINGS non_replicated_deduplication_window = 1000;

-- Per-minute rollups for charts. avg = sum(sum) / sum(count).
-- The view sees every insert, so a redelivered batch is counted twice here (see README).
CREATE TABLE IF NOT EXISTS metrics_1m (
  project_id UInt32,
  name LowCardinality(String),
  service LowCardinality(String),
  labels Map(LowCardinality(String), String),
  minute DateTime,
  count SimpleAggregateFunction(sum, UInt64),
  sum SimpleAggregateFunction(sum, Float64),
  min SimpleAggregateFunction(min, Float64),
  max SimpleAggregateFunction(max, Float64)
) ENGINE = AggregatingMergeTree
PARTITION BY toDate(minute)
ORDER BY (project_id, name, service, sipHash64(labels), minute)
TTL minute + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_1m_mv TO metrics_1m AS
SELECT project_id, name, service, labels, toStartOfMinute(ts) AS minute,
       count() AS count, sum(value) AS sum, min(value) AS min, max(value) AS max
FROM metrics
GROUP BY project_id, name, service, labels, minute;
