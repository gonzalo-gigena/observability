package main

// writer: pulls events from JetStream in batches and inserts them into ClickHouse.
// Runs as `api writer`, from the same image as the api. Scales with --scale writer=N:
// every replica shares the same durable pull consumers.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	batchSize     = 5000
	batchWait     = time.Second
	batchMaxBytes = 32 << 20 // 5,000 events of up to 256 KB each would be 1.2 GB
	insertTimeout = 20 * time.Second
	ackWait       = 30 * time.Second // > insertTimeout, so a slow insert isn't redelivered mid-flight
)

// Each table reads the JSON that ingest put on JetStream, plus project_id and id from the writer.
// Fields the event omits (trace_id, attrs) get the column default.
var inserts = map[string]string{
	"logs": `INSERT INTO logs (project_id, id, ts, service, level, message, trace_id, attrs)
SELECT project_id, id, fromUnixTimestamp64Milli(ts), service, level, message, trace_id, attrs
FROM input('project_id UInt32, id UInt64, ts Int64, service String, level String, message String, trace_id String, attrs Map(String, String)')
FORMAT JSONEachRow`,
	"metrics": `INSERT INTO metrics (project_id, id, ts, service, name, value, labels)
SELECT project_id, id, fromUnixTimestamp64Milli(ts), service, name, value, labels
FROM input('project_id UInt32, id UInt64, ts Int64, service String, name String, value Float64, labels Map(String, String)')
FORMAT JSONEachRow`,
}

var (
	writtenRows    = promauto.NewCounterVec(prometheus.CounterOpts{Name: "writer_rows_total", Help: "Rows inserted into ClickHouse."}, []string{"table"})
	insertFailures = promauto.NewCounterVec(prometheus.CounterOpts{Name: "writer_insert_failures_total", Help: "Failed batch inserts (redelivered)."}, []string{"table"})
	insertSeconds  = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "writer_insert_seconds", Help: "Batch insert latency.", Buckets: prometheus.DefBuckets}, []string{"table"})
)

type clickhouse struct {
	url, user, password string
	client              *http.Client
}

func (ch *clickhouse) exec(ctx context.Context, query string, body io.Reader) error {
	u := ch.url + "/?" + url.Values{
		"query": {query},
		// Pinned off: an async insert returns before the rows are written, so we'd ack too early.
		"async_insert": {"0"},
	}.Encode()
	req, err := http.NewRequestWithContext(ctx, "POST", u, body)
	if err != nil {
		return err
	}
	req.Header.Set("X-ClickHouse-User", ch.user)
	req.Header.Set("X-ClickHouse-Key", ch.password)
	res, err := ch.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(res.Body, 4<<10))
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("clickhouse %d: %s", res.StatusCode, bytes.TrimSpace(msg))
	}
	return nil
}

// rows turns a batch into JSONEachRow: each event's JSON object gets project_id and id
// (the stream sequence) spliced in front, without decoding the event.
func rows(batch []jetstream.Msg) ([]byte, error) {
	var buf bytes.Buffer
	for _, m := range batch {
		meta, err := m.Metadata()
		if err != nil {
			return nil, err
		}
		_, project, ok := strings.Cut(m.Subject(), ".")
		if !ok || len(m.Data()) < 2 || m.Data()[0] != '{' {
			return nil, fmt.Errorf("unexpected message on %q", m.Subject())
		}
		fmt.Fprintf(&buf, `{"project_id":%s,"id":%d`, project, meta.Sequence.Stream)
		if m.Data()[1] != '}' {
			buf.WriteByte(',')
		}
		buf.Write(m.Data()[1:])
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

func runWriter() error {
	replica, _ := os.Hostname()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("replica", replica, "service", "writer"))

	ch := &clickhouse{
		url:      os.Getenv("CLICKHOUSE_URL"),
		user:     os.Getenv("CLICKHOUSE_USER"),
		password: os.Getenv("CLICKHOUSE_PASSWORD"),
		client:   &http.Client{Timeout: insertTimeout},
	}
	natsURL := os.Getenv("NATS_URL")
	if ch.url == "" || natsURL == "" {
		return errors.New("missing env var CLICKHOUSE_URL or NATS_URL")
	}
	// Test-only fault injection: every Nth batch is inserted, then NAKed instead of acked,
	// as if the writer died between the two. The test proves those redeliveries add no rows.
	nakEvery, _ := strconv.Atoi(os.Getenv("WRITER_FAULT_NAK_EVERY"))

	nc, err := connectNATS(natsURL)
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	var ready atomic.Int32 // one per table loop that has its consumer
	var wg sync.WaitGroup
	for table := range inserts {
		wg.Go(func() { consume(ctx, js, ch, table, nakEvery, &ready) })
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if int(ready.Load()) < len(inserts) || !nc.IsConnected() || ch.exec(r.Context(), "SELECT 1", nil) != nil {
			w.WriteHeader(503)
		}
	})
	mux.Handle("GET /metrics", promhttp.Handler())
	srv := &http.Server{Addr: ":" + port(), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server", "err", err)
		}
	}()
	slog.Info("writer started")
	<-ctx.Done()
	slog.Info("SIGTERM received, finishing the current batch")
	wg.Wait() // unacked messages are redelivered anyway; this just avoids the AckWait delay
	return srv.Close()
}

// consume runs one table's loop: fetch up to batchSize messages or batchWait, one INSERT, ack.
func consume(ctx context.Context, js jetstream.JetStream, ch *clickhouse, table string, nakEvery int, ready *atomic.Int32) {
	var cons jetstream.Consumer
	for ctx.Err() == nil {
		var err error
		// The api creates the stream; until it has, this fails and retries.
		cons, err = js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
			Durable:       "writer-" + table,
			FilterSubject: table + ".*",
			AckPolicy:     jetstream.AckExplicitPolicy, // not AckAll: replicas share the consumer
			AckWait:       ackWait,
			MaxAckPending: 4 * batchSize, // the default (1,000) would cap batches at 1,000
		})
		if err == nil {
			break
		}
		slog.Warn("creating consumer, retrying", "table", table, "err", err)
		time.Sleep(time.Second)
	}
	if ctx.Err() != nil {
		return
	}
	ready.Add(1)

	query := inserts[table]
	for n := 1; ctx.Err() == nil; n++ {
		mb, err := cons.Fetch(batchSize, jetstream.FetchMaxBytes(batchMaxBytes), jetstream.FetchMaxWait(batchWait))
		if err != nil {
			slog.Warn("fetch failed", "table", table, "err", err)
			time.Sleep(time.Second)
			continue
		}
		var batch []jetstream.Msg
		for m := range mb.Messages() {
			batch = append(batch, m)
		}
		if len(batch) == 0 {
			continue
		}
		body, err := rows(batch)
		if err == nil {
			start := time.Now()
			ictx, cancel := context.WithTimeout(context.Background(), insertTimeout) // finish it on SIGTERM
			err = ch.exec(ictx, query, bytes.NewReader(body))
			cancel()
			insertSeconds.WithLabelValues(table).Observe(time.Since(start).Seconds())
		}
		if err != nil {
			// ponytail: no dead-letter subject; a batch ClickHouse always rejects retries forever.
			// ingest validates every event, so add one when that stops being enough.
			insertFailures.WithLabelValues(table).Inc()
			slog.Error("insert failed, will be redelivered", "table", table, "rows", len(batch), "err", err)
			for _, m := range batch {
				_ = m.NakWithDelay(time.Second)
			}
			continue
		}
		writtenRows.WithLabelValues(table).Add(float64(len(batch)))
		ack := func(m jetstream.Msg) error { return m.Ack() }
		if nakEvery > 0 && n%nakEvery == 0 {
			slog.Warn("fault: inserted batch NAKed instead of acked", "table", table, "rows", len(batch))
			ack = func(m jetstream.Msg) error { return m.Nak() }
		}
		for _, m := range batch {
			if err := ack(m); err != nil {
				// A lost ack means a redelivery, which ReplacingMergeTree absorbs.
				slog.Warn("ack failed", "table", table, "err", err)
				break
			}
		}
	}
}
