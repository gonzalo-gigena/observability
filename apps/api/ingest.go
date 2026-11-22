package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	maxBodyBytes    = 5 << 20  // on the wire
	maxDecodedBytes = 10 << 20 // after gunzip, so a gzip bomb can't eat memory
	maxInFlight     = 48 << 20 // decoded bytes per replica across requests; sized for mem_limit: 256m
	eventOverhead   = 256      // charged per event on top: message, header, ack future
	maxEvents       = 10_000
	maxRejections   = 100 // per-item errors echoed back
	keyCacheTTL     = 60 * time.Second
	ackTimeout      = 5 * time.Second
	lookupTimeout   = 3 * time.Second
	maxIdemKeyLen   = 128 // copied into the header of every message in the batch
	streamName      = "TELEMETRY"
)

var events = promauto.NewCounterVec(prometheus.CounterOpts{Name: "ingest_events_total", Help: "Ingested events by kind and result."}, []string{"kind", "result"})

// errUnavailable maps to 503 + Retry-After: the client should retry the same batch.
var errUnavailable = errors.New("temporarily unavailable, retry")

// ensureStream creates or updates the stream. discard: new rejects publishes once
// max_bytes is reached; the default (old) would silently delete the oldest data.
func ensureStream(ctx context.Context, js jetstream.JetStream, maxBytes int64) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{"logs.*", "metrics.*"},
		// Messages leave when the writer acks them. With the default limits retention and
		// discard: new, a full stream would stay full forever. Can't be changed later.
		Retention:  jetstream.WorkQueuePolicy,
		Storage:    jetstream.FileStorage,
		MaxBytes:   maxBytes,
		Discard:    jetstream.DiscardNew,
		Duplicates: 2 * time.Minute, // dedup window for Idempotency-Key retries
	})
	return err
}

// streamDepth is NaN, not 0, when NATS can't say.
func streamDepth(js jetstream.JetStream) float64 {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, err := js.Stream(ctx, streamName)
	if err != nil {
		return math.NaN()
	}
	return float64(s.CachedInfo().State.Msgs)
}

func connectNATS(url string) (*nats.Conn, error) {
	return nats.Connect(url,
		nats.Name("ingest"),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectBufSize(-1), // fail fast while disconnected instead of buffering in memory
	)
}

// keyCache maps sha256(api key) to a project ID, including misses, for keyCacheTTL.
type keyCache struct {
	pool *pgxpool.Pool
	mu   sync.Mutex
	m    map[[32]byte]cachedKey
}

type cachedKey struct {
	projectID int64 // 0 = no such key
	expires   time.Time
}

func (c *keyCache) project(ctx context.Context, key string) (int64, error) {
	h := sha256.Sum256([]byte(key))
	now := time.Now()
	c.mu.Lock()
	e, ok := c.m[h]
	c.mu.Unlock()
	if ok && now.Before(e.expires) {
		return e.projectID, nil
	}
	var id int64
	ctx, cancel := context.WithTimeout(ctx, lookupTimeout)
	defer cancel()
	err := c.pool.QueryRow(ctx, "SELECT project_id FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL", h[:]).Scan(&id)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, err
	}
	c.mu.Lock()
	if len(c.m) >= 100_000 {
		clear(c.m) // ponytail: crude bound against floods of random keys; an LRU if this ever thrashes
	}
	c.m[h] = cachedKey{id, now.Add(keyCacheTTL)}
	c.mu.Unlock()
	return id, nil
}

type ingester struct {
	keys     *keyCache
	js       jetstream.JetStream
	inFlight atomic.Int64
}

// budget charges one request's decoded bytes to the replica's total while they are read.
// Without it, eight small gzip bodies that inflate to 20 MB each get the process OOM-killed.
// ponytail: shared by all projects, so one can starve the rest; per-project quotas if it does.
type budget struct {
	total *atomic.Int64
	held  int64
	r     io.Reader
}

func (b *budget) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if overErr := b.charge(n); overErr != nil {
		return n, overErr
	}
	return n, err
}

func (b *budget) charge(n int) error {
	b.held += int64(n)
	if b.total.Add(int64(n)) > maxInFlight {
		return errUnavailable
	}
	return nil
}

func (b *budget) release() { b.total.Add(-b.held) }

type rejection struct {
	Line  int    `json:"line"`
	Error string `json:"error"`
}

// handle serves POST /v1/logs and /v1/metrics. kind is "logs" or "metrics".
func (ing *ingester) handle(kind string) handler {
	normalize := normalizers[kind]
	return func(r *http.Request) (int, any) {
		key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || key == "" {
			return 401, map[string]string{"error": "missing Authorization: Bearer <api key>"}
		}
		project, err := ing.keys.project(r.Context(), key)
		if err != nil {
			slog.Error("api key lookup failed", "err", err)
			return 503, map[string]string{"error": errUnavailable.Error()}
		}
		if project == 0 {
			return 401, map[string]string{"error": "invalid api key"}
		}

		idem := r.Header.Get("Idempotency-Key")
		if len(idem) > maxIdemKeyLen {
			return 400, map[string]string{"error": fmt.Sprintf("Idempotency-Key over %d bytes", maxIdemKeyLen)}
		}
		mem := &budget{total: &ing.inFlight}
		defer mem.release() // held until the acks are in
		lines, status, err := readLines(r, mem)
		if err != nil {
			return status, map[string]string{"error": err.Error()}
		}

		now := time.Now()
		subject := kind + "." + strconv.FormatInt(project, 10)
		rejected := []rejection{}
		nRejected := 0
		var msgs []*nats.Msg
		for _, l := range lines {
			data, err := normalize.encode(l.data, now)
			if err != nil {
				if len(rejected) < maxRejections {
					rejected = append(rejected, rejection{l.num, err.Error()})
				}
				nRejected++
				events.WithLabelValues(kind, "rejected").Inc()
				continue
			}
			m := nats.NewMsg(subject)
			m.Data = data
			if idem != "" {
				// A retried batch republishes the same IDs; JetStream drops the duplicates.
				m.Header.Set(jetstream.MsgIDHeader, fmt.Sprintf("%s:%s:%d", subject, idem, l.num))
			}
			msgs = append(msgs, m)
		}
		if len(msgs) == 0 {
			return 400, map[string]any{"error": "no valid events", "rejected": rejected}
		}

		if err := ing.publish(r.Context(), msgs); err != nil {
			events.WithLabelValues(kind, "unavailable").Add(float64(len(msgs)))
			slog.Warn("publish failed", "subject", subject, "err", err)
			return 503, map[string]string{"error": errUnavailable.Error()}
		}
		events.WithLabelValues(kind, "accepted").Add(float64(len(msgs)))
		return 202, map[string]any{"accepted": len(msgs), "rejected_count": nRejected, "rejected": rejected}
	}
}

// publish sends every message and waits for all JetStream acks. Any failure (stream
// full, NATS down, ack timeout) fails the request; the client retries the whole batch,
// and an Idempotency-Key makes that retry duplicate-free.
func (ing *ingester) publish(ctx context.Context, msgs []*nats.Msg) error {
	ctx, cancel := context.WithTimeout(ctx, ackTimeout)
	defer cancel()
	futures := make([]jetstream.PubAckFuture, 0, len(msgs))
	for _, m := range msgs {
		if err := ctx.Err(); err != nil {
			return err // a stall can block each publish, so check the deadline here too
		}
		f, err := ing.js.PublishMsgAsync(m, jetstream.WithStallWait(time.Second))
		if err != nil {
			return err
		}
		futures = append(futures, f)
	}
	for _, f := range futures {
		select {
		case <-f.Ok():
		case err := <-f.Err():
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

type numberedLine struct {
	num  int // 1-based position in the body, for error reports
	data []byte
}

// readLines reads the (optionally gzipped) NDJSON body and returns its non-blank lines.
func readLines(r *http.Request, mem *budget) ([]numberedLine, int, error) {
	var body io.Reader = http.MaxBytesReader(nil, r.Body, maxBodyBytes)
	switch r.Header.Get("Content-Encoding") {
	case "", "identity":
	case "gzip":
		gz, err := gzip.NewReader(body)
		if err != nil {
			return nil, 400, errors.New("invalid gzip body")
		}
		body = io.LimitReader(gz, maxDecodedBytes+1)
	default:
		return nil, 415, errors.New("Content-Encoding must be gzip or absent")
	}
	mem.r = body
	data, err := io.ReadAll(mem)
	var tooBig *http.MaxBytesError
	switch {
	case errors.As(err, &tooBig):
		return nil, 413, fmt.Errorf("body over %d bytes", maxBodyBytes)
	case errors.Is(err, errUnavailable):
		return nil, 503, errUnavailable
	case err != nil:
		return nil, 400, errors.New("could not read body")
	case len(data) > maxDecodedBytes:
		return nil, 413, fmt.Errorf("decompressed body over %d bytes", maxDecodedBytes)
	}
	var lines []numberedLine
	num := 0
	// Not bufio.Scanner: its Bytes() are overwritten by the next Scan.
	for line := range bytes.Lines(data) {
		num++
		if line = bytes.TrimSpace(line); len(line) == 0 {
			continue
		}
		if len(lines) == maxEvents {
			return nil, 413, fmt.Errorf("more than %d events", maxEvents)
		}
		lines = append(lines, numberedLine{num, line})
	}
	if len(lines) == 0 {
		return nil, 400, errors.New("empty body")
	}
	if err := mem.charge(len(lines) * eventOverhead); err != nil {
		return nil, 503, err
	}
	return lines, 0, nil
}
