package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	maxEventBytes   = 256 << 10 // before and after normalizing; NATS refuses payloads over 1 MB
	maxMessageBytes = 32 << 10  // longer messages are truncated, not rejected
	maxAttrs        = 64
	maxKeyLen       = 128
	maxValueLen     = 4 << 10
	maxServiceLen   = 100
	maxFuture       = time.Hour
	maxAge          = 30 * 24 * time.Hour // matches the ClickHouse TTL
)

// LogEvent and MetricEvent are what goes onto JetStream. ts is unix milliseconds.
type LogEvent struct {
	TS      int64             `json:"ts"`
	Service string            `json:"service"`
	Level   string            `json:"level"`
	Message string            `json:"message"`
	TraceID string            `json:"trace_id,omitempty"`
	Attrs   map[string]string `json:"attrs,omitempty"`
}

type MetricEvent struct {
	TS      int64             `json:"ts"`
	Service string            `json:"service"`
	Name    string            `json:"name"`
	Value   float64           `json:"value"`
	Labels  map[string]string `json:"labels,omitempty"`
}

var levels = map[string]string{
	"trace": "trace", "debug": "debug",
	"info": "info", "information": "info", "notice": "info",
	"warn": "warn", "warning": "warn",
	"error": "error", "err": "error",
	"fatal": "fatal", "critical": "fatal", "crit": "fatal", "panic": "fatal",
}

var (
	metricName = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:.]{0,199}$`)
	traceID    = regexp.MustCompile(`^[0-9a-fA-F]{1,64}$`)
)

func decodeLine(line []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(line))
	d.UseNumber()
	if err := d.Decode(v); err != nil {
		return errors.New("invalid JSON object")
	}
	// More() alone lets a trailing ] or } through.
	if d.Decode(new(json.RawMessage)) != io.EOF {
		return errors.New("more than one JSON value on the line")
	}
	return nil
}

func normalizeLog(line []byte, now time.Time) (LogEvent, error) {
	var in struct {
		TS        any            `json:"ts"`
		Timestamp any            `json:"timestamp"`
		Level     *string        `json:"level"`
		Service   *string        `json:"service"`
		Message   *string        `json:"message"`
		Msg       *string        `json:"msg"`
		TraceID   *string        `json:"trace_id"`
		Attrs     map[string]any `json:"attrs"`
	}
	if err := decodeLine(line, &in); err != nil {
		return LogEvent{}, err
	}
	var e LogEvent
	var err error
	if e.TS, err = normalizeTS(first(in.TS, in.Timestamp), now); err != nil {
		return e, err
	}
	if e.Service, err = normalizeService(in.Service); err != nil {
		return e, err
	}
	e.Level = "info"
	if in.Level != nil {
		var ok bool
		if e.Level, ok = levels[strings.ToLower(strings.TrimSpace(*in.Level))]; !ok {
			return e, fmt.Errorf("unknown level %q", *in.Level)
		}
	}
	msg := in.Message
	if msg == nil {
		msg = in.Msg
	}
	if msg == nil || strings.TrimSpace(*msg) == "" {
		return e, errors.New("message is required")
	}
	e.Message = truncate(*msg, maxMessageBytes)
	if in.TraceID != nil && *in.TraceID != "" {
		if !traceID.MatchString(*in.TraceID) {
			return e, errors.New("trace_id must be hex, at most 64 chars")
		}
		e.TraceID = strings.ToLower(*in.TraceID)
	}
	e.Attrs, err = normalizeMap(in.Attrs)
	return e, err
}

func normalizeMetric(line []byte, now time.Time) (MetricEvent, error) {
	var in struct {
		TS        any            `json:"ts"`
		Timestamp any            `json:"timestamp"`
		Service   *string        `json:"service"`
		Name      string         `json:"name"`
		Value     any            `json:"value"`
		Labels    map[string]any `json:"labels"`
	}
	if err := decodeLine(line, &in); err != nil {
		return MetricEvent{}, err
	}
	var e MetricEvent
	var err error
	if e.TS, err = normalizeTS(first(in.TS, in.Timestamp), now); err != nil {
		return e, err
	}
	if e.Service, err = normalizeService(in.Service); err != nil {
		return e, err
	}
	if !metricName.MatchString(in.Name) {
		return e, errors.New("name must match [a-zA-Z_:][a-zA-Z0-9_:.]*, at most 200 chars")
	}
	e.Name = in.Name
	n, ok := in.Value.(json.Number)
	if !ok {
		return e, errors.New("value must be a number")
	}
	if e.Value, err = n.Float64(); err != nil || math.IsInf(e.Value, 0) || math.IsNaN(e.Value) {
		return e, errors.New("value must be a finite number")
	}
	e.Labels, err = normalizeMap(in.Labels)
	return e, err
}

type normalizer func(line []byte, now time.Time) (any, error)

// normalizers is keyed by kind, which is also the route and the subject prefix.
var normalizers = map[string]normalizer{
	"logs":    func(line []byte, now time.Time) (any, error) { return normalizeLog(line, now) },
	"metrics": func(line []byte, now time.Time) (any, error) { return normalizeMetric(line, now) },
}

// encode turns one NDJSON line into a JetStream payload. Anything too large is rejected
// here as a bad item: at publish it would fail the batch with a 503 on every retry.
func (n normalizer) encode(line []byte, now time.Time) ([]byte, error) {
	errTooLarge := fmt.Errorf("event over %d bytes", maxEventBytes)
	if len(line) > maxEventBytes {
		return nil, errTooLarge
	}
	ev, err := n(line, now)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return nil, err
	}
	if len(data) > maxEventBytes { // JSON escaping can grow a value 6x
		return nil, errTooLarge
	}
	return data, nil
}

func first(a, b any) any {
	if a != nil {
		return a
	}
	return b
}

// normalizeTS accepts RFC 3339 strings or unix numbers in s, ms, µs or ns (guessed by
// magnitude). Missing means "now". Returns unix milliseconds.
func normalizeTS(v any, now time.Time) (int64, error) {
	var t time.Time
	switch x := v.(type) {
	case nil:
		return now.UnixMilli(), nil
	case string:
		var err error
		if t, err = time.Parse(time.RFC3339Nano, x); err != nil {
			return 0, errors.New("ts must be RFC 3339 or a unix number")
		}
	case json.Number:
		f, err := x.Float64()
		if err != nil || f <= 0 {
			return 0, errors.New("ts must be a positive unix number")
		}
		switch {
		case f < 1e11: // seconds until year 5138
			t = time.UnixMilli(int64(f * 1e3))
		case f < 1e14:
			t = time.UnixMilli(int64(f))
		case f < 1e17:
			t = time.UnixMicro(int64(f))
		default:
			t = time.Unix(0, int64(f))
		}
	default:
		return 0, errors.New("ts must be RFC 3339 or a unix number")
	}
	if t.After(now.Add(maxFuture)) {
		return 0, errors.New("ts is more than 1 h in the future")
	}
	if t.Before(now.Add(-maxAge)) {
		return 0, errors.New("ts is older than the 30-day retention")
	}
	return t.UnixMilli(), nil
}

func normalizeService(s *string) (string, error) {
	if s == nil || strings.TrimSpace(*s) == "" {
		return "", errors.New("service is required")
	}
	v := strings.TrimSpace(*s)
	if len(v) > maxServiceLen || !utf8.ValidString(v) {
		return "", fmt.Errorf("service must be valid UTF-8, at most %d bytes", maxServiceLen)
	}
	return v, nil
}

// normalizeMap flattens attribute values to strings: numbers and bools as written,
// objects and arrays as compact JSON, null dropped.
func normalizeMap(in map[string]any) (map[string]string, error) {
	if len(in) == 0 {
		return nil, nil
	}
	if len(in) > maxAttrs {
		return nil, fmt.Errorf("at most %d attributes", maxAttrs)
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		if k == "" || len(k) > maxKeyLen {
			return nil, fmt.Errorf("attribute keys must be 1-%d bytes", maxKeyLen)
		}
		var s string
		switch x := v.(type) {
		case nil:
			continue
		case string:
			s = x
		case json.Number:
			s = x.String()
		case bool:
			s = fmt.Sprint(x)
		default:
			b, _ := json.Marshal(x)
			s = string(b)
		}
		out[k] = truncate(s, maxValueLen)
	}
	return out, nil
}

// truncate cuts s to at most n bytes without splitting a UTF-8 rune.
func truncate(s string, n int) string {
	s = strings.ToValidUTF8(s, "�")
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
