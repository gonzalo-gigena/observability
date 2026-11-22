package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var now = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

func TestNormalizeLog(t *testing.T) {
	ms := now.UnixMilli()
	tests := []struct {
		name, in string
		want     LogEvent
		err      string
	}{
		{"minimal, defaults ts and level", `{"service":"api","message":"hi"}`,
			LogEvent{TS: ms, Service: "api", Level: "info", Message: "hi"}, ""},
		{"rfc3339 ts, level alias, msg alias, trace id lowercased", `{"ts":"2026-11-21T11:59:59.5Z","service":" api ","level":"WARNING","msg":"x","trace_id":"ABCDEF"}`,
			LogEvent{TS: ms - 500, Service: "api", Level: "warn", Message: "x", TraceID: "abcdef"}, ""},
		{"unix seconds", `{"ts":1795262400,"service":"a","message":"m"}`, LogEvent{TS: 1795262400000, Service: "a", Level: "info", Message: "m"}, ""},
		{"unix ms", `{"ts":1795262400123,"service":"a","message":"m"}`, LogEvent{TS: 1795262400123, Service: "a", Level: "info", Message: "m"}, ""},
		{"unix ns", `{"timestamp":1795262400123456789,"service":"a","message":"m"}`, LogEvent{TS: 1795262400123, Service: "a", Level: "info", Message: "m"}, ""},
		{"attrs flattened, null dropped", `{"service":"a","message":"m","attrs":{"status":500,"ok":false,"region":"eu","obj":{"k":[1,2]},"gone":null}}`,
			LogEvent{TS: ms, Service: "a", Level: "info", Message: "m", Attrs: map[string]string{"status": "500", "ok": "false", "region": "eu", "obj": `{"k":[1,2]}`}}, ""},
		{"long message truncated", `{"service":"a","message":"` + strings.Repeat("é", maxMessageBytes) + `"}`,
			LogEvent{TS: ms, Service: "a", Level: "info", Message: strings.Repeat("é", maxMessageBytes/2)}, ""},

		{"not json", `hello`, LogEvent{}, "invalid JSON"},
		{"array", `[1]`, LogEvent{}, "invalid JSON"},
		{"trailing bracket", `{"service":"a","message":"m"}]oops`, LogEvent{}, "more than one"},
		{"two values", `{"service":"a","message":"m"} {}`, LogEvent{}, "more than one"},
		{"missing service", `{"message":"m"}`, LogEvent{}, "service is required"},
		{"service too long", `{"service":"` + strings.Repeat("s", 101) + `","message":"m"}`, LogEvent{}, "service must be"},
		{"missing message", `{"service":"a"}`, LogEvent{}, "message is required"},
		{"blank message", `{"service":"a","message":"  "}`, LogEvent{}, "message is required"},
		{"unknown level", `{"service":"a","message":"m","level":"loud"}`, LogEvent{}, "unknown level"},
		{"bad ts string", `{"ts":"yesterday","service":"a","message":"m"}`, LogEvent{}, "RFC 3339"},
		{"bool ts", `{"ts":true,"service":"a","message":"m"}`, LogEvent{}, "RFC 3339"},
		{"negative ts", `{"ts":-1,"service":"a","message":"m"}`, LogEvent{}, "positive"},
		{"future ts", `{"ts":"2026-11-21T14:00:00Z","service":"a","message":"m"}`, LogEvent{}, "future"},
		{"too old ts", `{"ts":"2026-10-01T00:00:00Z","service":"a","message":"m"}`, LogEvent{}, "retention"},
		{"bad trace id", `{"service":"a","message":"m","trace_id":"not-hex"}`, LogEvent{}, "trace_id"},
		{"empty attr key", `{"service":"a","message":"m","attrs":{"":"x"}}`, LogEvent{}, "attribute keys"},
		{"attrs not an object", `{"service":"a","message":"m","attrs":"x"}`, LogEvent{}, "invalid JSON"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeLog([]byte(tt.in), now)
			if tt.err != "" {
				if err == nil || !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("err = %v, want containing %q", err, tt.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got  %+v\nwant %+v", got, tt.want)
			}
		})
	}
}

func TestNormalizeMetric(t *testing.T) {
	ms := now.UnixMilli()
	tests := []struct {
		name, in string
		want     MetricEvent
		err      string
	}{
		{"ok", `{"service":"api","name":"http.requests_total","value":3,"labels":{"code":200}}`,
			MetricEvent{TS: ms, Service: "api", Name: "http.requests_total", Value: 3, Labels: map[string]string{"code": "200"}}, ""},
		{"float value", `{"service":"api","name":"cpu","value":0.25}`, MetricEvent{TS: ms, Service: "api", Name: "cpu", Value: 0.25}, ""},
		{"missing name", `{"service":"api","value":1}`, MetricEvent{}, "name must"},
		{"bad name", `{"service":"api","name":"1abc","value":1}`, MetricEvent{}, "name must"},
		{"string value", `{"service":"api","name":"x","value":"1"}`, MetricEvent{}, "value must be a number"},
		{"missing value", `{"service":"api","name":"x"}`, MetricEvent{}, "value must be a number"},
		{"overflow value", `{"service":"api","name":"x","value":1e999}`, MetricEvent{}, "finite"},
		{"missing service", `{"name":"x","value":1}`, MetricEvent{}, "service is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeMetric([]byte(tt.in), now)
			if tt.err != "" {
				if err == nil || !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("err = %v, want containing %q", err, tt.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got  %+v\nwant %+v", got, tt.want)
			}
		})
	}
}

func TestReadLines(t *testing.T) {
	gz := func(s string) string {
		var b bytes.Buffer
		w := gzip.NewWriter(&b)
		w.Write([]byte(s))
		w.Close()
		return b.String()
	}
	tests := []struct {
		name, body, encoding string
		lines, status        int
	}{
		{"plain", "{}\n{}\n", "", 2, 0},
		{"gzip", gz("{}\n{}\n{}"), "gzip", 3, 0},
		{"bad gzip", "nope", "gzip", 0, 400},
		{"unsupported encoding", "{}", "br", 0, 415},
		{"empty", "", "", 0, 400},
		{"only blank lines", "\n \n\r\n", "", 0, 400},
		{"blank lines are not events", "\n" + strings.Repeat("{}\n\n", maxEvents), "", maxEvents, 0},
		{"exactly max events", strings.Repeat("{}\n", maxEvents), "", maxEvents, 0},
		{"too many events", strings.Repeat("{}\n", maxEvents+1), "", 0, 413},
		{"body too big", strings.Repeat("x", maxBodyBytes+1), "", 0, 413},
		{"gzip bomb", gz(strings.Repeat("x", maxDecodedBytes+1)), "gzip", 0, 413},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/v1/logs", strings.NewReader(tt.body))
			if tt.encoding != "" {
				r.Header.Set("Content-Encoding", tt.encoding)
			}
			lines, status, err := readLines(r, &budget{total: new(atomic.Int64)})
			if status != tt.status || len(lines) != tt.lines {
				t.Fatalf("got %d lines, status %d (%v); want %d lines, status %d", len(lines), status, err, tt.lines, tt.status)
			}
		})
	}
}

// Enough bytes to outgrow any reader's internal buffer.
func TestReadLinesKeepsContent(t *testing.T) {
	var b strings.Builder
	for i := range 2000 {
		fmt.Fprintf(&b, "{\"message\":\"event-%05d\"}\r\n\n", i)
	}
	lines, _, err := readLines(httptest.NewRequest("POST", "/v1/logs", strings.NewReader(b.String())), &budget{total: new(atomic.Int64)})
	if err != nil || len(lines) != 2000 {
		t.Fatalf("got %d lines, err %v", len(lines), err)
	}
	for i, l := range lines {
		if want := fmt.Sprintf(`{"message":"event-%05d"}`, i); l.num != 2*i+1 || string(l.data) != want {
			t.Fatalf("line %d: got %q, want %q", l.num, l.data, want)
		}
	}
}

func TestEncode(t *testing.T) {
	var attrs []string
	for i := range maxAttrs {
		attrs = append(attrs, fmt.Sprintf(`"k%d":%q`, i, strings.Repeat("<", maxValueLen)))
	}
	tests := []struct {
		name, kind, in, want, err string
	}{
		{"log", "logs", `{"service":"a","message":"m"}`, fmt.Sprintf(`{"ts":%d,"service":"a","level":"info","message":"m"}`, now.UnixMilli()), ""},
		{"metric", "metrics", `{"service":"a","name":"x","value":0}`, fmt.Sprintf(`{"ts":%d,"service":"a","name":"x","value":0}`, now.UnixMilli()), ""},
		{"line too large", "logs", strings.Repeat("x", maxEventBytes+1), "", "event over"},
		{"too large once escaped", "logs", `{"service":"a","message":"m","attrs":{` + strings.Join(attrs[:20], ",") + `}}`, "", "event over"},
		{"bad item", "metrics", `{"service":"a"}`, "", "name must"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizers[tt.kind].encode([]byte(tt.in), now)
			if tt.err != "" {
				if err == nil || !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("err = %v, want containing %q", err, tt.err)
				}
				return
			}
			if err != nil || string(got) != tt.want {
				t.Fatalf("got %s (%v), want %s", got, err, tt.want)
			}
		})
	}
}

func TestReadLinesBudget(t *testing.T) {
	var total atomic.Int64
	total.Store(maxInFlight - 10) // other requests in flight
	mem := &budget{total: &total}
	_, status, _ := readLines(httptest.NewRequest("POST", "/v1/logs", strings.NewReader(strings.Repeat("x", 11))), mem)
	if status != 503 {
		t.Fatalf("status %d, want 503", status)
	}
	mem.release()
	if got := total.Load(); got != maxInFlight-10 {
		t.Fatalf("release left %d bytes charged", got-(maxInFlight-10))
	}
}
