package main

import (
	"encoding/json"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

type fakeMsg struct {
	jetstream.Msg // only the methods below are called
	subject       string
	data          string
	seq           uint64
}

func (m fakeMsg) Subject() string { return m.subject }
func (m fakeMsg) Data() []byte    { return []byte(m.data) }
func (m fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: m.seq}}, nil
}

func TestRows(t *testing.T) {
	got, err := rows([]jetstream.Msg{
		fakeMsg{subject: "logs.7", data: `{"ts":1,"service":"a","message":"x"}`, seq: 42},
		fakeMsg{subject: "metrics.12", data: `{}`, seq: 43},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"project_id":7,"id":42,"ts":1,"service":"a","message":"x"}` + "\n" +
		`{"project_id":12,"id":43}` + "\n"
	if string(got) != want {
		t.Fatalf("got\n%s\nwant\n%s", got, want)
	}
	for _, line := range []string{`{"project_id":7,"id":42,"ts":1,"service":"a","message":"x"}`, `{"project_id":12,"id":43}`} {
		if !json.Valid([]byte(line)) {
			t.Errorf("invalid JSON: %s", line)
		}
	}

	for _, bad := range []fakeMsg{{subject: "logs", data: `{}`}, {subject: "logs.1", data: `[]`}, {subject: "logs.1", data: `{`}} {
		if _, err := rows([]jetstream.Msg{bad}); err == nil {
			t.Errorf("%+v: want error", bad)
		}
	}
}
