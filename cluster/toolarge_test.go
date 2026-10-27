package cluster

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/xraph/tether"
)

// requireTooLarge fails t unless err wraps both ErrTooLarge and
// tether.ErrInvalidInput: a caller must not send it again (A-I1).
func requireTooLarge(t *testing.T, what string, err error) {
	t.Helper()

	if !errors.Is(err, ErrTooLarge) || !errors.Is(err, tether.ErrInvalidInput) {
		t.Fatalf("%s: want ErrTooLarge and ErrInvalidInput, got %v", what, err)
	}

	if errors.Is(err, ErrUnavailable) {
		t.Fatalf("%s: a too large write is not an outage: %v", what, err)
	}
}

// A write over the server's max_payload fails at once with an error that
// says it will never succeed, in the log, the retained store and the
// sessions bucket.
func TestWritesOverMaxPayloadRefused(t *testing.T) {
	logs := startLogs(t, 1)
	limit := logs[0].c.MaxPayload()

	if limit <= 0 {
		t.Fatalf("MaxPayload %d", limit)
	}

	big := bytes.Repeat([]byte("x"), int(limit)+1)

	_, _, err := logs[0].Append(tctx(t, 10*time.Second), LogMessage{Tenant: testTenant, Topic: "a/b", Payload: big, QoS: 1}, "")
	requireTooLarge(t, "append", err)

	ret := startRetained(t, 1)[0]
	msg := LogMessage{Payload: bytes.Repeat([]byte("x"), int(limit)*3/4)}

	requireTooLarge(t, "check retained", ret.Check(msg))
	requireTooLarge(t, "put retained", ret.Put(tctx(t, 10*time.Second), testTenant, "a/b", msg, 0))

	if cerr := ret.Check(LogMessage{Payload: []byte("small")}); cerr != nil {
		t.Fatalf("check small: %v", cerr)
	}

	s := startCell(t, 1)[0]
	_, rev, _, err := s.Acquire(tctx(t, 10*time.Second), testTenant, "big", "n1")
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.Update(tctx(t, 10*time.Second), testTenant, "big", rev, &SessionRecord{
		Owner: "n1", Will: &WillRecord{Topic: "w", Payload: big},
	})
	requireTooLarge(t, "session update", err)
}
