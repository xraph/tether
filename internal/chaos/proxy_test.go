package chaos

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// echoServer accepts connections and writes back what it reads.
func echoServer(t *testing.T) string {
	t.Helper()

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}

			go func() {
				defer c.Close()

				_, _ = io.Copy(c, c)
			}()
		}
	}()

	return ln.Addr().String()
}

func newProxy(t *testing.T) *Proxy {
	t.Helper()

	p, err := NewProxy(echoServer(t))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(p.Close)

	return p
}

func dial(t *testing.T, p *Proxy) net.Conn {
	t.Helper()

	c, err := dialProxy(t, p.Addr(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = c.Close() })

	return c
}

// roundTrip writes msg and reads it back within d.
func roundTrip(c net.Conn, msg string, d time.Duration) error {
	if _, err := c.Write([]byte(msg)); err != nil {
		return err
	}

	_ = c.SetReadDeadline(time.Now().Add(d))

	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(c, buf); err != nil {
		return err
	}

	if string(buf) != msg {
		return errors.New("echo mismatch: " + string(buf))
	}

	return nil
}

func isTimeout(err error) bool {
	var ne net.Error

	return errors.As(err, &ne) && ne.Timeout()
}

func TestPassRoundTrips(t *testing.T) {
	p := newProxy(t)
	c := dial(t, p)

	for _, msg := range []string{"hello", "world", string(make([]byte, 100_000))} {
		if err := roundTrip(c, msg, 5*time.Second); err != nil {
			t.Fatalf("round trip of %d bytes: %v", len(msg), err)
		}
	}
}

func TestDelayAddsLatency(t *testing.T) {
	p := newProxy(t)
	c := dial(t, p)

	p.SetDelay(ClientToServer, 150*time.Millisecond)
	p.SetMode(ClientToServer, Delay)

	start := time.Now()

	if err := roundTrip(c, "ping", 5*time.Second); err != nil {
		t.Fatal(err)
	}

	if got := time.Since(start); got < 150*time.Millisecond {
		t.Fatalf("round trip took %v, want at least 150ms", got)
	}

	// The other direction is still plain: back to pass, fast again.
	p.SetMode(Both, Pass)

	start = time.Now()

	if err := roundTrip(c, "ping", 5*time.Second); err != nil {
		t.Fatal(err)
	}

	if got := time.Since(start); got > 100*time.Millisecond {
		t.Fatalf("round trip after pass took %v", got)
	}
}

func TestBlackholeStallsWithoutClosing(t *testing.T) {
	p := newProxy(t)
	c := dial(t, p)

	if err := roundTrip(c, "before", 2*time.Second); err != nil {
		t.Fatal(err)
	}

	p.SetMode(ServerToClient, Blackhole)

	// The echo is held back: a timeout, not EOF and not a reset.
	err := roundTrip(c, "held", 300*time.Millisecond)
	if !isTimeout(err) {
		t.Fatalf("round trip during blackhole = %v, want a timeout", err)
	}

	if p.Conns() != 1 {
		t.Fatalf("proxy holds %d connections, want 1", p.Conns())
	}

	// A connection accepted during the blackhole is accepted and silent.
	c2 := dial(t, p)

	err = roundTrip(c2, "new", 300*time.Millisecond)
	if !isTimeout(err) {
		t.Fatalf("new connection during blackhole = %v, want a timeout", err)
	}

	// Healing delivers what was held.
	p.SetMode(Both, Pass)

	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))

	buf := make([]byte, len("held"))
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "held" {
		t.Fatalf("after pass read %q, %v; want the held echo", buf, err)
	}

	_ = c2.SetReadDeadline(time.Now().Add(2 * time.Second))

	buf = make([]byte, len("new"))
	if _, err := io.ReadFull(c2, buf); err != nil || string(buf) != "new" {
		t.Fatalf("second connection read %q, %v; want its held echo", buf, err)
	}

	if err := roundTrip(c2, "again", 2*time.Second); err != nil {
		t.Fatalf("second connection after healing: %v", err)
	}
}

func TestBlackholeClientToServer(t *testing.T) {
	p := newProxy(t)
	c := dial(t, p)

	p.SetMode(ClientToServer, Blackhole)

	if err := roundTrip(c, "lost", 300*time.Millisecond); !isTimeout(err) {
		t.Fatalf("got %v, want a timeout", err)
	}

	p.SetMode(ClientToServer, Pass)

	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))

	buf := make([]byte, 4)
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "lost" {
		t.Fatalf("read %q, %v after healing", buf, err)
	}
}

func TestResetClosesExistingAndNew(t *testing.T) {
	p := newProxy(t)
	c := dial(t, p)

	if err := roundTrip(c, "x", 2*time.Second); err != nil {
		t.Fatal(err)
	}

	p.SetMode(Both, Reset)

	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))

	if _, err := c.Read(make([]byte, 1)); err == nil || isTimeout(err) {
		t.Fatalf("read after reset = %v, want a close or reset", err)
	}

	expectReset(t, p)
}

// expectReset dials during Reset. The connect may itself fail with a reset on
// loopback; otherwise the first read must fail without timing out.
func expectReset(t *testing.T, p *Proxy) {
	t.Helper()

	c, err := dialProxy(t, p.Addr(), 2*time.Second)
	if err != nil {
		return
	}

	defer c.Close()

	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))

	if _, err := c.Read(make([]byte, 1)); err == nil || isTimeout(err) {
		t.Fatalf("new connection read = %v, want a close or reset", err)
	}
}

func TestPassAgainAfterReset(t *testing.T) {
	p := newProxy(t)

	p.SetMode(Both, Reset)

	expectReset(t, p)

	p.SetMode(Both, Pass)

	c2 := dial(t, p)

	if err := roundTrip(c2, "alive", 2*time.Second); err != nil {
		t.Fatalf("round trip after switching back to pass: %v", err)
	}
}

func TestCloseDropsConnections(t *testing.T) {
	p := newProxy(t)
	c := dial(t, p)

	if err := roundTrip(c, "x", 2*time.Second); err != nil {
		t.Fatal(err)
	}

	p.Close()
	p.Close() // idempotent

	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))

	if _, err := c.Read(make([]byte, 1)); err == nil || isTimeout(err) {
		t.Fatalf("read after Close = %v, want a close or reset", err)
	}

	if _, err := dialProxy(t, p.Addr(), 300*time.Millisecond); err == nil {
		t.Fatal("proxy still accepts after Close")
	}
}

func dialProxy(t *testing.T, addr string, timeout time.Duration) (net.Conn, error) {
	t.Helper()

	d := net.Dialer{Timeout: timeout}

	return d.DialContext(t.Context(), "tcp", addr)
}
