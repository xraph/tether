package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The tests share one 3-server JetStream cluster of child processes, started
// once in TestMain and stopped (store dirs removed) before the binary exits.
var testCluster *Cluster

func TestMain(m *testing.M) {
	MaybeRunChild()

	fmt.Printf("machine: %s; load at start: %s\n", machine(), loadavg())

	cl, err := StartCluster(3)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start cluster:", err)
		os.Exit(1)
	}

	testCluster = cl
	code := m.Run()

	cl.Stop()
	fmt.Printf("load at end: %s\n", loadavg())
	os.Exit(code)
}

// connect opens a connection to server i and returns it with a JetStream handle.
func connect(t *testing.T, i int) (*nats.Conn, jetstream.JetStream) {
	t.Helper()

	nc, err := nats.Connect(testCluster.URLs()[i], nats.Name(fmt.Sprintf("test-n%d", i)))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	return nc, js
}

func ctxT(t *testing.T, d time.Duration) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), d)
	t.Cleanup(cancel)

	return ctx
}

// apiCode returns the JetStream API error code inside err, or 0.
func apiCode(err error) jetstream.ErrorCode {
	var api *jetstream.APIError
	if asAPI(err, &api) {
		return api.ErrorCode
	}

	return 0
}

// cleanupDelete deletes a stream or bucket at the end of a test. t.Context is
// already cancelled when cleanups run, so it uses its own context; without
// this, -count=N reuses the previous iteration's bucket and its data.
func cleanupDelete(t *testing.T, del func(context.Context, string) error, name string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := del(ctx, name); err != nil {
		t.Errorf("delete %s: %v", name, err)
	}
}
