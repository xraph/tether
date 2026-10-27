package cluster

import (
	"context"
	"errors"
	"testing"
	"time"
)

// JetStream not answering the health check (its meta leader is being
// elected, here modelled by a request that cannot be answered in time) is
// not unhealthy until it has gone on for longer than Lease/2 since it last
// answered, so a readiness probe does not take every node of the cell out
// for an election (D-I2). A connection that is down is reported at once.
func TestHealthyRidesOutElection(t *testing.T) {
	c := startCellConns2(t)

	if err := c.Healthy(tctx(t, 10*time.Second)); err != nil {
		t.Fatalf("healthy cell: %v", err)
	}

	unanswered, cancel := context.WithCancel(context.Background())
	cancel()

	if err := c.Healthy(unanswered); err != nil {
		t.Fatalf("an election shorter than the grace made the node unhealthy: %v", err)
	}

	c.jsSeen.Store(time.Now().Add(-c.jsGrace - time.Second).UnixNano())

	if err := c.Healthy(unanswered); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("jetstream silent past the grace: want ErrUnavailable, got %v", err)
	}

	if err := c.Healthy(tctx(t, 10*time.Second)); err != nil {
		t.Fatalf("jetstream answering again: %v", err)
	}

	c.nc.Close()

	if err := c.Healthy(tctx(t, 10*time.Second)); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("closed connection: want ErrUnavailable, got %v", err)
	}
}

// startCellConns2 is one node's connection to a three-server cell.
func startCellConns2(t *testing.T) *Conn {
	t.Helper()

	conns, _ := startCellConns(t, 1)

	return conns[0]
}
