package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const casRounds = 1000

// codeWrongLastSeqConstant is nats-server's JSStreamWrongLastSequenceConstantErr
// (10164). nats.go has no constant for it.
const codeWrongLastSeqConstant jetstream.ErrorCode = 10164

// Review Focus 4: two nodes take over the same client at once. Each round, two
// goroutines on connections to different servers CAS the same session key from
// the same revision; exactly one may win, and the loser must get "wrong last
// sequence" (10071), never a silent success.
//
// Two write paths are checked, because Tether uses both: kv.Update (the
// session takeover) and a raw publish to the KV stream with
// Nats-Expected-Last-Subject-Sequence (the path that also carries Nats-TTL).
func TestCASOneWinner(t *testing.T) {
	_, jsA := connect(t, 0)
	_, jsB := connect(t, 1)

	kvA, err := jsA.CreateKeyValue(ctxT(t, 30*time.Second), jetstream.KeyValueConfig{
		Bucket: "CAS", History: 1, Replicas: 3, Storage: jetstream.FileStorage,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { cleanupDelete(t, jsA.DeleteKeyValue, "CAS") })

	kvB, err := jsB.KeyValue(ctxT(t, 10*time.Second), "CAS")
	if err != nil {
		t.Fatal(err)
	}

	type writer func(key string, val []byte, rev uint64) (uint64, error)

	kvWriter := func(kv jetstream.KeyValue) writer {
		return func(key string, val []byte, rev uint64) (uint64, error) {
			return kv.Update(t.Context(), key, val, rev)
		}
	}

	rawWriter := func(js jetstream.JetStream) writer {
		return func(key string, val []byte, rev uint64) (uint64, error) {
			m := nats.NewMsg("$KV.CAS." + key)
			m.Data = val
			m.Header.Set(jetstream.ExpectedLastSubjSeqHeader, fmt.Sprint(rev))

			ack, err := js.PublishMsg(t.Context(), m)
			if err != nil {
				return 0, err
			}

			return ack.Sequence, nil
		}
	}

	cases := []struct {
		name string
		a, b writer
	}{
		{"kv.Update", kvWriter(kvA), kvWriter(kvB)},
		{"raw publish", rawWriter(jsA), rawWriter(jsB)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key := "s.ttnt_x." + map[string]string{"kv.Update": "dev-kv", "raw publish": "dev-raw"}[tc.name]

			rev, err := kvA.Put(t.Context(), key, []byte("init"))
			if err != nil {
				t.Fatal(err)
			}

			var winsA, winsB, both, none int

			loserCodes := map[jetstream.ErrorCode]int{}

			start := time.Now()

			for round := range casRounds {
				var (
					wg         sync.WaitGroup
					revA, revB uint64
					errA, errB error
				)

				gate := make(chan struct{})

				wg.Go(func() {
					<-gate

					revA, errA = tc.a(key, fmt.Appendf(nil, "A%d", round), rev)
				})
				wg.Go(func() {
					<-gate

					revB, errB = tc.b(key, fmt.Appendf(nil, "B%d", round), rev)
				})
				close(gate)
				wg.Wait()

				switch {
				case errA == nil && errB == nil:
					both++
				case errA != nil && errB != nil:
					none++

					t.Logf("round %d: both failed: %v / %v", round, errA, errB)
				case errA == nil:
					winsA++
					loserCodes[apiCode(errB)]++
					rev = revA
				default:
					winsB++
					loserCodes[apiCode(errA)]++
					rev = revB
				}

				if errA != nil && errB != nil {
					// Re-read so the next round starts from the real revision.
					e, err := kvA.Get(t.Context(), key)
					if err != nil {
						t.Fatal(err)
					}

					rev = e.Revision()
				}
			}

			elapsed := time.Since(start)

			// The stored value must belong to the last winner, at the last winning revision.
			e, err := kvB.Get(t.Context(), key)
			if err != nil {
				t.Fatal(err)
			}

			if e.Revision() != rev {
				t.Errorf("stored revision %d, last winner wrote %d", e.Revision(), rev)
			}

			// A stale owner's write fails.
			if _, err := tc.a(key, []byte("stale"), rev-1); apiCode(err) != jetstream.JSErrCodeStreamWrongLastSequence {
				t.Errorf("stale write: got %v, want wrong last sequence", err)
			}

			t.Logf("%d rounds in %v (%.0f rounds/s): A won %d, B won %d, both won %d, neither %d; loser error codes %v",
				casRounds, elapsed.Round(time.Millisecond), float64(casRounds)/elapsed.Seconds(), winsA, winsB, both, none, loserCodes)

			if both != 0 {
				t.Fatalf("%d rounds had two winners", both)
			}

			if none != 0 {
				t.Errorf("%d rounds had no winner", none)
			}

			// The loser gets 10071 ("wrong last sequence: N") when the winner was
			// already applied, or 10164 ("wrong last sequence", no N) when the
			// winner's proposal was still in flight on the stream leader. Both
			// mean "not stored"; anything else would be a new failure mode.
			for code, n := range loserCodes {
				if code != jetstream.JSErrCodeStreamWrongLastSequence && code != codeWrongLastSeqConstant {
					t.Errorf("loser got code %d %d times", code, n)
				}
			}
		})
	}
}

// Two CAS writes on one key without waiting for the first ack: the second
// names the sequence the first will get, but the stream leader refuses any
// expected-sequence write to a subject with a proposal in flight. So a node may
// have at most one outstanding CAS per key.
func TestCASPipelinedOnOneKey(t *testing.T) {
	_, js := connect(t, 0)

	kv, err := js.CreateKeyValue(ctxT(t, 30*time.Second), jetstream.KeyValueConfig{
		Bucket: "CASPIPE", History: 1, Replicas: 3, Storage: jetstream.FileStorage,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { cleanupDelete(t, js.DeleteKeyValue, "CASPIPE") })

	const key = "s.ttnt_x.dev-1"

	rev, err := kv.Put(t.Context(), key, []byte("init"))
	if err != nil {
		t.Fatal(err)
	}

	var rejected, ok int

	codes := map[jetstream.ErrorCode]int{}

	const rounds = 200

	for range rounds {
		futs := make([]jetstream.PubAckFuture, 2)

		for i := range futs {
			m := nats.NewMsg("$KV.CASPIPE." + key)
			m.Data = []byte("v")
			m.Header.Set(jetstream.ExpectedLastSubjSeqHeader, fmt.Sprint(rev+uint64(i)))

			f, err := js.PublishMsgAsync(m)
			if err != nil {
				t.Fatal(err)
			}

			futs[i] = f
		}

		var last uint64

		for i, f := range futs {
			select {
			case a := <-f.Ok():
				ok++
				last = a.Sequence
			case err := <-f.Err():
				if i == 0 {
					t.Fatalf("first CAS of the pair failed: %v", err)
				}

				rejected++
				codes[apiCode(err)]++
			}
		}

		rev = last
	}

	t.Logf("%d pairs: %d stored, %d second writes refused, codes %v", rounds, ok, rejected, codes)
}
