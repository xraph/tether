package cluster

import (
	"hash/fnv"
	"sync"
	"time"
)

const (
	// dedupWindow is how long a Tether-Id is remembered. A message that arrives
	// twice because two of a node's subscriptions match it arrives within
	// milliseconds, so 2 s is generous.
	dedupWindow = 2 * time.Second

	// dedupMaxEntries caps the ids remembered at once, across all shards. Under
	// steady traffic the window bounds the table (rate x 2 s); the cap only
	// matters in a burst, where the oldest ids are forgotten early.
	dedupMaxEntries = 1 << 18

	dedupShardCount = 16
)

// dedup remembers recently seen message ids so a message that reaches a node
// through two overlapping subscriptions is delivered once. It has the shape of
// forge extensions/streaming/dedup.go, a sharded table of id to first-seen time,
// with one change: each shard also keeps its ids in arrival order, so expired
// ids are dropped from the front on every insert and no sweeper goroutine is
// needed. Memory is bounded by the window and by dedupMaxEntries.
type dedup struct {
	shards   [dedupShardCount]dedupShard
	ttl      time.Duration
	perShard int
	now      func() time.Time
}

type dedupShard struct {
	mu   sync.Mutex
	seen map[string]struct{}
	// order holds the ids of seen, oldest first, from index head on.
	order []dedupEntry
	head  int
}

type dedupEntry struct {
	id string
	at time.Time
}

func newDedup(maxEntries int, ttl time.Duration) *dedup {
	d := &dedup{ttl: ttl, perShard: max(maxEntries/dedupShardCount, 1), now: time.Now}

	for i := range d.shards {
		d.shards[i].seen = make(map[string]struct{})
	}

	return d
}

func (d *dedup) shard(id string) *dedupShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))

	return &d.shards[h.Sum32()%dedupShardCount]
}

// duplicate records id and reports whether it was already seen within the
// window. An empty id is never a duplicate.
func (d *dedup) duplicate(id string) bool {
	if id == "" {
		return false
	}

	s := d.shard(id)
	now := d.now()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.expire(now, d.ttl, d.perShard)

	if _, ok := s.seen[id]; ok {
		return true
	}

	s.seen[id] = struct{}{}
	s.order = append(s.order, dedupEntry{id: id, at: now})

	return false
}

// expire drops ids older than ttl and, if the shard is still full, the oldest
// ones until there is room for one more.
func (s *dedupShard) expire(now time.Time, ttl time.Duration, limit int) {
	for s.head < len(s.order) {
		e := s.order[s.head]
		if now.Sub(e.at) < ttl && len(s.seen) < limit {
			break
		}

		delete(s.seen, e.id)

		s.order[s.head] = dedupEntry{}
		s.head++
	}

	// Reclaim the consumed prefix once it is at least half the slice.
	if s.head > 64 && s.head*2 >= len(s.order) {
		n := copy(s.order, s.order[s.head:])

		clear(s.order[n:])

		s.order = s.order[:n]
		s.head = 0
	}
}

// size is the number of ids currently remembered, for tests.
func (d *dedup) size() int {
	n := 0

	for i := range d.shards {
		s := &d.shards[i]

		s.mu.Lock()
		n += len(s.seen)
		s.mu.Unlock()
	}

	return n
}
