package main

import (
	"hash/fnv"
	"sync"
	"time"
)

// dedup borrows the shape of forge extensions/streaming/dedup.go: a sharded map
// of message ID to first-seen time, entries older than the TTL count as new, and
// a shard over its share of maxSize drops expired entries, then half of what is
// left. The background sweep is left out; eviction on insert is enough here.
const dedupShards = 16

type dedup struct {
	shards  [dedupShards]dedupShard
	maxSize int
	ttl     time.Duration
}

type dedupShard struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

func newDedup(maxSize int, ttl time.Duration) *dedup {
	d := &dedup{maxSize: maxSize, ttl: ttl}

	per := max(maxSize/dedupShards, 64)
	for i := range d.shards {
		d.shards[i].seen = make(map[string]time.Time, per)
	}

	return d
}

// IsDuplicate records id and reports whether it was already seen within the TTL.
func (d *dedup) IsDuplicate(id string) bool {
	if id == "" {
		return false
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	sh := &d.shards[h.Sum32()%dedupShards]

	sh.mu.Lock()
	defer sh.mu.Unlock()

	now := time.Now()
	if at, ok := sh.seen[id]; ok && now.Sub(at) < d.ttl {
		return true
	}

	if len(sh.seen) >= d.maxSize/dedupShards {
		for k, at := range sh.seen {
			if now.Sub(at) >= d.ttl {
				delete(sh.seen, k)
			}
		}

		n, target := 0, len(sh.seen)/2
		for k := range sh.seen {
			if n >= target {
				break
			}

			delete(sh.seen, k)
			n++
		}
	}

	sh.seen[id] = now

	return false
}
