package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/xraph/tether"
	"github.com/xraph/tether/subject"
)

const (
	// opTimeout bounds a session operation whose context has no deadline.
	opTimeout = 10 * time.Second

	// kvOpHeader is the header nats.go puts on KV deletes and purges.
	kvOpHeader = "KV-Operation"
	kvOpDel    = "DEL"
	kvOpPurge  = "PURGE"
)

// SessionRecord is one MQTT session as the cell stores it: the value of one key
// in the sessions bucket. Owner and Epoch say who runs the session; the rest is
// what a node needs to resume it.
type SessionRecord struct {
	// Owner is the ID of the node that runs the session.
	Owner string `json:"owner"`
	// Epoch counts the takeovers of the session. Acquire raises it by one.
	Epoch uint64 `json:"epoch"`
	// Clean is the clean start flag of the connection that created the record.
	Clean bool `json:"clean,omitempty"`
	// ProtocolVersion is the MQTT protocol level of the last connection.
	ProtocolVersion byte `json:"protocol_version,omitempty"`
	// Username and DeviceID identify who connected.
	Username string `json:"username,omitempty"`
	DeviceID string `json:"device_id,omitempty"`
	// ExpirySeconds is the session expiry interval.
	ExpirySeconds uint32 `json:"expiry_seconds,omitempty"`
	// Subscriptions is the session's subscriptions.
	Subscriptions []SubRecord `json:"subscriptions,omitempty"`
	// Cursor is the log sequence the session has delivered up to.
	Cursor uint64 `json:"cursor,omitempty"`
	// Will is the pending will message, nil when none.
	Will *WillRecord `json:"will,omitempty"`
	// QoS2In maps the packet IDs of inbound QoS 2 publishes that are half
	// way through to the log subject they were stored under.
	QoS2In map[uint16]string `json:"qos2_in,omitempty"`
	// QoS2Out maps the packet IDs of outbound QoS 2 deliveries to their state.
	QoS2Out map[uint16]OutRecord `json:"qos2_out,omitempty"`
	// DoneThrough is the log sequence up to which every message of the
	// tenant has been dealt with for the session except those in Pending:
	// delivered and acknowledged, or not matching. Cursor is at most it.
	DoneThrough uint64 `json:"done_through,omitempty"`
	// Pending lists the log sequences of the outbound QoS 1 and 2
	// deliveries in flight (bounded by the client's inflight window).
	Pending []uint64 `json:"pending,omitempty"`
	// QoS2Done lists the log sequences above DoneThrough of outbound QoS 2
	// deliveries the client has completed, so a node that resumes the session
	// does not send them again. Their packet IDs may be in use again already.
	QoS2Done []uint64 `json:"qos2_done,omitempty"`
	// DisconnectedAt is when the session lost its connection, zero while
	// connected.
	DisconnectedAt time.Time `json:"disconnected_at,omitzero"`
}

// SubRecord is one subscription of a session.
type SubRecord struct {
	Filter            string `json:"filter"`
	QoS               byte   `json:"qos"`
	NoLocal           bool   `json:"no_local,omitempty"`
	RetainAsPublished bool   `json:"retain_as_published,omitempty"`
	RetainHandling    byte   `json:"retain_handling,omitempty"`
	// Identifier is the MQTT 5 subscription identifier, 0 for none.
	Identifier int `json:"identifier,omitempty"`
}

// WillRecord is a stored will message.
type WillRecord struct {
	Topic   string `json:"topic"`
	Payload []byte `json:"payload,omitempty"`
	QoS     byte   `json:"qos,omitempty"`
	Retain  bool   `json:"retain,omitempty"`
	// DelaySeconds is the will delay interval.
	DelaySeconds uint32 `json:"delay_seconds,omitempty"`
	Props        Props  `json:"props,omitzero"`
}

// OutRecord is the state of one outbound QoS 2 delivery: the log sequence of
// the message and where the handshake stands.
//
// A shared subscription's QoS 2 delivery has Seq 0 (the session's cursor
// plays no part in it) and its message's log sequence in Shared, so a resume
// sends that message again under the same packet ID (or its PUBREL) and no
// other member of the group ever gets it [MQTT-4.8.2-4], [MQTT-4.8.2-5].
type OutRecord struct {
	Seq    uint64 `json:"seq"`
	State  byte   `json:"state"`
	Shared uint64 `json:"shared,omitempty"`
}

// Sessions stores MQTT sessions in the sessions bucket, one key per session,
// and every write is a compare-and-set on the key's revision.
//
// A write that loses returns an error wrapping ErrLostCAS. The caller decides
// what that means: for a node it usually means another node took the session
// over. Sessions never retries a lost compare-and-set on its own, with one
// exception named on Acquire.
//
// A node has at most one write outstanding per key. JetStream refuses an
// expected-sequence write on a subject that has another write in flight, even
// when the expected value is right, so Sessions serializes the writes of one
// key with a per-key lock. Different keys run in parallel.
//
// Reads go to the stream leader, so a node sees its own writes.
//
// It is safe for concurrent use.
type Sessions struct {
	c      *Conn
	bucket string
	h      atomic.Pointer[sessionHandles]

	keys keyedMutex

	wmu      sync.Mutex
	watchers []*ownedWatch

	// afterRead, when set by a test, runs after the first read of an Acquire.
	afterRead func()
}

// sessionHandles is what Sessions looks up once per process.
type sessionHandles struct {
	kv     jetstream.KeyValue
	prefix string // "$KV.<bucket>.", the subject of a key's messages
	getMsg string // the stream's MSG.GET API subject
}

// NewSessions returns the session store of the cell cfg describes, over c. The
// bucket must exist: call Provision first.
func NewSessions(c *Conn, cfg Config) *Sessions {
	return &Sessions{c: c, bucket: cfg.SessionsBucket()}
}

func (s *Sessions) handles(ctx context.Context) (*sessionHandles, error) {
	if h := s.h.Load(); h != nil {
		return h, nil
	}

	kv, err := s.c.js.KeyValue(ctx, s.bucket)
	if err != nil {
		return nil, s.wrapErr("open sessions bucket", err)
	}

	h := &sessionHandles{
		kv:     kv,
		prefix: "$KV." + s.bucket + ".",
		getMsg: "$JS.API.STREAM.MSG.GET.KV_" + s.bucket,
	}
	s.h.CompareAndSwap(nil, h)

	return s.h.Load(), nil
}

// sessionKey checks the tenant and client ID and returns the bucket key. It
// never returns "".
func sessionKey(tenant, clientID string) (string, error) {
	if err := subject.CheckClientID(clientID); err != nil {
		return "", fmt.Errorf("cluster: session key: %w", err)
	}

	key := subject.SessionKey(tenant, clientID)
	if key == "" {
		return "", fmt.Errorf("cluster: session key: empty tenant: %w", tether.ErrInvalidInput)
	}

	return key, nil
}

// splitSessionKey is the inverse of sessionKey.
func splitSessionKey(key string) (tenant, clientID string, err error) {
	t, c, ok := strings.Cut(key, ".")
	if !ok {
		return "", "", fmt.Errorf("cluster: session key %q has no separator: %w", key, tether.ErrInvalidInput)
	}

	if tenant, err = subject.DecodeLevel(t); err != nil {
		return "", "", fmt.Errorf("cluster: session key %q: %w", key, err)
	}

	if clientID, err = subject.DecodeLevel(c); err != nil {
		return "", "", fmt.Errorf("cluster: session key %q: %w", key, err)
	}

	return tenant, clientID, nil
}

func (s *Sessions) wrapErr(op string, err error) error {
	switch {
	case isLostCAS(err):
		return fmt.Errorf("cluster: %s: %w: %w", op, ErrLostCAS, err)
	case isUnavailable(err):
		return fmt.Errorf("cluster: %s: %w: %w", op, ErrUnavailable, err)
	default:
		return fmt.Errorf("cluster: %s: %w", op, err)
	}
}

// opContext gives ctx the default deadline when it has none.
func opContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, opTimeout)
}

type sessionState int

const (
	// stateAbsent means the key has no message at all: a create takes
	// revision 0.
	stateAbsent sessionState = iota
	// stateMarker means the key's last message is a limit marker or a purge or
	// delete marker. The key reads as absent, but a write has to name the
	// marker's revision until the marker itself expires.
	stateMarker
	// stateLive means the key holds a session.
	stateLive
)

type sessionRead struct {
	state sessionState
	rev   uint64 // the last message's sequence, 0 when absent
	rec   *SessionRecord
}

// msgGetRequest and msgGetResponse are the JetStream API's non-direct "get
// message" exchange. The stream leader answers it, where a direct get may come
// from a replica that has not applied the latest write yet.
type msgGetRequest struct {
	LastBySubject string `json:"last_by_subj"`
}

type msgGetResponse struct {
	Error   *jetstream.APIError `json:"error"`
	Message *struct {
		Subject  string `json:"subject"`
		Sequence uint64 `json:"seq"`
		Data     []byte `json:"data"`
		Headers  []byte `json:"hdrs"`
	} `json:"message"`
}

// read returns the state of key from the stream leader.
func (s *Sessions) read(ctx context.Context, h *sessionHandles, key string) (sessionRead, error) {
	body, err := json.Marshal(msgGetRequest{LastBySubject: h.prefix + key})
	if err != nil {
		return sessionRead{}, fmt.Errorf("cluster: read session: %w", err)
	}

	reply, err := s.c.nc.RequestWithContext(ctx, h.getMsg, body)
	if err != nil {
		return sessionRead{}, s.wrapErr("read session", err)
	}

	var resp msgGetResponse
	if err = json.Unmarshal(reply.Data, &resp); err != nil {
		return sessionRead{}, fmt.Errorf("cluster: read session: bad reply: %w", err)
	}

	if resp.Error != nil {
		if resp.Error.ErrorCode == jetstream.JSErrCodeMessageNotFound {
			return sessionRead{state: stateAbsent}, nil
		}

		return sessionRead{}, s.wrapErr("read session", resp.Error)
	}

	if resp.Message == nil {
		return sessionRead{}, errors.New("cluster: read session: empty reply")
	}

	m := resp.Message

	var hdr nats.Header

	if len(m.Headers) > 0 {
		if hdr, err = nats.DecodeHeadersMsg(m.Headers); err != nil {
			return sessionRead{}, fmt.Errorf("cluster: read session: bad headers: %w", err)
		}
	}

	if isMarker(hdr) {
		return sessionRead{state: stateMarker, rev: m.Sequence}, nil
	}

	rec, err := decodeSession(m.Data)
	if err != nil {
		return sessionRead{}, err
	}

	return sessionRead{state: stateLive, rev: m.Sequence, rec: rec}, nil
}

// isMarker reports whether a message's headers mark a delete, a purge or a
// limit marker.
func isMarker(h nats.Header) bool {
	if len(h) == 0 {
		return false
	}

	if op := h.Get(kvOpHeader); op == kvOpDel || op == kvOpPurge {
		return true
	}

	return h.Get(jetstream.MarkerReasonHeader) != ""
}

func decodeSession(data []byte) (*SessionRecord, error) {
	rec := new(SessionRecord)
	if err := json.Unmarshal(data, rec); err != nil {
		return nil, fmt.Errorf("cluster: decode session record: %w", err)
	}

	return rec, nil
}

// publish writes one message to key's subject as a compare-and-set against
// expect (0 for a create).
func (s *Sessions) publish(ctx context.Context, h *sessionHandles, key string, data []byte, expect uint64, ttl time.Duration, extra nats.Header) (uint64, error) {
	msg := nats.NewMsg(h.prefix + key)
	msg.Data = data

	for k, v := range extra {
		msg.Header[k] = v
	}

	msg.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(expect, 10))

	if ttl > 0 {
		msg.Header.Set(jetstream.MsgTTLHeader, ttlHeader(ttl))
	}

	ack, err := s.c.js.PublishMsg(ctx, msg)
	if err != nil {
		return 0, err
	}

	return ack.Sequence, nil
}

// ttlHeader formats a Nats-TTL value: whole seconds, rounded up, at least one.
// The server refuses anything under a second.
func ttlHeader(ttl time.Duration) string {
	secs := max(1, int64((ttl+time.Second-1)/time.Second))

	return strconv.FormatInt(secs, 10) + "s"
}

// write stores rec on key as a compare-and-set and tells the watchers about it.
func (s *Sessions) write(ctx context.Context, h *sessionHandles, key string, rec *SessionRecord, expect uint64, ttl time.Duration) (uint64, error) {
	data, err := json.Marshal(rec)
	if err != nil {
		return 0, fmt.Errorf("cluster: encode session record: %w", err)
	}

	ws := s.watcherSnapshot()
	for _, w := range ws {
		w.begin(key)
	}

	rev, err := s.publish(ctx, h, key, data, expect, ttl, nil)

	for _, w := range ws {
		w.end(key, rev, rec, false, err == nil)
	}

	return rev, err
}

// Acquire takes the session over for nodeID. It reads the key, then
// compare-and-sets the record it read with Owner = nodeID and the epoch raised
// by one. An absent key is created, at epoch 1.
//
// It returns the record as it was before the takeover (subscriptions, cursor,
// will, QoS 2 state) with the new owner and epoch, the key's new revision, and
// whether a session existed. A key that holds only a limit marker, left by an
// expired session, counts as absent; the create names the marker's revision,
// because JetStream refuses revision 0 while a marker lives.
//
// If the write loses, Acquire returns ErrLostCAS: another writer got in
// between the read and the write. The one exception is a create that loses
// against a key that was read as absent or as a marker, and that reads as
// absent or as a different marker afterwards. The first read was stale about an
// expiring marker, so Acquire tries once more.
func (s *Sessions) Acquire(ctx context.Context, tenant, clientID, nodeID string) (rec *SessionRecord, rev uint64, existed bool, err error) {
	rec, prev, rev, err := s.AcquireWith(ctx, tenant, clientID, nodeID, func(prev *SessionRecord) *SessionRecord {
		if prev == nil {
			return new(SessionRecord)
		}

		c := *prev

		return &c
	})

	return rec, rev, prev != nil, err
}

// AcquireWith is Acquire where build decides what the takeover stores. build
// gets the session as it was (nil when none existed) and returns the record to
// write; AcquireWith then sets its Owner to nodeID and its Epoch to one above
// the previous record's (1 for a new session). build must not change prev, and
// it may run twice (see Acquire for the one retry).
//
// It returns the record it stored, the previous one (nil when no session
// existed) and the key's new revision.
func (s *Sessions) AcquireWith(ctx context.Context, tenant, clientID, nodeID string,
	build func(prev *SessionRecord) *SessionRecord,
) (rec, prev *SessionRecord, rev uint64, err error) {
	key, err := sessionKey(tenant, clientID)
	if err != nil {
		return nil, nil, 0, err
	}

	if nodeID == "" || build == nil {
		return nil, nil, 0, fmt.Errorf("cluster: acquire session: needs a node id and a builder: %w", tether.ErrInvalidInput)
	}

	ctx, cancel := opContext(ctx)
	defer cancel()

	h, err := s.handles(ctx)
	if err != nil {
		return nil, nil, 0, err
	}

	unlock, err := s.keys.lock(ctx, key)
	if err != nil {
		return nil, nil, 0, s.wrapErr("acquire session", err)
	}
	defer unlock()

	var (
		firstRev uint64
		firstErr error
	)

	for attempt := 0; ; attempt++ {
		cur, rerr := s.read(ctx, h, key)
		if rerr != nil {
			return nil, nil, 0, rerr
		}

		if attempt == 0 && s.afterRead != nil {
			s.afterRead()
		}

		if attempt > 0 && (cur.state == stateLive || cur.rev == firstRev) {
			return nil, nil, 0, firstErr
		}

		var epoch uint64
		if cur.rec != nil {
			epoch = cur.rec.Epoch
		}

		next := build(cur.rec)
		if next == nil {
			next = new(SessionRecord)
		}

		next.Owner = nodeID
		next.Epoch = epoch + 1

		newRev, werr := s.write(ctx, h, key, next, cur.rev, 0)
		if werr == nil {
			if cur.state != stateLive {
				return next, nil, newRev, nil
			}

			return next, cur.rec, newRev, nil
		}

		if !isLostCAS(werr) || cur.state == stateLive || attempt > 0 {
			return nil, nil, 0, s.wrapErr("acquire session", werr)
		}

		firstRev, firstErr = cur.rev, s.wrapErr("acquire session", werr)
	}
}

// Update stores rec on the session's key if the key is still at revision rev,
// and returns the new revision. Losing returns an error wrapping ErrLostCAS.
// The write carries no expiry: it clears any expiry an earlier Expire set.
func (s *Sessions) Update(ctx context.Context, tenant, clientID string, rev uint64, rec *SessionRecord) (uint64, error) {
	return s.cas(ctx, "update session", tenant, clientID, rev, rec, 0)
}

// Expire stores rec on the session's key like Update, and makes the key expire
// after ttl. ttl is rounded up to whole seconds, one second at least. When it
// runs out, JetStream replaces the record with a limit marker and every watcher
// sees the key disappear. A later Update or Acquire cancels the expiry.
func (s *Sessions) Expire(ctx context.Context, tenant, clientID string, rev uint64, rec *SessionRecord, ttl time.Duration) (uint64, error) {
	if ttl <= 0 {
		return 0, fmt.Errorf("cluster: expire session: ttl %s: %w", ttl, tether.ErrInvalidInput)
	}

	return s.cas(ctx, "expire session", tenant, clientID, rev, rec, ttl)
}

func (s *Sessions) cas(ctx context.Context, op, tenant, clientID string, rev uint64, rec *SessionRecord, ttl time.Duration) (uint64, error) {
	key, err := sessionKey(tenant, clientID)
	if err != nil {
		return 0, err
	}

	if rec == nil || rev == 0 {
		return 0, fmt.Errorf("cluster: %s: needs a record and a revision: %w", op, tether.ErrInvalidInput)
	}

	ctx, cancel := opContext(ctx)
	defer cancel()

	h, err := s.handles(ctx)
	if err != nil {
		return 0, err
	}

	unlock, err := s.keys.lock(ctx, key)
	if err != nil {
		return 0, s.wrapErr(op, err)
	}
	defer unlock()

	newRev, err := s.write(ctx, h, key, rec, rev, ttl)
	if err != nil {
		return 0, s.wrapErr(op, err)
	}

	return newRev, nil
}

// Delete removes the session's key if it is still at revision rev. It leaves a
// purge marker that expires after the bucket's limit marker TTL.
func (s *Sessions) Delete(ctx context.Context, tenant, clientID string, rev uint64) error {
	key, err := sessionKey(tenant, clientID)
	if err != nil {
		return err
	}

	if rev == 0 {
		return fmt.Errorf("cluster: delete session: needs a revision: %w", tether.ErrInvalidInput)
	}

	ctx, cancel := opContext(ctx)
	defer cancel()

	h, err := s.handles(ctx)
	if err != nil {
		return err
	}

	unlock, err := s.keys.lock(ctx, key)
	if err != nil {
		return s.wrapErr("delete session", err)
	}
	defer unlock()

	ws := s.watcherSnapshot()
	for _, w := range ws {
		w.begin(key)
	}

	hdr := nats.Header{}
	hdr.Set(kvOpHeader, kvOpPurge)
	hdr.Set(jetstream.MsgRollup, jetstream.MsgRollupSubject)

	markerRev, err := s.publish(ctx, h, key, nil, rev, limitMarkerTTL, hdr)

	for _, w := range ws {
		w.end(key, markerRev, nil, true, err == nil)
	}

	if err != nil {
		return s.wrapErr("delete session", err)
	}

	return nil
}

// Get returns the session on a key and its revision. A missing or expired
// session returns an error wrapping tether.ErrNotFound.
func (s *Sessions) Get(ctx context.Context, tenant, clientID string) (*SessionRecord, uint64, error) {
	key, err := sessionKey(tenant, clientID)
	if err != nil {
		return nil, 0, err
	}

	ctx, cancel := opContext(ctx)
	defer cancel()

	h, err := s.handles(ctx)
	if err != nil {
		return nil, 0, err
	}

	cur, err := s.read(ctx, h, key)
	if err != nil {
		return nil, 0, err
	}

	if cur.state != stateLive {
		return nil, 0, fmt.Errorf("cluster: get session %q of tenant %q: %w", clientID, tenant, tether.ErrNotFound)
	}

	return cur.rec, cur.rev, nil
}

// SessionEntry is one session Entries or OwnedBy found: the tenant and client
// ID its key names, its record and the key's revision.
type SessionEntry struct {
	Tenant   string
	ClientID string
	Record   *SessionRecord
	Revision uint64
}

// List returns every live session of a tenant, in no particular order. It reads
// the whole tenant through a watcher, so it costs a pass over the tenant's
// keys.
func (s *Sessions) List(ctx context.Context, tenant string) ([]*SessionRecord, error) {
	entries, err := s.Entries(ctx, tenant)
	if err != nil {
		return nil, err
	}

	out := make([]*SessionRecord, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Record)
	}

	return out, nil
}

// Entries is List with each session's client ID and revision.
func (s *Sessions) Entries(ctx context.Context, tenant string) ([]SessionEntry, error) {
	probe, err := sessionKey(tenant, "x")
	if err != nil {
		return nil, err
	}

	prefix, _, _ := strings.Cut(probe, ".")

	return s.scan(ctx, prefix+".>", "list sessions", nil)
}

// OwnedBy returns every live session of the cell, in any tenant, whose record
// names one of nodeIDs as its owner, in no particular order. It reads the
// whole bucket through a watcher, so it costs one pass over every key of the
// cell however many nodes it asks for. The revisions are those the pass read:
// a write that uses one loses its CAS if the key moved since.
func (s *Sessions) OwnedBy(ctx context.Context, nodeIDs ...string) ([]SessionEntry, error) {
	if len(nodeIDs) == 0 {
		return nil, fmt.Errorf("cluster: owned sessions: no node ID: %w", tether.ErrInvalidInput)
	}

	want := make(map[string]struct{}, len(nodeIDs))

	for _, id := range nodeIDs {
		if id == "" {
			return nil, fmt.Errorf("cluster: owned sessions: empty node ID: %w", tether.ErrInvalidInput)
		}

		want[id] = struct{}{}
	}

	return s.scan(ctx, ">", "list owned sessions", func(rec *SessionRecord) bool {
		_, ok := want[rec.Owner]

		return ok
	})
}

// scan reads the keys under filter through a watcher and returns those whose
// record keep accepts (all of them when keep is nil).
func (s *Sessions) scan(ctx context.Context, filter, op string, keep func(*SessionRecord) bool) ([]SessionEntry, error) {
	h, err := s.handles(ctx)
	if err != nil {
		return nil, err
	}

	w, err := h.kv.Watch(ctx, filter, jetstream.IgnoreDeletes())
	if err != nil {
		return nil, s.wrapErr(op, err)
	}
	defer s.stopWatcher(w)

	var out []SessionEntry

	for {
		select {
		case <-ctx.Done():
			return nil, s.wrapErr(op, ctx.Err())
		case e, ok := <-w.Updates():
			if !ok {
				return nil, fmt.Errorf("cluster: %s: watcher closed: %w", op, ErrUnavailable)
			}

			if e == nil {
				return out, nil
			}

			tenant, clientID, kerr := splitSessionKey(e.Key())
			if kerr != nil {
				s.c.logger.Warn("skipping undecodable session key", "key", e.Key(), "error", kerr)

				continue
			}

			rec, derr := decodeSession(e.Value())
			if derr != nil {
				s.c.logger.Warn("skipping undecodable session record", "key", e.Key(), "error", derr)

				continue
			}

			if keep != nil && !keep(rec) {
				continue
			}

			out = append(out, SessionEntry{Tenant: tenant, ClientID: clientID, Record: rec, Revision: e.Revision()})
		}
	}
}

// stopWatcher stops a KV watcher, and logs the error rather than losing it.
func (s *Sessions) stopWatcher(w jetstream.KeyWatcher) {
	if err := w.Stop(); err != nil {
		s.c.logger.Debug("stopping kv watcher", "error", err)
	}
}

func (s *Sessions) watcherSnapshot() []*ownedWatch {
	s.wmu.Lock()
	defer s.wmu.Unlock()

	if len(s.watchers) == 0 {
		return nil
	}

	return append([]*ownedWatch(nil), s.watchers...)
}

// keyedMutex is a lock per key that frees its entry when nobody holds or waits
// for it, so the map holds only the keys in use.
type keyedMutex struct {
	mu sync.Mutex
	m  map[string]*keyLock
}

type keyLock struct {
	token chan struct{} // holds one token while the lock is taken
	refs  int           // holders and waiters
}

// lock takes the lock on key, or fails when ctx ends first. Call the returned
// func to release it.
func (k *keyedMutex) lock(ctx context.Context, key string) (func(), error) {
	k.mu.Lock()

	if k.m == nil {
		k.m = make(map[string]*keyLock)
	}

	e := k.m[key]
	if e == nil {
		e = &keyLock{token: make(chan struct{}, 1)}
		k.m[key] = e
	}

	e.refs++

	k.mu.Unlock()

	select {
	case e.token <- struct{}{}:
		return func() {
			<-e.token
			k.release(key, e)
		}, nil
	case <-ctx.Done():
		k.release(key, e)

		return nil, ctx.Err()
	}
}

func (k *keyedMutex) release(key string, e *keyLock) {
	k.mu.Lock()
	defer k.mu.Unlock()

	e.refs--

	if e.refs == 0 {
		delete(k.m, key)
	}
}

func (k *keyedMutex) size() int {
	k.mu.Lock()
	defer k.mu.Unlock()

	return len(k.m)
}
