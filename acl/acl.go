// Package acl compiles a role's topic grants into a per-connection check.
//
// Grants are MQTT filters in tenant-relative form, with two placeholders:
// "%c" is replaced by the connecting client's ID and "%u" by its username.
// [Compile] expands them once per connection, and the broker then calls
// [Set.CanPublish] on every PUBLISH and [Set.CanSubscribe] on every SUBSCRIBE
// filter and every delivery. Both checks only read the compiled set, so they
// are safe for concurrent use and allocate nothing.
//
// The package is pure: it knows nothing about the broker, warden or forge.
package acl

import (
	"slices"
	"strings"

	"github.com/xraph/tether/topic"
)

// Rules holds the topic filters a role grants, as stored in warden role
// metadata. Publish grants are matched against the topic name of a PUBLISH.
// Subscribe grants must cover the filter of a SUBSCRIBE.
type Rules struct {
	Publish   []string `json:"publish"`
	Subscribe []string `json:"subscribe"`
}

// Vars are the values substituted for placeholders: "%c" takes ClientID and
// "%u" takes Username.
type Vars struct {
	ClientID, Username string
}

// Merge returns the union of the given rules. Exact duplicates are removed and
// the first-seen order is kept. The result never shares memory with the inputs.
func Merge(rs ...Rules) Rules {
	var out Rules

	for _, r := range rs {
		for _, g := range r.Publish {
			if !slices.Contains(out.Publish, g) {
				out.Publish = append(out.Publish, g)
			}
		}

		for _, g := range r.Subscribe {
			if !slices.Contains(out.Subscribe, g) {
				out.Subscribe = append(out.Subscribe, g)
			}
		}
	}

	return out
}

// Set is a compiled, immutable grant set for one connection. A nil Set allows
// nothing.
type Set struct {
	publish   []string
	subscribe []string
	dropped   []string
}

// Compile expands the placeholders in r with v and returns the checkable set.
//
// A grant is dropped, never widened, when a placeholder value it uses is empty
// or holds "/", "+", "#" or NUL (so a client ID of "a/b" or "#" cannot turn
// "devices/%c/up" into a broader grant), or when the expanded grant is not a
// valid plain filter. A grant in the "$share/<group>/<filter>" form is dropped
// too: shared subscriptions are checked against their plain filter, so a
// grant is never written that way. A value only affects the grants that use its
// placeholder. [Set.Dropped] lists what was dropped.
func Compile(r Rules, v Vars) *Set {
	s := &Set{}
	s.publish = s.expandAll(r.Publish, v)
	s.subscribe = s.expandAll(r.Subscribe, v)

	return s
}

func (s *Set) expandAll(grants []string, v Vars) []string {
	var out []string

	for _, g := range grants {
		e, ok := expand(g, v)
		if !ok {
			s.dropped = append(s.dropped, g)

			continue
		}

		out = append(out, e)
	}

	return out
}

// expand substitutes "%c" and "%u" in grant, scanning left to right. A "%"
// that is not followed by "c" or "u" is kept as is.
func expand(grant string, v Vars) (string, bool) {
	if strings.Contains(grant, "%") {
		var b strings.Builder

		for i := 0; i < len(grant); i++ {
			if grant[i] != '%' || i+1 == len(grant) {
				b.WriteByte(grant[i])

				continue
			}

			var val string

			switch grant[i+1] {
			case 'c':
				val = v.ClientID
			case 'u':
				val = v.Username
			default:
				b.WriteByte('%')

				continue
			}

			if !safeValue(val) {
				return "", false
			}

			b.WriteString(val)

			i++
		}

		grant = b.String()
	}

	if topic.Valid(grant, true, 0) != nil || isShared(grant) {
		return "", false
	}

	return grant, true
}

// safeValue reports whether val can stand in for a placeholder without
// changing the shape of the grant.
func safeValue(val string) bool {
	return val != "" && !strings.ContainsAny(val, "/+#\x00")
}

func isShared(grant string) bool {
	first, _, _ := strings.Cut(grant, "/")

	return strings.EqualFold(first, "$share")
}

// CanPublish reports whether the client may publish to the topic name, which
// is plain and tenant-relative. A name that is empty or holds a wildcard is
// never publishable, whatever the grants say.
func (s *Set) CanPublish(name string) bool {
	if s == nil || name == "" || strings.ContainsAny(name, "+#") {
		return false
	}

	for _, g := range s.publish {
		if topic.Match(g, name) {
			return true
		}
	}

	return false
}

// CanSubscribe reports whether the client may subscribe to filterOrTopic, a
// plain tenant-relative filter (the broker strips "$share/<group>/" first). A
// subscribe grant has to cover every topic the filter can match. A literal
// topic is covered exactly when it matches, so the same call also decides
// whether a delivery topic is still allowed.
func (s *Set) CanSubscribe(filterOrTopic string) bool {
	if s == nil {
		return false
	}

	for _, g := range s.subscribe {
		if topic.Covers(g, filterOrTopic) {
			return true
		}
	}

	return false
}

// Dropped returns the original, unexpanded grants that Compile dropped,
// publish grants first and then subscribe grants, each in input order. The
// broker logs them. The slice is a copy.
func (s *Set) Dropped() []string {
	if s == nil {
		return nil
	}

	return slices.Clone(s.dropped)
}
