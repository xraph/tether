package encoding

import (
	"strings"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/xraph/tether/topic"
)

var topicSeeds = []string{
	"a/b/c", "a//b", ".", " ", "*", ">", "$SYS/broker/uptime", "$", "é/日本/😀", "=a/=/==",
	"/", "//", "a/", "/a", "a.b/c.d", "a b/c\td", "\x00", "-", "_", "a=b", "=",
}

var filterSeeds = []string{
	"#", "+", "+/+", "a/#", "a/+/c", "+/a", "$SYS/#", "$SYS/+", "+/#", "a//b", "a/+/+/#", "//", "+//#",
	"é/+", "$", "$+", "a#", "a+/b", "=a/#", "./+", "*/#", ">/+",
}

func FuzzRoundTrip(f *testing.F) {
	for _, s := range topicSeeds {
		f.Add(tenantID, s)
	}

	f.Add("t.x*>", "a/b")
	f.Add("", "")

	f.Fuzz(func(t *testing.T, tenant, name string) {
		// One level.
		tok := EncodeLevel(name)
		if got, err := DecodeLevel(tok); err != nil || got != name {
			t.Fatalf("level %q -> %q -> %q, %v", name, tok, got, err)
		}

		if strings.ContainsAny(tok, ".*> \t\r\n\x00/") {
			t.Fatalf("token %q for level %q holds a delimiter or wildcard", tok, name)
		}

		// A passthrough token never starts with the marker, an encoded one always does.
		if (tok == name) == strings.HasPrefix(tok, "=") {
			t.Fatalf("token %q for %q: passthrough/encoded kinds overlap", tok, name)
		}

		// A whole topic, any string, wildcards and invalid UTF-8 included.
		subj := EncodeTopic(tenant, name)

		gotTenant, gotTopic, err := DecodeTopic(subj)
		if err != nil || gotTenant != tenant || gotTopic != name {
			t.Fatalf("topic (%q,%q) -> %q -> (%q,%q), %v", tenant, name, subj, gotTenant, gotTopic, err)
		}

		// Length bound: the topic part is at most 2*len+1 bytes.
		body := subj[strings.Index(subj, ".")+1:]
		if len(body) > 2*len(name)+1 {
			t.Fatalf("topic %q (%d bytes) encoded to %d bytes, over 2n+1", name, len(name), len(body))
		}
	})
}

func FuzzKVKeySafe(f *testing.F) {
	for _, s := range topicSeeds {
		f.Add(tenantID, s, s)
	}

	for _, s := range filterSeeds {
		f.Add(tenantID, s, s)
	}

	f.Fuzz(func(t *testing.T, tenant, name, client string) {
		keys := []string{
			"r." + EncodeTopic(tenant, name),
			"s." + EncodeLevel(tenant) + "." + EncodeLevel(client),
			"n." + EncodeLevel(client),
		}
		for _, key := range keys {
			if !kvKeyValid(key) {
				t.Fatalf("key %q (tenant %q, topic %q, client %q) is not a valid KV key", key, tenant, name, client)
			}
		}

		// Subscriptions on the KV side watch with the filter subjects.
		if topic.Valid(name, true, 0) == nil && !strings.EqualFold(firstLevel(name), "$share") {
			for _, s := range EncodeFilter(tenant, name) {
				if !kvSearchValid("r." + s) {
					t.Fatalf("watch pattern %q for filter %q is not a valid KV search key", "r."+s, name)
				}
			}
		}
	})
}

func FuzzSubjectValid(f *testing.F) {
	for _, s := range topicSeeds {
		f.Add(tenantID, s, s)
	}

	for _, s := range filterSeeds {
		f.Add(tenantID, s, s)
	}

	f.Fuzz(func(t *testing.T, tenant, name, filter string) {
		for _, prefix := range []string{"f.", "d."} {
			subj := prefix + EncodeTopic(tenant, name)
			if !server.IsValidPublishSubject(subj) {
				t.Fatalf("publish subject %q (tenant %q, topic %q) rejected by nats-server", subj, tenant, name)
			}
		}

		if topic.Valid(filter, true, 0) != nil || strings.EqualFold(firstLevel(filter), "$share") {
			return
		}

		for _, s := range EncodeFilter(tenant, filter) {
			if !server.IsValidSubject("f." + s) {
				t.Fatalf("filter subject %q for filter %q rejected by nats-server", "f."+s, filter)
			}
		}

		for _, s := range TenantAll(tenant) {
			if !server.IsValidSubject("f." + s) {
				t.Fatalf("tenant-all subject %q rejected by nats-server", "f."+s)
			}
		}
	})
}

func FuzzWildcardEquivalence(f *testing.F) {
	for _, flt := range filterSeeds {
		for _, tp := range topicSeeds {
			f.Add(flt, tp, byte(0), "ttnt_other")
			f.Add(flt, tp, byte(1), "ttnt_other")
			f.Add(flt, tp, byte(3), "ttnt_other")
		}
	}

	f.Fuzz(func(t *testing.T, filter, name string, mix byte, otherTenant string) {
		name = derive(filter, name, mix)
		if !mqttValid(filter, name) {
			return
		}

		want := topic.Match(filter, name)
		subs := EncodeFilter(tenantID, filter)
		subj := EncodeTopic(tenantID, name)

		got := matchAny(subs, subj)
		if got != want {
			t.Fatalf("filter %q topic %q: mqtt %v, nats %v (filter %q, topic %q)", filter, name, want, got, subs, subj)
		}

		// The hand-written matcher must agree with nats-server's own.
		var oracle bool

		for _, s := range subs {
			oracle = oracle || server.SubjectMatchesFilter(subj, s)
		}

		if oracle != got {
			t.Fatalf("filter %v topic %q: own matcher %v, nats-server %v", subs, subj, got, oracle)
		}

		// A filter in one tenant never reaches another tenant's topic.
		if otherTenant != tenantID && matchAny(subs, EncodeTopic(otherTenant, name)) {
			t.Fatalf("tenant %q filter %q matched tenant %q topic %q", tenantID, filter, otherTenant, name)
		}

		// The tenant-wide pair carries every topic of the tenant, "$" topics included.
		if !matchAny(TenantAll(tenantID), subj) {
			t.Fatalf("TenantAll misses %q", subj)
		}
	})
}

func FuzzDecodeCanonical(f *testing.F) {
	f.Add("ttnt_x.a.b")
	f.Add("==ttnt_x.=JFNZUw.x")
	f.Add("===Zm9v.=JFNZUw")
	f.Add("a.=")
	f.Add("a.=YQ")
	f.Add("a.=YR")
	f.Add("a..b")
	f.Add("a.b*")
	f.Add("==.a")
	f.Add("x.$a")
	f.Add("t.=Lw")
	f.Add("=\r")
	f.Add("t.=Lg\n")
	f.Add("=.=000v.0000000")

	f.Fuzz(func(t *testing.T, s string) {
		if lvl, err := DecodeLevel(s); err == nil {
			if got := EncodeLevel(lvl); got != s {
				t.Fatalf("token %q decoded to %q which encodes to %q", s, lvl, got)
			}
		}

		tenant, name, err := DecodeTopic(s)
		if err != nil {
			return
		}

		if got := EncodeTopic(tenant, name); got != s {
			t.Fatalf("subject %q decoded to (%q,%q) which encodes to %q", s, tenant, name, got)
		}
	})
}
