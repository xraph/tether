package subject

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

// FuzzRoundTrip checks that any tenant and topic, wildcards and invalid UTF-8
// included, survive every encoder and its decoder.
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

		if tenant == "" || len(name) > MaxTopicBytes {
			if QoS0(tenant, name) != "" || Log(tenant, name) != "" || RetainedKey(tenant, name) != "" {
				t.Fatalf("tenant %q topic of %d bytes was not refused", tenant, len(name))
			}

			return
		}

		// A whole topic through each of the three encoders.
		subj := Log(tenant, name)

		gotTenant, gotTopic, err := DecodeLog(subj)
		if err != nil || gotTenant != tenant || gotTopic != name {
			t.Fatalf("log (%q,%q) -> %q -> (%q,%q), %v", tenant, name, subj, gotTenant, gotTopic, err)
		}

		key := RetainedKey(tenant, name)

		gotTenant, gotTopic, err = DecodeRetainedKey(key)
		if err != nil || gotTenant != tenant || gotTopic != name {
			t.Fatalf("retained (%q,%q) -> %q -> (%q,%q), %v", tenant, name, key, gotTenant, gotTopic, err)
		}

		// The QoS 0 subject is the retained key plus the prefix, "$" space included.
		if got := QoS0(tenant, name); got != "f."+key {
			t.Fatalf("QoS0 %q is not f. + retained key %q", got, key)
		}

		// The log differs from the retained key only in the "$" space.
		if !strings.HasPrefix(name, "$") && subj != "d."+key {
			t.Fatalf("log %q is not d. + retained key %q", subj, key)
		}

		// Length bound: the topic part is at most 2*len+1 bytes.
		body := subj[strings.Index(subj, ".")+1:]
		body = body[strings.Index(body, ".")+1:]

		if len(body) > 2*len(name)+1 {
			t.Fatalf("topic %q (%d bytes) encoded to %d bytes, over 2n+1", name, len(name), len(body))
		}
	})
}

// FuzzKVKeySafe checks that every key and key filter is valid for JetStream KV.
func FuzzKVKeySafe(f *testing.F) {
	for _, s := range topicSeeds {
		f.Add(tenantID, s, s)
	}

	for _, s := range filterSeeds {
		f.Add(tenantID, s, s)
	}

	f.Fuzz(func(t *testing.T, tenant, name, client string) {
		if tenant == "" || len(name) > MaxTopicBytes || len(client) > MaxClientIDBytes {
			return
		}

		for _, key := range []string{RetainedKey(tenant, name), SessionKey(tenant, client)} {
			if !kvKeyValid(key) {
				t.Fatalf("key %q (tenant %q, topic %q, client %q) is not a valid KV key", key, tenant, name, client)
			}
		}

		// Subscriptions on the KV side watch with the filter keys.
		if topic.Valid(name, true, 0) == nil && !strings.EqualFold(firstLevel(name), "$share") {
			for _, s := range RetainedKeyFilter(tenant, name) {
				if !kvSearchValid(s) {
					t.Fatalf("watch pattern %q for filter %q is not a valid KV search key", s, name)
				}
			}
		}
	})
}

// FuzzSubjectValid checks that nats-server accepts every subject and filter.
func FuzzSubjectValid(f *testing.F) {
	for _, s := range topicSeeds {
		f.Add(tenantID, s, s)
	}

	for _, s := range filterSeeds {
		f.Add(tenantID, s, s)
	}

	f.Fuzz(func(t *testing.T, tenant, name, filter string) {
		if tenant == "" || len(name) > MaxTopicBytes || len(filter) > MaxTopicBytes {
			return
		}

		for _, subj := range []string{QoS0(tenant, name), Log(tenant, name)} {
			if !server.IsValidPublishSubject(subj) {
				t.Fatalf("publish subject %q (tenant %q, topic %q) rejected by nats-server", subj, tenant, name)
			}
		}

		if !server.IsValidSubject(LogTenantAll(tenant)) {
			t.Fatalf("log tenant-all %q rejected by nats-server", LogTenantAll(tenant))
		}

		if topic.Valid(filter, true, 0) != nil || strings.EqualFold(firstLevel(filter), "$share") {
			return
		}

		for _, s := range QoS0Filters(tenant, filter) {
			if !server.IsValidSubject(s) {
				t.Fatalf("filter subject %q for filter %q rejected by nats-server", s, filter)
			}
		}
	})
}

// FuzzWildcardEquivalence checks that NATS matching of the encoded names equals
// MQTT matching, for the QoS 0 subjects and the retained keys.
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
		if !mqttValid(filter, name) || len(name) > MaxTopicBytes || len(filter) > MaxTopicBytes || otherTenant == "" {
			return
		}

		want := topic.Match(filter, name)
		subs := QoS0Filters(tenantID, filter)
		subj := QoS0(tenantID, name)

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

		// The retained keys follow the same rule.
		key := RetainedKey(tenantID, name)
		if rgot := matchAny(RetainedKeyFilter(tenantID, filter), key); rgot != want {
			t.Fatalf("filter %q topic %q: mqtt %v, kv %v (%q vs %q)", filter, name, want, rgot, RetainedKeyFilter(tenantID, filter), key)
		}

		// A filter in one tenant never reaches another tenant's topic.
		if otherTenant != tenantID {
			if matchAny(subs, QoS0(otherTenant, name)) || matchAny(RetainedKeyFilter(tenantID, filter), RetainedKey(otherTenant, name)) {
				t.Fatalf("tenant %q filter %q matched tenant %q topic %q", tenantID, filter, otherTenant, name)
			}
		}

		// The log reader takes everything of the tenant and nothing of another.
		if !server.SubjectMatchesFilter(Log(tenantID, name), LogTenantAll(tenantID)) {
			t.Fatalf("LogTenantAll misses %q", Log(tenantID, name))
		}

		if otherTenant != tenantID && server.SubjectMatchesFilter(Log(otherTenant, name), LogTenantAll(tenantID)) {
			t.Fatalf("LogTenantAll of %q matched tenant %q", tenantID, otherTenant)
		}
	})
}

// FuzzDecodeCanonical checks that anything a decoder accepts re-encodes to the
// same string, so every name has exactly one reading.
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

		if tenant, name, err := DecodeRetainedKey(s); err == nil {
			if got := RetainedKey(tenant, name); got != s {
				t.Fatalf("key %q decoded to (%q,%q) which encodes to %q", s, tenant, name, got)
			}
		}

		if tenant, name, err := DecodeLog("d." + s); err == nil {
			if got := Log(tenant, name); got != "d."+s {
				t.Fatalf("log subject %q decoded to (%q,%q) which encodes to %q", "d."+s, tenant, name, got)
			}
		}
	})
}
