package subject

import (
	"errors"
	"strings"
	"testing"

	"github.com/nats-io/nats-server/v2/server"

	"github.com/xraph/tether"
	"github.com/xraph/tether/topic"
)

func TestEncodeTable(t *testing.T) {
	cases := []struct {
		name  string
		topic string
		want  string // topic part after the tenant token
		dollr bool
	}{
		{"plain", "a/b/c", "a.b.c", false},
		{"empty level", "a//b", "a.=.b", false},
		{"leading slash", "/a", "=.a", false},
		{"trailing slash", "a/", "a.=", false},
		{"only slashes", "//", "=.=.=", false},
		{"empty topic", "", "=", false},
		{"dot", ".", "=Lg", false},
		{"dot inside a level", "a.b/c", "=YS5i.c", false},
		{"space", " ", "=IA", false},
		{"star", "*", "=Kg", false},
		{"gt", ">", "=Pg", false},
		{"plus and hash as text", "+/#", "=Kw.=Iw", false},
		{"dollar sys", "$SYS/broker", "=JFNZUw.broker", true},
		{"lone dollar", "$", "=JA", true},
		{"dollar not first", "a/$b", "a.=JGI", false},
		{"emoji", "😀", "=8J-YgA", false},
		{"utf8 level", "é", "=w6k", false},
		{"leading equals", "=a", "=PWE", false},
		{"lone equals", "=", "=PQ", false},
		{"inner equals passes", "a=b", "a=b", false},
		{"dash and underscore", "-_", "-_", false},
		{"nul", "\x00", "=AA", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wantHead := tenantID
			if tc.dollr {
				wantHead = "==" + tenantID
			}

			// QoS 0 and the retained key put "$" topics under "==<tenant>".
			if got, want := QoS0(tenantID, tc.topic), "f."+wantHead+"."+tc.want; got != want {
				t.Fatalf("QoS0(%q) = %q, want %q", tc.topic, got, want)
			}

			key := RetainedKey(tenantID, tc.topic)
			if want := wantHead + "." + tc.want; key != want {
				t.Fatalf("RetainedKey(%q) = %q, want %q", tc.topic, key, want)
			}

			tenant, got, err := DecodeRetainedKey(key)
			if err != nil || tenant != tenantID || got != tc.topic {
				t.Fatalf("DecodeRetainedKey(%q) = %q, %q, %v", key, tenant, got, err)
			}

			qos0 := QoS0(tenantID, tc.topic)

			tenant, got, err = DecodeQoS0(qos0)
			if err != nil || tenant != tenantID || got != tc.topic {
				t.Fatalf("DecodeQoS0(%q) = %q, %q, %v", qos0, tenant, got, err)
			}

			// The log keeps "$" topics under the plain tenant token.
			logSubj := Log(tenantID, tc.topic)
			if want := "d." + tenantID + "." + tc.want; logSubj != want {
				t.Fatalf("Log(%q) = %q, want %q", tc.topic, logSubj, want)
			}

			tenant, got, err = DecodeLog(logSubj)
			if err != nil || tenant != tenantID || got != tc.topic {
				t.Fatalf("DecodeLog(%q) = %q, %q, %v", logSubj, tenant, got, err)
			}

			if !server.IsValidPublishSubject(QoS0(tenantID, tc.topic)) || !server.IsValidPublishSubject(logSubj) || !kvKeyValid(key) {
				t.Fatalf("%q, %q or %q is not a valid subject and KV key", QoS0(tenantID, tc.topic), logSubj, key)
			}
		})
	}
}

func TestEncodeFilterTable(t *testing.T) {
	cases := []struct {
		filter string
		want   []string // after the tenant token
		dollr  bool
	}{
		{"#", []string{">"}, false},
		{"+", []string{"*"}, false},
		{"a/#", []string{"a", "a.>"}, false},
		{"a/+/c", []string{"a.*.c"}, false},
		{"+/#", []string{"*", "*.>"}, false},
		{"a//b", []string{"a.=.b"}, false},
		{"//#", []string{"=.=", "=.=.>"}, false},
		{"$SYS/#", []string{"=JFNZUw", "=JFNZUw.>"}, true},
		{"$SYS/+", []string{"=JFNZUw.*"}, true},
		{"*/>", []string{"=Kg.=Pg"}, false},
	}

	for _, tc := range cases {
		head := tenantID
		if tc.dollr {
			head = "==" + tenantID
		}

		for _, kind := range []struct {
			name   string
			prefix string
			got    []string
		}{
			{"QoS0Filters", "f.", QoS0Filters(tenantID, tc.filter)},
			{"RetainedKeyFilter", "", RetainedKeyFilter(tenantID, tc.filter)},
		} {
			if len(kind.got) != len(tc.want) {
				t.Fatalf("%s(%q) = %q, want %v", kind.name, tc.filter, kind.got, tc.want)
			}

			for i := range kind.got {
				if want := kind.prefix + head + "." + tc.want[i]; kind.got[i] != want {
					t.Errorf("%s(%q)[%d] = %q, want %q", kind.name, tc.filter, i, kind.got[i], want)
				}
			}
		}
	}
}

func TestLogTenantAll(t *testing.T) {
	if got, want := LogTenantAll(tenantID), "d."+tenantID+".>"; got != want {
		t.Fatalf("LogTenantAll = %q, want %q", got, want)
	}

	// One filter carries every logged message of the tenant, "$" topics included.
	for _, name := range []string{"a", "a/b", "$SYS/x", "$", "//", "é/😀"} {
		if !natsMatch(LogTenantAll(tenantID), Log(tenantID, name)) {
			t.Errorf("LogTenantAll misses %q", name)
		}
	}

	if natsMatch(LogTenantAll(tenantID), Log("ttnt_other", "a")) {
		t.Error("LogTenantAll matches another tenant")
	}
}

func TestLogSubjectKeepsDollarUnderTenant(t *testing.T) {
	if got := Log("t", "$SYS/x"); !strings.HasPrefix(got, "d.t.") {
		t.Fatalf("Log(t, $SYS/x) = %q, want the d.t. prefix", got)
	}
}

func TestQoS0DollarUnderDoubleEquals(t *testing.T) {
	if got := QoS0("t", "$SYS/x"); !strings.HasPrefix(got, "f.==t.") {
		t.Fatalf("QoS0(t, $SYS/x) = %q, want the f.==t. prefix", got)
	}
}

func TestKeyShapes(t *testing.T) {
	if got := SessionKey("t", "c"); got != "t.c" {
		t.Errorf("SessionKey = %q, want t.c", got)
	}

	if got := SessionKey("t", "a.b"); got != "t.=YS5i" {
		t.Errorf("SessionKey with a dot = %q, want t.=YS5i", got)
	}

	if got := RetainedKey("t", "a/b"); got != "t.a.b" {
		t.Errorf("RetainedKey = %q, want t.a.b", got)
	}

	if got := RetainedKey("t", "$SYS/x"); got != "==t.=JFNZUw.x" {
		t.Errorf("RetainedKey $SYS = %q, want ==t.=JFNZUw.x", got)
	}
}

func TestKeysMatchKVRegex(t *testing.T) {
	re := kvKeyRe

	topics := []string{"a/b", "$SYS/x", "", "//", ".", "*", ">", "é/😀", "=a", "a b/c\td", "\x00", "+/#", "a.b"}
	for _, tp := range topics {
		for _, key := range []string{RetainedKey(tenantID, tp), SessionKey(tenantID, tp), QoS0(tenantID, tp), Log(tenantID, tp)} {
			key = strings.TrimPrefix(strings.TrimPrefix(key, "f."), "d.")
			if !re.MatchString(key) || !kvKeyValid(key) {
				t.Errorf("key %q (from %q) does not match %s", key, tp, re)
			}
		}
	}

	for _, f := range []string{"#", "+", "a/#", "$SYS/+", "+/a/+"} {
		for _, k := range RetainedKeyFilter(tenantID, f) {
			if !kvSearchValid(k) {
				t.Errorf("key filter %q (from %q) is not a valid KV search key", k, f)
			}
		}
	}
}

func TestCaps(t *testing.T) {
	long := strings.Repeat("a", MaxTopicBytes+1)
	ok := strings.Repeat("a", MaxTopicBytes)

	if err := CheckTopic(long); !errors.Is(err, tether.ErrInvalidInput) {
		t.Errorf("CheckTopic(1025 bytes) = %v, want ErrInvalidInput", err)
	}

	if err := CheckTopic(ok); err != nil {
		t.Errorf("CheckTopic(1024 bytes) = %v", err)
	}

	if got := QoS0(tenantID, long); got != "" {
		t.Errorf("QoS0 of a 1025 byte topic = %q, want refusal", got)
	}

	if got := Log(tenantID, long); got != "" {
		t.Errorf("Log of a 1025 byte topic = %q, want refusal", got)
	}

	if got := RetainedKey(tenantID, long); got != "" {
		t.Errorf("RetainedKey of a 1025 byte topic = %q, want refusal", got)
	}

	if got := QoS0Filters(tenantID, long); got != nil {
		t.Errorf("QoS0Filters of a 1025 byte filter = %q, want refusal", got)
	}

	if got := RetainedKeyFilter(tenantID, long); got != nil {
		t.Errorf("RetainedKeyFilter of a 1025 byte filter = %q, want refusal", got)
	}

	if got := QoS0(tenantID, ok); got == "" {
		t.Error("QoS0 refused a 1024 byte topic")
	}

	longID := strings.Repeat("c", MaxClientIDBytes+1)
	okID := strings.Repeat("c", MaxClientIDBytes)

	if err := CheckClientID(longID); !errors.Is(err, tether.ErrInvalidInput) {
		t.Errorf("CheckClientID(257 bytes) = %v, want ErrInvalidInput", err)
	}

	if err := CheckClientID(okID); err != nil {
		t.Errorf("CheckClientID(256 bytes) = %v", err)
	}

	if got := SessionKey(tenantID, longID); got != "" {
		t.Errorf("SessionKey of a 257 byte client id = %q, want refusal", got)
	}

	if got := SessionKey(tenantID, okID); got == "" {
		t.Error("SessionKey refused a 256 byte client id")
	}

	// A decoded name over the cap is refused too, so decode never yields what the
	// encoders would refuse. Build the over-long name by hand.
	over := tenantID + "." + strings.Repeat("a.", MaxTopicBytes/2) + "a"
	if _, _, err := DecodeRetainedKey(over); !errors.Is(err, tether.ErrInvalidInput) {
		t.Errorf("DecodeRetainedKey of a 1025 byte topic = %v, want ErrInvalidInput", err)
	}
}

func TestEmptyTenantRefused(t *testing.T) {
	if QoS0("", "a") != "" || Log("", "a") != "" || RetainedKey("", "a") != "" || SessionKey("", "c") != "" || LogTenantAll("") != "" {
		t.Error("an empty tenant was accepted")
	}

	if QoS0Filters("", "a/#") != nil || RetainedKeyFilter("", "a/#") != nil {
		t.Error("an empty tenant was accepted for a filter")
	}
}

func TestClientIDToken(t *testing.T) {
	for _, id := range []string{"dev-1", "a.b", "x y", "", "=eq", "日本", "a/b", "*", ">"} {
		tok := EncodeLevel(id)
		if strings.ContainsAny(tok, ".*> /") {
			t.Errorf("client id %q gives token %q with a delimiter", id, tok)
		}

		if got, err := DecodeLevel(tok); err != nil || got != id {
			t.Errorf("client id %q round trip: %q, %v", id, got, err)
		}

		if key := SessionKey(tenantID, id); key != tenantID+"."+tok || !kvKeyValid(key) {
			t.Errorf("session key for %q is %q", id, key)
		}
	}
}

func TestDecodeRejects(t *testing.T) {
	for _, tok := range []string{
		"", "a.b", "a*", "a>", "a b", "a/b", "=!", "=YQ", // "=YQ" is "a", which must pass through
		"=YR", // non-canonical trailing bits
		"=YQ==", "==", "=Lg=", "é", "=\r", "=\n", "=Lg\n", "=L\rg",
	} {
		got, err := DecodeLevel(tok)
		if err == nil {
			t.Errorf("DecodeLevel(%q) = %q, want an error", tok, got)
		} else if !errors.Is(err, tether.ErrInvalidInput) {
			t.Errorf("DecodeLevel(%q) error %v does not wrap ErrInvalidInput", tok, err)
		}
	}

	for _, name := range []string{
		"", "t", "t.", ".a", "t..a", "t.a.", "==t.a", "t.=JFNZUw", "==t.x", "==.a", "t.a.*", "t.>", "==t",
		"t.=Lw", // a level holding "/" (the fuzz found =000v, which decodes to one)
		"=.a",   // an empty tenant
	} {
		if tenant, got, err := DecodeRetainedKey(name); err == nil {
			t.Errorf("DecodeRetainedKey(%q) = %q, %q, want an error", name, tenant, got)
		} else if !errors.Is(err, tether.ErrInvalidInput) {
			t.Errorf("DecodeRetainedKey(%q) error %v does not wrap ErrInvalidInput", name, err)
		}
	}

	// The log has no "$" space: "==" tenants are not log subjects, and a missing
	// prefix is refused.
	for _, subj := range []string{"d.==t.a", "f.t.a", "t.a", "d.", "d.t"} {
		if tenant, got, err := DecodeLog(subj); err == nil {
			t.Errorf("DecodeLog(%q) = %q, %q, want an error", subj, tenant, got)
		}
	}

	// DecodeQoS0 shares the retained key's "$" space rule and wants its prefix.
	for _, subj := range []string{"", "f.", "f.t", "d.t.a", "t.a", "f.==t.a", "f.t.=JFNZUw", "f.t.a.*", "f.t.>", "f.t.=Lw", "f.=.a"} {
		if tenant, got, err := DecodeQoS0(subj); err == nil {
			t.Errorf("DecodeQoS0(%q) = %q, %q, want an error", subj, tenant, got)
		} else if !errors.Is(err, tether.ErrInvalidInput) {
			t.Errorf("DecodeQoS0(%q) error %v does not wrap ErrInvalidInput", subj, err)
		}
	}

	if tenant, got, err := DecodeQoS0("f.==t.=JFNZUw"); err != nil || tenant != "t" || got != "$SYS" {
		t.Errorf("DecodeQoS0 reads the == space: %q, %q, %v", tenant, got, err)
	}

	if _, got, err := DecodeLog("d.t.=JFNZUw"); err != nil || got != "$SYS" {
		t.Errorf("DecodeLog keeps $ topics under the tenant: %q, %v", got, err)
	}
}

// TestEquivalenceExhaustive compares MQTT matching to NATS matching over every
// filter and topic up to three levels drawn from a hostile alphabet, on both the
// QoS 0 subjects and the retained keys.
func TestEquivalenceExhaustive(t *testing.T) {
	if testing.Short() {
		t.Skip("21 million pairs; skipped with -short")
	}

	levels := []string{"a", "b", "", "$SYS", "$x", ".", " ", "*", ">", "é", "=a", "=", "a.b", "😀"}
	wild := append([]string{"+"}, levels...)

	var topics []string

	filters := []string{"#"}

	var build func(prefix []string, depth int, alpha []string, out *[]string, hashOK bool)

	build = func(prefix []string, depth int, alpha []string, out *[]string, hashOK bool) {
		if len(prefix) > 0 {
			*out = append(*out, strings.Join(prefix, "/"))
			if hashOK {
				*out = append(*out, strings.Join(append(append([]string{}, prefix...), "#"), "/"))
			}
		}

		if depth == 0 {
			return
		}

		for _, l := range alpha {
			build(append(append([]string{}, prefix...), l), depth-1, alpha, out, hashOK)
		}
	}

	build(nil, 3, levels, &topics, false)
	build(nil, 3, wild, &filters, true)

	var pairs, matches, dollarBlocked int

	for _, f := range filters {
		if topic.Valid(f, true, 0) != nil {
			continue
		}

		qsubs := QoS0Filters(tenantID, f)
		rsubs := RetainedKeyFilter(tenantID, f)

		for _, name := range topics {
			if !mqttValid(f, name) {
				continue
			}

			pairs++

			want := topic.Match(f, name)
			subj := QoS0(tenantID, name)

			if got := matchAny(qsubs, subj); got != want {
				t.Fatalf("filter %q topic %q: mqtt %v, nats %v (%q vs %q)", f, name, want, got, qsubs, subj)
			}

			key := RetainedKey(tenantID, name)
			if got := matchAny(rsubs, key); got != want {
				t.Fatalf("filter %q topic %q: mqtt %v, kv %v (%q vs %q)", f, name, want, got, rsubs, key)
			}

			if want {
				matches++
			}

			if strings.HasPrefix(name, "$") && (strings.HasPrefix(f, "+") || strings.HasPrefix(f, "#")) {
				dollarBlocked++
			}
		}
	}

	t.Logf("compared %d filter/topic pairs, %d match, %d are $-topics behind a wildcard-first filter", pairs, matches, dollarBlocked)
}

// TestDeriveMatchRate guards the fuzz generator: derive must make matches common,
// or FuzzWildcardEquivalence would mostly compare false against false.
func TestDeriveMatchRate(t *testing.T) {
	var pairs, matches int

	for _, f := range filterSeeds {
		for _, tp := range topicSeeds {
			for _, mix := range []byte{1, 3} {
				name := derive(f, tp, mix)
				if !mqttValid(f, name) {
					continue
				}

				pairs++

				if topic.Match(f, name) {
					matches++
				}
			}
		}
	}

	t.Logf("derived pairs %d, matching %d", pairs, matches)

	if pairs == 0 || matches*5 < pairs {
		t.Fatalf("only %d of %d derived pairs match", matches, pairs)
	}
}

func TestLengthBound(t *testing.T) {
	// The worst case is a topic of single-byte levels that all need encoding, or
	// of empty levels: 2n+1 bytes for n bytes.
	for _, tp := range []string{strings.Repeat("/", 1024), strings.Repeat(" /", 512)[:1023], strings.Repeat("é/", 341)[:1023]} {
		subj := Log(tenantID, tp)
		body := subj[len("d.")+len(tenantID)+1:]
		t.Logf("%d byte topic -> %d byte topic part (ratio %.3f), subject %d bytes", len(tp), len(body), float64(len(body))/float64(len(tp)), len(subj))

		if len(body) > 2*len(tp)+1 {
			t.Fatalf("bound broken: %d -> %d", len(tp), len(body))
		}
	}
}
