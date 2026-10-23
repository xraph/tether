package encoding

import (
	"strings"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
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
			subj := EncodeTopic(tenantID, tc.topic)

			head, body, _ := strings.Cut(subj, ".")
			wantHead := tenantID
			if tc.dollr {
				wantHead = "==" + tenantID
			}

			if head != wantHead || body != tc.want {
				t.Fatalf("EncodeTopic(%q) = %q, want %q.%s", tc.topic, subj, wantHead, tc.want)
			}

			tenant, got, err := DecodeTopic(subj)
			if err != nil || tenant != tenantID || got != tc.topic {
				t.Fatalf("DecodeTopic(%q) = %q, %q, %v", subj, tenant, got, err)
			}

			if !server.IsValidPublishSubject("f."+subj) || !kvKeyValid("r."+subj) {
				t.Fatalf("%q is not a valid subject and KV key", subj)
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
		got := EncodeFilter(tenantID, tc.filter)
		head := tenantID

		if tc.dollr {
			head = "==" + tenantID
		}

		if len(got) != len(tc.want) {
			t.Fatalf("EncodeFilter(%q) = %q, want %v", tc.filter, got, tc.want)
		}

		for i := range got {
			if got[i] != head+"."+tc.want[i] {
				t.Errorf("EncodeFilter(%q)[%d] = %q, want %q", tc.filter, i, got[i], head+"."+tc.want[i])
			}
		}
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

		if !kvKeyValid("s." + tenantID + "." + tok) {
			t.Errorf("session key for %q is invalid", id)
		}
	}
}

func TestDecodeRejects(t *testing.T) {
	for _, tok := range []string{
		"", "a.b", "a*", "a>", "a b", "a/b", "=!", "=YQ", // "=YQ" is "a", which must pass through
		"=YR", // non-canonical trailing bits
		"=YQ==", "==", "=Lg=", "é", "=\r", "=\n", "=Lg\n", "=L\rg",
	} {
		if got, err := DecodeLevel(tok); err == nil {
			t.Errorf("DecodeLevel(%q) = %q, want an error", tok, got)
		}
	}

	for _, subj := range []string{
		"", "t", "t.", ".a", "t..a", "t.a.", "==t.a", "t.=JFNZUw", "==t.x", "==.a", "t.a.*", "t.>", "==t",
		"t.=Lw", // a level holding "/" (the fuzz found =000v, which decodes to one)
	} {
		if tenant, name, err := DecodeTopic(subj); err == nil {
			t.Errorf("DecodeTopic(%q) = %q, %q, want an error", subj, tenant, name)
		}
	}
}

// TestEquivalenceExhaustive compares MQTT matching to NATS matching over every
// filter and topic up to three levels drawn from a hostile alphabet.
func TestEquivalenceExhaustive(t *testing.T) {
	levels := []string{"a", "b", "", "$SYS", "$x", ".", " ", "*", ">", "é", "=a", "=", "a.b", "😀"}
	wild := append([]string{"+"}, levels...)

	var topics, filters []string

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
	filters = append(filters, "#")

	var pairs, matches, dollarBlocked int

	for _, f := range filters {
		if topic.Valid(f, true, 0) != nil {
			continue
		}

		subs := EncodeFilter(tenantID, f)

		for _, name := range topics {
			if !mqttValid(f, name) {
				continue
			}

			pairs++

			want := topic.Match(f, name)
			subj := EncodeTopic(tenantID, name)

			if got := matchAny(subs, subj); got != want {
				t.Fatalf("filter %q topic %q: mqtt %v, nats %v (%q vs %q)", f, name, want, got, subs, subj)
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
		subj := EncodeTopic(tenantID, tp)
		body := subj[strings.Index(subj, ".")+1:]
		t.Logf("%d byte topic -> %d byte topic part (ratio %.3f), subject %d bytes", len(tp), len(body), float64(len(body))/float64(len(tp)), len("d.")+len(subj))

		if len(body) > 2*len(tp)+1 {
			t.Fatalf("bound broken: %d -> %d", len(tp), len(body))
		}
	}
}
