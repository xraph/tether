package topic_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/topic"
)

func FuzzNamespaceRoundTrip(f *testing.F) {
	for _, seed := range []string{
		"sensors/1/temp", "#", "+", "a//b", "/", "/a", "a/", "$SYS", "$SYS/broker/load",
		"$share/g/cmd/+", "$share/g/$SYS/x", "$sharex/g", "ttnt_a/x", "ttnt_b/x", "$SYS/ttnt_a/x",
		"$SHARE/ttnt_b/#", "$Share/g/x", "$sHARE/g/$SYS/x",
	} {
		f.Add(seed, true)
		f.Add(seed, false)
	}

	f.Fuzz(func(t *testing.T, x string, isFilter bool) {
		if topic.Valid(x, isFilter, 0) != nil {
			t.Skip()
		}

		ns := topic.Namespace("ttnt_a", x)

		got, ok := topic.Strip("ttnt_a", ns)
		require.True(t, ok, "Strip(%q)", ns)
		require.Equal(t, x, got)

		// Another tenant never sees this tenant's names.
		_, ok = topic.Strip("ttnt_b", ns)
		require.False(t, ok, "tenant b stripped %q", ns)

		// The namespaced form stays valid, so the broker can hand it on.
		require.NoError(t, topic.Valid(ns, isFilter, 0))
	})
}

func FuzzCoversImpliesMatch(f *testing.F) {
	for _, seed := range [][3]string{
		{"a/#", "a/+/c", "a/b/c"},
		{"a/+/c", "a/b/c", "a/b/c"},
		{"#", "a/b", "a/b"},
		{"#", "$SYS/#", "$SYS/x"},
		{"+/x", "$SYS/x", "$SYS/x"},
		{"a/#", "a", "a"},
		{"a/+", "a/#", "a/b"},
		{"+", "+", "$a"},
		{"$SYS/#", "$SYS/+", "$SYS/x"},
		{"a//b", "a/+/b", "a//b"},
	} {
		f.Add(seed[0], seed[1], seed[2])
	}

	f.Fuzz(func(t *testing.T, g, filter, n string) {
		for _, s := range []string{g, filter} {
			if topic.Valid(s, true, 0) != nil || strings.HasPrefix(s, "$share") {
				t.Skip()
			}
		}

		if topic.Valid(n, false, 0) != nil {
			t.Skip()
		}

		if topic.Covers(g, filter) && topic.Match(filter, n) {
			require.True(t, topic.Match(g, n), "Covers(%q, %q) and Match(%q, %q) but not Match(%q, %q)", g, filter, filter, n, g, n)
		}
	})
}

// mochiEffectiveFilter returns the filter mochi matches against its topic
// index. It detects a shared subscription with a case-insensitive "$share"
// first level and starts the filter at level three (mochi topics.go,
// isolateParticle with depth 2); anything else is used whole.
func mochiEffectiveFilter(filter string) string {
	levels := strings.Split(filter, "/")
	if strings.EqualFold(levels[0], "$share") && len(levels) >= 3 {
		return strings.Join(levels[2:], "/")
	}

	return filter
}

// FuzzNamespaceIsolation checks, the way mochi reads the namespaced filter,
// that the effective filter always sits inside the tenant's space: its first
// level is the tenant, or, for a "$" first level, its second level is.
func FuzzNamespaceIsolation(f *testing.F) {
	for _, seed := range []string{
		"#", "+", "+/#", "a/b", "$SYS/#", "$SYS", "$share/g/#", "$SHARE/ttnt_b/#", "$Share/g/x",
		"$sHaRe/ttnt_b/+/x", "$SHARE/ttnt_b/$SYS/#", "$SHARE/g/", "$SHARE", "$sharex/ttnt_b/#",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, x string) {
		if topic.Valid(x, true, 0) != nil {
			t.Skip()
		}

		effective := mochiEffectiveFilter(topic.Namespace("ttnt_a", x))
		levels := strings.Split(effective, "/")

		switch {
		case strings.HasPrefix(levels[0], "$"):
			require.GreaterOrEqual(t, len(levels), 2, "effective filter %q", effective)
			require.Equal(t, "ttnt_a", levels[1], "effective filter %q for %q", effective, x)
		default:
			require.Equal(t, "ttnt_a", levels[0], "effective filter %q for %q", effective, x)
		}
	})
}
