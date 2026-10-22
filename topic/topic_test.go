package topic_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xraph/tether"
	"github.com/xraph/tether/topic"
)

func TestNamespaceStrip(t *testing.T) {
	cases := []struct{ in, ns string }{
		{"sensors/1/temp", "ttnt_a/sensors/1/temp"},
		{"#", "ttnt_a/#"},
		{"$SYS/broker/load", "$SYS/ttnt_a/broker/load"},
		{"$share/g/cmd/+", "$share/g/ttnt_a/cmd/+"},
		{"a//b", "ttnt_a/a//b"},
		{"$SYS", "$SYS/ttnt_a"},
		{"$share/g/$SYS/x", "$share/g/$SYS/ttnt_a/x"},
		{"/a", "ttnt_a//a"},
	}
	for _, c := range cases {
		require.Equal(t, c.ns, topic.Namespace("ttnt_a", c.in))

		got, ok := topic.Strip("ttnt_a", c.ns)
		require.True(t, ok, c.ns)
		require.Equal(t, c.in, got)
	}

	_, ok := topic.Strip("ttnt_a", "ttnt_b/x")
	require.False(t, ok)

	_, ok = topic.Strip("ttnt_a", "$SYS/broker/load") // mochi's global $SYS
	require.False(t, ok)
}

func TestStripRejectsForeignNames(t *testing.T) {
	for _, ns := range []string{
		"",
		"ttnt_a",         // no level after the tenant
		"ttnt_ab/x",      // tenant is only a prefix of the first level
		"ttnt_a/$SYS/x",  // Namespace never produces a $ level after the tenant
		"$SYS/ttnt_b/x",  // another tenant's $ topic
		"$SYS/ttnt_ab/x", // tenant is only a prefix of the second level
		"$SYS/x",
		"$share/g",
		"$share/g/ttnt_b/x",
		"$share/g/$SYS/x",
		"$share",
	} {
		got, ok := topic.Strip("ttnt_a", ns)
		require.False(t, ok, "%q stripped to %q", ns, got)
	}
}

func TestMatch(t *testing.T) {
	require.True(t, topic.Match("a/#", "a"))
	require.True(t, topic.Match("a/+/c", "a/b/c"))
	require.False(t, topic.Match("#", "$SYS/x"))
	require.False(t, topic.Match("+/x", "$SYS/x"))
	require.True(t, topic.Match("$SYS/#", "$SYS/x"))
}

func TestMatchTable(t *testing.T) {
	cases := []struct {
		filter, name string
		want         bool
	}{
		{"a/b", "a/b", true},
		{"a/b", "a/c", false},
		{"a/b", "a", false},
		{"a", "a/b", false},
		{"#", "a", true},
		{"#", "a/b/c", true},
		{"a/#", "a/b/c", true},
		{"a/#", "a", true},
		{"a/#", "ab", false},
		{"a/+", "a", false},
		{"a/+", "a/b", true},
		{"a/+", "a/b/c", false},
		{"+", "a", true},
		{"+", "a/b", false},
		{"+/+", "a", false},
		{"+/b", "a/b", true},
		{"a/+/c", "a//c", true},
		{"a/+", "a/", true},
		{"#", "$SYS", false},
		{"+", "$SYS", false},
		{"#", "$a/b", false},
		{"+/#", "$a/b", false},
		{"$SYS/+", "$SYS/x", true},
		{"$SYS/#", "$SYS", true},
		{"$SYS/+", "x/$SYS", false},
		{"a/#", "a/$b", true},
		{"a/+/c", "a/$b/c", true},
		{"a/#/c", "a/b/c", false},
	}
	for _, c := range cases {
		require.Equal(t, c.want, topic.Match(c.filter, c.name), "Match(%q, %q)", c.filter, c.name)
	}
}

func TestCovers(t *testing.T) {
	require.True(t, topic.Covers("a/#", "a/+/c"))
	require.True(t, topic.Covers("a/+/c", "a/b/c"))
	require.False(t, topic.Covers("a/+/c", "a/#"))
	require.False(t, topic.Covers("a/b", "a/+"))
	require.True(t, topic.Covers("#", "a/b"))
	require.False(t, topic.Covers("#", "$SYS/x"))
}

func TestCoversTable(t *testing.T) {
	cases := []struct {
		grant, filter string
		want          bool
	}{
		{"a/b", "a/b", true},
		{"a/b", "a/c", false},
		{"a/b", "a", false},
		{"a", "a/b", false},
		{"a/#", "a", true},
		{"a/#", "a/#", true},
		{"a/#", "a/b/#", true},
		{"a/b/#", "a/#", false},
		{"a/+", "a/#", false},
		{"a/+", "a/+", true},
		{"a/+", "a/b", true},
		{"a/+", "a/b/c", false},
		{"a/+", "a", false},
		{"+", "+", true},
		{"+", "a", true},
		{"+", "#", false},
		{"+/b", "a/b", true},
		{"+/b", "+/b", true},
		{"+/b", "+/+", false},
		{"#", "#", true},
		{"#", "+", true},
		{"#", "+/#", true},
		{"#", "$SYS", false},
		{"+", "$SYS", false},
		{"+/x", "$SYS/x", false},
		{"#", "$SYS/#", false},
		{"$SYS/#", "$SYS/x", true},
		{"$SYS/+", "$SYS/x", true},
		{"$SYS/#", "#", false},
		{"a/#", "a/$b", true},
	}
	for _, c := range cases {
		require.Equal(t, c.want, topic.Covers(c.grant, c.filter), "Covers(%q, %q)", c.grant, c.filter)
	}
}

func TestValid(t *testing.T) {
	long := strings.Repeat("a", 1024)

	cases := []struct {
		name     string
		isFilter bool
		maxLen   int
		ok       bool
	}{
		{"a/b/c", false, 0, true},
		{"a//b", false, 0, true},
		{"/", false, 0, true},
		{"$SYS/x", false, 0, true},
		{"", false, 0, false},
		{"", true, 0, false},
		{"a\x00b", false, 0, false},
		{"a\x00b", true, 0, false},
		{"a/+", false, 0, false},
		{"a/#", false, 0, false},
		{"a+", false, 0, false},
		{"a/+", true, 0, true},
		{"a/#", true, 0, true},
		{"#", true, 0, true},
		{"+", true, 0, true},
		{"+/+/#", true, 0, true},
		{"a/#/b", true, 0, false},
		{"a#", true, 0, false},
		{"a+/b", true, 0, false},
		{"a/b+", true, 0, false},
		{"##", true, 0, false},
		{"a/#/", true, 0, false},
		{long, false, 1024, true},
		{long + "a", false, 1024, false},
		{long + "a", false, 0, true},
		{"abc", true, 2, false},
		{"$share/g/cmd/+", true, 0, true},
		{"$share/g/#", true, 0, true},
		{"$share/g/$SYS/x", true, 0, true},
		{"$share/g", true, 0, false},
		{"$share/g/", true, 0, false},
		{"$share//x", true, 0, false},
		{"$share", true, 0, false},
		{"$share/", true, 0, false},
		{"$share/+/x", true, 0, false},
		{"$share/#/x", true, 0, false},
		{"$share/g+/x", true, 0, false},
		{"$share/g/a/#/b", true, 0, false},
		{"$share/g/x", false, 0, false},
		{"$share", false, 0, false},
		{"$sharex/g/x", false, 0, true},
	}
	for _, c := range cases {
		err := topic.Valid(c.name, c.isFilter, c.maxLen)
		if c.ok {
			require.NoError(t, err, "Valid(%q, %v, %d)", c.name, c.isFilter, c.maxLen)

			continue
		}

		require.ErrorIs(t, err, tether.ErrInvalidInput, "Valid(%q, %v, %d)", c.name, c.isFilter, c.maxLen)
	}
}
