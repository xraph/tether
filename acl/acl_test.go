package acl_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/acl"
)

func TestPlaceholderExpansion(t *testing.T) {
	s := acl.Compile(acl.Rules{Publish: []string{"devices/%c/up"}, Subscribe: []string{"devices/%c/cmd/#"}},
		acl.Vars{ClientID: "pump-1", Username: "akey_x"})
	require.True(t, s.CanPublish("devices/pump-1/up"))
	require.False(t, s.CanPublish("devices/pump-2/up"))
	require.True(t, s.CanSubscribe("devices/pump-1/cmd/+"))
}

func TestPlaceholderInjectionDropsGrant(t *testing.T) { // Review Focus 5
	for _, bad := range []string{"a/b", "+", "#", "x/#", "", "a\x00b"} {
		s := acl.Compile(acl.Rules{Subscribe: []string{"devices/%c/#"}}, acl.Vars{ClientID: bad})
		require.False(t, s.CanSubscribe("devices/a/b/c"), bad)
		require.False(t, s.CanSubscribe("devices/+/#"), bad)
		require.Len(t, s.Dropped(), 1, bad)
	}
}

func TestPlaceholderInjectionDropsPublishGrant(t *testing.T) {
	for _, bad := range []string{"a/b", "+", "#", "", "a\x00b"} {
		s := acl.Compile(acl.Rules{Publish: []string{"devices/%c/up"}}, acl.Vars{ClientID: bad})
		require.False(t, s.CanPublish("devices/x/up"), bad)
		require.False(t, s.CanPublish("devices//up"), bad)
		require.Equal(t, []string{"devices/%c/up"}, s.Dropped(), bad)
	}
}

func TestSubscribeNeedsCoverage(t *testing.T) {
	s := acl.Compile(acl.Rules{Subscribe: []string{"telemetry/+/temp"}}, acl.Vars{})
	require.True(t, s.CanSubscribe("telemetry/7/temp"))
	require.False(t, s.CanSubscribe("telemetry/#"))
}

func TestUsernamePlaceholderAndRepeats(t *testing.T) {
	s := acl.Compile(acl.Rules{
		Publish:   []string{"u/%u/c/%c", "both/%c/%c"},
		Subscribe: []string{"%u/#"},
	}, acl.Vars{ClientID: "c1", Username: "akey_1"})
	require.True(t, s.CanPublish("u/akey_1/c/c1"))
	require.True(t, s.CanPublish("both/c1/c1"))
	require.False(t, s.CanPublish("u/c1/c/akey_1"))
	require.True(t, s.CanSubscribe("akey_1/a/b"))
	require.Empty(t, s.Dropped())
}

func TestPlaceholderInsideLevel(t *testing.T) {
	s := acl.Compile(acl.Rules{Publish: []string{"dev-%c-x/up"}}, acl.Vars{ClientID: "p1"})
	require.True(t, s.CanPublish("dev-p1-x/up"))
	require.False(t, s.CanPublish("dev-p2-x/up"))
}

func TestBadValueOnlyDropsGrantsThatUseIt(t *testing.T) {
	s := acl.Compile(acl.Rules{
		Publish:   []string{"a/%u", "b/%c", "static/up"},
		Subscribe: []string{"s/%u/#", "t/%c/#", "static/#"},
	}, acl.Vars{ClientID: "ok", Username: "bad/user"})
	require.True(t, s.CanPublish("b/ok"))
	require.True(t, s.CanPublish("static/up"))
	require.False(t, s.CanPublish("a/bad/user"))
	require.True(t, s.CanSubscribe("t/ok/x"))
	require.True(t, s.CanSubscribe("static/x"))
	require.Equal(t, []string{"a/%u", "s/%u/#"}, s.Dropped())
}

func TestLiteralPercentIsKept(t *testing.T) {
	s := acl.Compile(acl.Rules{
		Publish:   []string{"100%/up", "x/%d/y", "trail/%"},
		Subscribe: []string{"%%c/#"},
	}, acl.Vars{ClientID: "c1"})
	require.True(t, s.CanPublish("100%/up"))
	require.True(t, s.CanPublish("x/%d/y"))
	require.True(t, s.CanPublish("trail/%"))
	require.True(t, s.CanSubscribe("%c1/a"), "%% is a literal % followed by an expanded %c")
	require.Empty(t, s.Dropped())
}

func TestInvalidExpandedGrantIsDropped(t *testing.T) {
	grants := []string{"", "a/#/b", "a/b+", "a/\x00", "$share/g/x", "$SHARE/g/x"}
	s := acl.Compile(acl.Rules{Publish: grants, Subscribe: grants}, acl.Vars{})
	require.False(t, s.CanSubscribe("a/x"))
	require.False(t, s.CanSubscribe("x"))
	require.False(t, s.CanPublish("a/b"))
	require.Len(t, s.Dropped(), 2*len(grants))
}

func TestDroppedKeepsOriginalGrantsInOrder(t *testing.T) {
	s := acl.Compile(acl.Rules{
		Publish:   []string{"p/%c", "p/ok"},
		Subscribe: []string{"s/%c/#", "bad/#/x"},
	}, acl.Vars{ClientID: ""})
	require.Equal(t, []string{"p/%c", "s/%c/#", "bad/#/x"}, s.Dropped())
}

func TestDroppedIsACopy(t *testing.T) {
	s := acl.Compile(acl.Rules{Publish: []string{"p/%c"}}, acl.Vars{})
	d := s.Dropped()
	d[0] = "mutated"
	require.Equal(t, []string{"p/%c"}, s.Dropped())
}

func TestCompileDoesNotAliasRules(t *testing.T) {
	r := acl.Rules{Publish: []string{"a/b"}}
	s := acl.Compile(r, acl.Vars{})
	r.Publish[0] = "x/y"
	require.True(t, s.CanPublish("a/b"))
	require.False(t, s.CanPublish("x/y"))
}

func TestEmptyAndNilSetsAllowNothing(t *testing.T) {
	for name, s := range map[string]*acl.Set{
		"empty": acl.Compile(acl.Rules{}, acl.Vars{}),
		"nil":   nil,
	} {
		require.False(t, s.CanPublish("a"), name)
		require.False(t, s.CanSubscribe("a"), name)
		require.False(t, s.CanSubscribe("#"), name)
		require.Empty(t, s.Dropped(), name)
	}
}

func TestPublishUsesMatch(t *testing.T) {
	s := acl.Compile(acl.Rules{Publish: []string{"sensors/+/temp", "logs/#"}}, acl.Vars{})
	require.True(t, s.CanPublish("sensors/7/temp"))
	require.False(t, s.CanPublish("sensors/7/hum"))
	require.True(t, s.CanPublish("logs"))
	require.True(t, s.CanPublish("logs/a/b"))
	require.False(t, s.CanPublish("other"))
}

func TestPublishRejectsWildcardOrEmptyNames(t *testing.T) {
	s := acl.Compile(acl.Rules{Publish: []string{"sensors/+/temp", "#"}}, acl.Vars{})
	require.False(t, s.CanPublish("sensors/+/temp"))
	require.False(t, s.CanPublish("a/#"))
	require.False(t, s.CanPublish(""))
}

func TestDollarTopicsNeedExplicitGrant(t *testing.T) {
	s := acl.Compile(acl.Rules{Publish: []string{"#"}, Subscribe: []string{"#", "$SYS/mine/#"}}, acl.Vars{})
	require.False(t, s.CanPublish("$SYS/x"))
	require.False(t, s.CanSubscribe("$SYS/x"))
	require.True(t, s.CanSubscribe("$SYS/mine/x"))
	require.True(t, s.CanSubscribe("a/b"))
}

func TestSubscribeServesDeliveryTopics(t *testing.T) {
	s := acl.Compile(acl.Rules{Subscribe: []string{"a/+/c", "x/#"}}, acl.Vars{})
	require.True(t, s.CanSubscribe("a/b/c"))
	require.False(t, s.CanSubscribe("a/b/d"))
	require.True(t, s.CanSubscribe("x"))
	require.True(t, s.CanSubscribe("x/y/z"))
	require.True(t, s.CanSubscribe("x/+"))
	require.False(t, s.CanSubscribe("#"))
}

func TestMerge(t *testing.T) {
	m := acl.Merge(
		acl.Rules{Publish: []string{"a", "b"}, Subscribe: []string{"s1"}},
		acl.Rules{Publish: []string{"b", "c", "a"}, Subscribe: []string{"s2", "s1"}},
		acl.Rules{},
	)
	require.Equal(t, []string{"a", "b", "c"}, m.Publish)
	require.Equal(t, []string{"s1", "s2"}, m.Subscribe)
}

func TestMergeEmpty(t *testing.T) {
	m := acl.Merge()
	require.Empty(t, m.Publish)
	require.Empty(t, m.Subscribe)

	m = acl.Merge(acl.Rules{}, acl.Rules{})
	require.Empty(t, m.Publish)
	require.Empty(t, m.Subscribe)
}

func TestMergeDoesNotAliasInputs(t *testing.T) {
	in := acl.Rules{Publish: []string{"a"}}
	m := acl.Merge(in)
	m.Publish[0] = "z"
	require.Equal(t, []string{"a"}, in.Publish)
}

func BenchmarkChecks(b *testing.B) {
	s := acl.Compile(acl.Rules{
		Publish:   []string{"devices/%c/up", "devices/%c/state/#"},
		Subscribe: []string{"devices/%c/cmd/#", "broadcast/+"},
	}, acl.Vars{ClientID: "pump-1", Username: "akey_x"})

	b.ReportAllocs()

	for b.Loop() {
		_ = s.CanPublish("devices/pump-1/state/a/b")
		_ = s.CanSubscribe("devices/pump-1/cmd/+")
		_ = s.CanSubscribe("broadcast/x")
	}
}
