// Package topic implements tenant namespacing and MQTT topic matching.
//
// The broker rewrites every inbound topic name and filter with [Namespace] and
// every outbound topic name with [Strip], so two tenants never share a topic
// even when their devices pick the same names. The package is pure: it knows
// nothing about the broker, the store or forge.
//
// Names are split on "/" into levels. A tenant ID (a TypeID such as
// "ttnt_01h...") holds only lowercase letters, digits and underscores, so it is
// always a single valid MQTT level.
package topic

import (
	"fmt"
	"strings"

	"github.com/xraph/tether"
)

const (
	sep   = "/"
	share = "$share"
)

// Namespace places name inside tenant's topic space.
//
// A plain name becomes tenant + "/" + name. A name whose first level starts with
// "$" gets the tenant inserted after that level, so "$SYS/x" becomes
// "$SYS/<tenant>/x". That keeps the MQTT rule that a filter starting with a
// wildcard never matches a "$" topic true after namespacing, and keeps the
// broker's own global $SYS topics out of every tenant's space. A shared
// subscription "$share/<group>/<filter>" keeps its "$share/<group>/" prefix and
// the filter part is namespaced like any other name.
//
// Namespace expects input that passed [Valid]. It never fails, but on invalid
// input (a "$share/" name without a group and a filter) the result is
// unspecified.
func Namespace(tenant, name string) string {
	if prefix, filter, ok := cutShare(name); ok {
		return prefix + namespacePlain(tenant, filter)
	}

	return namespacePlain(tenant, name)
}

// cutShare splits a shared subscription "$share/<group>/<filter>" into its
// "$share/<group>/" prefix, with the client's own casing, and the filter. The
// "$share" level is matched case-insensitively because mochi does the same: a
// "$SHARE/g/x" filter is a shared subscription there, and it has to be
// namespaced as one, or the group would be read as the filter's first level.
func cutShare(name string) (prefix, filter string, ok bool) {
	first, rest, more := strings.Cut(name, sep)
	if !more || !strings.EqualFold(first, share) {
		return "", "", false
	}

	_, filter, found := strings.Cut(rest, sep)
	if !found {
		return "", "", false
	}

	return name[:len(name)-len(filter)], filter, true
}

// isShare reports whether a topic level is the reserved "$share" level, in any casing.
func isShare(level string) bool {
	return strings.EqualFold(level, share)
}

func namespacePlain(tenant, name string) string {
	if !strings.HasPrefix(name, "$") {
		return tenant + sep + name
	}

	first, rest, more := strings.Cut(name, sep)
	if !more {
		return first + sep + tenant
	}

	return first + sep + tenant + sep + rest
}

// Strip is the exact inverse of [Namespace]. It returns false when name is not
// in tenant's namespace, for example another tenant's topic or one of the
// broker's own global "$SYS" topics.
func Strip(tenant, name string) (string, bool) {
	if isShare(firstLevel(name)) {
		prefix, filter, ok := cutShare(name)
		if !ok {
			return "", false
		}

		stripped, ok := stripPlain(tenant, filter)
		if !ok {
			return "", false
		}

		return prefix + stripped, true
	}

	return stripPlain(tenant, name)
}

func stripPlain(tenant, name string) (string, bool) {
	if strings.HasPrefix(name, "$") {
		first, rest, more := strings.Cut(name, sep)
		if !more {
			return "", false
		}

		second, tail, more := strings.Cut(rest, sep)
		if second != tenant {
			return "", false
		}

		if !more {
			return first, true
		}

		return first + sep + tail, true
	}

	if len(name) <= len(tenant) || name[len(tenant)] != '/' || !strings.HasPrefix(name, tenant) {
		return "", false
	}

	rest := name[len(tenant)+1:]
	// Namespace never puts a "$" level right after the tenant, so a name that
	// does was not produced by it.
	if strings.HasPrefix(rest, "$") {
		return "", false
	}

	return rest, true
}

// Match reports whether the topic name matches the filter, following MQTT
// rules: "+" matches exactly one level (an empty one included), "#" matches
// the parent level and everything below it, and a filter whose first level is
// "+" or "#" never matches a name whose first level starts with "$".
//
// Match works on plain names. Callers strip "$share/<group>/" from a shared
// filter first. A "#" anywhere but the last level matches nothing.
func Match(filter, name string) bool {
	if strings.HasPrefix(name, "$") && wildcardFirst(filter) {
		return false
	}

	for {
		fl, frest, fmore := strings.Cut(filter, sep)
		nl, nrest, nmore := strings.Cut(name, sep)

		if fl == "#" {
			return !fmore
		}

		if fl != "+" && fl != nl {
			return false
		}

		switch {
		case !fmore && !nmore:
			return true
		case !fmore:
			return false
		case !nmore:
			// The name ended; only a trailing "#" can still match, as the parent.
			return frest == "#"
		}

		filter, name = frest, nrest
	}
}

// Covers reports whether every topic name that filter can match is also matched
// by grant. It is how the ACL layer decides that a subscription fits inside a
// role's allowed filters.
//
// Grant "#" covers everything below its position (and the parent), a grant "+"
// covers a "+" or a literal in that position, a literal covers only the same
// literal, and a filter "#" is covered only by a grant "#" at that level. The
// "$" rule from [Match] applies: a grant whose first level is "+" or "#" does
// not cover a filter whose first level starts with "$".
//
// Covers works on plain filters. Callers strip "$share/<group>/" first.
func Covers(grant, filter string) bool {
	if strings.HasPrefix(filter, "$") && wildcardFirst(grant) {
		return false
	}

	for {
		gl, grest, gmore := strings.Cut(grant, sep)
		fl, frest, fmore := strings.Cut(filter, sep)

		if gl == "#" {
			return !gmore
		}

		if fl == "#" {
			return false
		}

		if gl != "+" && gl != fl {
			return false
		}

		switch {
		case !gmore && !fmore:
			return true
		case !gmore:
			return false
		case !fmore:
			// The filter ended; the grant covers it only through a trailing "#".
			return grest == "#"
		}

		grant, filter = grest, frest
	}
}

// wildcardFirst reports whether the first level of filter is "+" or "#".
func wildcardFirst(filter string) bool {
	first, _, _ := strings.Cut(filter, sep)

	return first == "+" || first == "#"
}

// Valid checks a topic name (isFilter false) or a topic filter (isFilter true).
// maxLen caps the length in bytes; 0 means no limit. Every error wraps
// [tether.ErrInvalidInput].
//
// A name must be non-empty, hold no NUL and, as a name, no "+" or "#". A filter
// may use "+" and "#" only as whole levels, with "#" last. A shared filter
// "$share/<group>/<filter>" needs a non-empty group without wildcards and a
// valid, non-empty filter part that is not itself shared. A topic name may not
// start with the reserved "$share" level, which only subscriptions use. The
// level is matched in any casing, as mochi does.
func Valid(name string, isFilter bool, maxLen int) error {
	switch {
	case name == "":
		return invalid("empty")
	case maxLen > 0 && len(name) > maxLen:
		return invalid(fmt.Sprintf("longer than %d bytes", maxLen))
	case strings.IndexByte(name, 0) >= 0:
		return invalid("contains NUL")
	}

	if !isFilter {
		if strings.ContainsAny(name, "+#") {
			return invalid("topic name contains a wildcard")
		}

		if isShare(firstLevel(name)) {
			return invalid("topic name uses the reserved $share level")
		}

		return nil
	}

	if first, rest, more := strings.Cut(name, sep); isShare(first) {
		if !more {
			return invalid("shared filter needs a group and a filter")
		}

		group, filter, found := strings.Cut(rest, sep)

		switch {
		case !found || filter == "":
			return invalid("shared filter needs a group and a filter")
		case group == "":
			return invalid("shared filter has an empty group")
		case strings.ContainsAny(group, "+#"):
			return invalid("shared filter group contains a wildcard")
		case isShare(firstLevel(filter)):
			return invalid("shared filter nests $share")
		}

		name = filter
	}

	return validFilterLevels(name)
}

func validFilterLevels(filter string) error {
	for {
		level, rest, more := strings.Cut(filter, sep)

		switch {
		case strings.Contains(level, "#"):
			if level != "#" {
				return invalid("# must occupy a whole level")
			}

			if more {
				return invalid("# must be the last level")
			}
		case strings.Contains(level, "+") && level != "+":
			return invalid("+ must occupy a whole level")
		}

		if !more {
			return nil
		}

		filter = rest
	}
}

func firstLevel(name string) string {
	first, _, _ := strings.Cut(name, sep)

	return first
}

func invalid(reason string) error {
	return fmt.Errorf("%w: topic %s", tether.ErrInvalidInput, reason)
}
