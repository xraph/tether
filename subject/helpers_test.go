package subject

import (
	"regexp"
	"strings"

	"github.com/xraph/tether/topic"
)

// tenantID is a TypeID-shaped tenant, as the broker uses.
const tenantID = "ttnt_01h2xcejqtf2nbrexx3vqjhp41"

// natsMatch is this package's own NATS wildcard matcher: tokens split on ".", "*"
// matches exactly one token, ">" matches one or more trailing tokens and is only
// a wildcard as the last filter token, every other token matches itself. The
// published subject is a literal.
func natsMatch(filter, subject string) bool {
	ft := strings.Split(filter, ".")
	st := strings.Split(subject, ".")

	for i, f := range ft {
		if f == ">" && i == len(ft)-1 {
			return len(st) > i
		}

		if i >= len(st) {
			return false
		}

		if f != "*" && f != st[i] {
			return false
		}
	}

	return len(ft) == len(st)
}

func matchAny(filters []string, subject string) bool {
	for _, f := range filters {
		if natsMatch(f, subject) {
			return true
		}
	}

	return false
}

// The jetstream KV key rules from nats.go jetstream/kv.go (keyValid and
// searchKeyValid, which are unexported): the regex plus no leading or trailing
// ".", no "..".
var (
	kvKeyRe    = regexp.MustCompile(`^[-/_=\.a-zA-Z0-9]+$`)
	kvSearchRe = regexp.MustCompile(`^[-/_=\.a-zA-Z0-9*]*[>]?$`)
)

func kvDots(key string) bool {
	return key != "" && key[0] != '.' && key[len(key)-1] != '.' && !strings.Contains(key, "..")
}

func kvKeyValid(key string) bool    { return kvDots(key) && kvKeyRe.MatchString(key) }
func kvSearchValid(key string) bool { return kvDots(key) && kvSearchRe.MatchString(key) }

// mqttValid reports whether filter and name are a legal pair for the MQTT
// reference. A "$share" filter is out: callers strip it before encoding.
func mqttValid(filter, name string) bool {
	return topic.Valid(filter, true, 0) == nil && topic.Valid(name, false, 0) == nil &&
		!strings.EqualFold(firstLevel(filter), "$share")
}

func firstLevel(s string) string {
	first, _, _ := strings.Cut(s, "/")

	return first
}

// derive builds a topic from the fuzzed filter and topic so that matches are
// common: with mix bit 0 set, each "+" takes the matching level of topic and a
// "#" takes the rest of topic (or nothing, the parent case, with mix bit 1 set).
func derive(filter, name string, mix byte) string {
	if mix&1 == 0 {
		return name
	}

	src := strings.Split(name, "/")
	out := make([]string, 0, 4)
	idx := 0

	for _, level := range strings.Split(filter, "/") {
		switch level {
		case "+":
			if idx < len(src) {
				out = append(out, src[idx])
			} else {
				out = append(out, "")
			}

			idx++
		case "#":
			if mix&2 == 0 && idx < len(src) {
				out = append(out, src[idx:]...)
			}

			return strings.Join(out, "/")
		default:
			out = append(out, level)
			idx++
		}
	}

	return strings.Join(out, "/")
}
