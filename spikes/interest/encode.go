// This file is a copy of spikes/encoding/encode.go (Phase 0, Task 2), kept
// verbatim apart from the package name so subjects match spike 2 exactly.
//
// The original package comment: the Phase 0 spike for mapping MQTT topics and client IDs
// onto NATS subjects and JetStream KV keys.
//
// # Scheme
//
// An MQTT topic is split on "/" into levels. Each level becomes one NATS token
// and "." joins the tokens, so MQTT level depth equals NATS token depth.
//
//   - A level that is non-empty, holds only [-_=a-zA-Z0-9] and does not start
//     with "=" passes through unchanged.
//   - Every other level (empty, UTF-8, "$", ".", "*", ">", whitespace, NUL, a
//     leading "=") becomes "=" followed by the unpadded base64url of its bytes.
//     The empty level is the single token "=".
//
// The marker is "=". A passthrough token never starts with "=", an encoded token
// always does, so the first byte alone says which kind a token is. The decoder is
// strict (canonical base64, and an encoded token must hold a level that would not
// have passed through), so every valid token has exactly one decoding and every
// level exactly one token. Encoded tokens use only [=A-Za-z0-9_-], so they are
// valid in subjects and in KV keys (^[-/_=.a-zA-Z0-9]+$).
//
// # The "$" rule
//
// MQTT says a filter whose first level is "+" or "#" never matches a topic whose
// first level starts with "$". NATS "*" and ">" match any token, so the token
// content cannot carry that rule. It has to live in a token before the wildcard.
// The tenant token is that token: a topic whose first level starts with "$" gets
// the tenant token "==" + EncodeLevel(tenant) instead of EncodeLevel(tenant). No
// ordinary token starts with "==" (an encoded one has a base64url character
// after the single "="), so the two spaces never collide. A wildcard-first
// filter is built in the ordinary space and cannot reach the "$" space. A filter
// whose first level is a literal "$..." level is built in the "$" space.
//
// The price: "all of a tenant's topics" is two subjects, see [TenantAll].
package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

const (
	marker       = '='
	dollarMarker = "=="
	levelSep     = "/"
	tokenSep     = "."
)

var b64 = base64.RawURLEncoding.Strict()

// passthroughByte reports whether c may appear in a level that passes through.
func passthroughByte(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	case c == '-', c == '_', c == '=':
		return true
	}

	return false
}

// passesThrough reports whether level is written as is: non-empty, only
// passthrough bytes, and not starting with the marker.
func passesThrough(level string) bool {
	if level == "" || level[0] == marker {
		return false
	}

	for i := 0; i < len(level); i++ {
		if !passthroughByte(level[i]) {
			return false
		}
	}

	return true
}

// EncodeLevel turns one MQTT level, or a client ID, into one NATS token.
func EncodeLevel(level string) string {
	if passesThrough(level) {
		return level
	}

	return string(marker) + b64.EncodeToString([]byte(level))
}

// ErrInvalidToken is returned for a token that EncodeLevel could not have produced.
var ErrInvalidToken = errors.New("encoding: invalid token")

// DecodeLevel is the exact inverse of EncodeLevel. It rejects every token that
// EncodeLevel would not produce, so decoding is injective.
func DecodeLevel(token string) (string, error) {
	if token == "" {
		return "", fmt.Errorf("%w: empty", ErrInvalidToken)
	}

	if token[0] != marker {
		for i := 0; i < len(token); i++ {
			if !passthroughByte(token[i]) {
				return "", fmt.Errorf("%w: byte %q", ErrInvalidToken, token[i])
			}
		}

		return token, nil
	}

	// encoding/base64 silently skips "\r" and "\n", so check the alphabet first.
	for i := 1; i < len(token); i++ {
		if c := token[i]; !passthroughByte(c) || c == marker {
			return "", fmt.Errorf("%w: byte %q after the marker", ErrInvalidToken, c)
		}
	}

	raw, err := b64.DecodeString(token[1:])
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}

	level := string(raw)
	if passesThrough(level) {
		return "", fmt.Errorf("%w: %q should pass through", ErrInvalidToken, level)
	}

	return level, nil
}

func encodeLevels(topic string) string {
	var b strings.Builder

	b.Grow(len(topic) + len(topic)/2 + 8)

	rest := topic
	for first := true; ; first = false {
		level, tail, more := strings.Cut(rest, levelSep)

		if !first {
			b.WriteString(tokenSep)
		}

		b.WriteString(EncodeLevel(level))

		if !more {
			break
		}

		rest = tail
	}

	return b.String()
}

func dollar(topic string) bool {
	return strings.HasPrefix(topic, "$")
}

func tenantToken(tenant string, dollarSpace bool) string {
	if dollarSpace {
		return dollarMarker + EncodeLevel(tenant)
	}

	return EncodeLevel(tenant)
}

// EncodeTopic returns "<tenant token>.<level tokens>" for a plain topic name.
// Callers add the "d." or "f." prefix. The topic is not validated: any string
// round trips, an empty one as the single empty level.
func EncodeTopic(tenant, topic string) string {
	return tenantToken(tenant, dollar(topic)) + tokenSep + encodeLevels(topic)
}

// EncodeFilter returns the NATS subjects to subscribe to for a plain MQTT filter
// (no "$share/<group>/" prefix, which callers strip). A trailing "#" gives two
// subjects, "a" and "a.>", because MQTT "a/#" also matches "a" and NATS ">" needs
// one more token. A lone "#" gives only "T.>": it has no parent topic.
//
// A filter must be valid MQTT: "+" and "#" only as whole levels and "#" last.
// Wildcards elsewhere are encoded as literal text.
func EncodeFilter(tenant, filter string) []string {
	levels := strings.Split(filter, levelSep)
	toks := make([]string, 0, len(levels))
	hash := false

	for i, level := range levels {
		switch {
		case level == "+":
			toks = append(toks, "*")
		case level == "#" && i == len(levels)-1:
			hash = true
		default:
			toks = append(toks, EncodeLevel(level))
		}
	}

	head := tenantToken(tenant, dollar(filter))
	body := strings.Join(toks, tokenSep)

	switch {
	case !hash:
		return []string{head + tokenSep + body}
	case body == "":
		return []string{head + tokenSep + ">"}
	default:
		return []string{head + tokenSep + body, head + tokenSep + body + tokenSep + ">"}
	}
}

// TenantAll returns the subjects that together carry every topic of a tenant,
// the "$" topics included. MQTT "#" excludes "$" topics, so a consumer that
// wants the tenant's whole log (a node reading d.<tenant>.>) needs both.
func TenantAll(tenant string) []string {
	return []string{
		tenantToken(tenant, false) + tokenSep + ">",
		tenantToken(tenant, true) + tokenSep + ">",
	}
}

// DecodeTopic is the exact inverse of EncodeTopic. It takes the subject without
// its "d." or "f." prefix and rejects anything EncodeTopic could not have produced.
func DecodeTopic(subject string) (tenant, topic string, err error) {
	tenantTok, rest, ok := strings.Cut(subject, tokenSep)
	if !ok {
		return "", "", fmt.Errorf("%w: no topic tokens", ErrInvalidToken)
	}

	dollarSpace := strings.HasPrefix(tenantTok, dollarMarker)
	if dollarSpace {
		tenantTok = tenantTok[len(dollarMarker):]
	}

	tenant, err = DecodeLevel(tenantTok)
	if err != nil {
		return "", "", err
	}

	toks := strings.Split(rest, tokenSep)
	levels := make([]string, len(toks))

	for i, tok := range toks {
		if levels[i], err = DecodeLevel(tok); err != nil {
			return "", "", err
		}

		// "/" is the level separator, so no level of a topic holds one. Joining
		// such a level back would split it into two and EncodeTopic would not
		// reproduce this subject.
		if strings.Contains(levels[i], levelSep) {
			return "", "", fmt.Errorf("%w: level holds %q", ErrInvalidToken, levelSep)
		}
	}

	topic = strings.Join(levels, levelSep)
	if dollar(topic) != dollarSpace {
		return "", "", fmt.Errorf("%w: tenant token in the wrong space", ErrInvalidToken)
	}

	return tenant, topic, nil
}
