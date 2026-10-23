// Package subject turns MQTT topics, topic filters and client IDs into NATS
// subjects and JetStream KV keys, and back. It is the one place that knows how
// Tether names things on the wire between broker nodes.
//
// # Scheme
//
// An MQTT topic is split on "/" into levels. Each level becomes one NATS token
// and "." joins the tokens, so MQTT depth equals token depth and "+" maps to "*"
// one for one.
//
//   - A level that is non-empty, uses only [-_=a-zA-Z0-9] and does not start with
//     "=" passes through unchanged.
//   - Every other level (empty, UTF-8, "$", ".", "*", ">", whitespace, NUL, a
//     leading "=") becomes "=" plus the unpadded base64url of its bytes. The empty
//     level is the single token "=".
//
// A passthrough token never starts with "=" and an encoded one always does, so
// the first byte says which kind a token is. The decoder is strict, so every
// valid token has one decoding and every level one token. Tokens use only
// [=A-Za-z0-9_-], which makes them valid in subjects and in KV keys.
//
// # Names
//
//	QoS 0 subject     f.<tenant>.<levels>     "$" topics: f.==<tenant>.<levels>
//	QoS 1/2 log       d.<tenant>.<levels>     "$" topics stay under <tenant>
//	session KV key    <tenant>.<client id>
//	retained KV key   <tenant>.<levels>       "$" topics: ==<tenant>.<levels>
//
// # The "$" rule
//
// MQTT says a filter that starts with "+" or "#" never matches a topic whose
// first level starts with "$". NATS "*" and ">" match any token, so the rule has
// to live in a token before the wildcard. For the QoS 0 subjects and the
// retained keys that token is the tenant: a "$" topic gets "==" in front of the
// tenant token. No ordinary token starts with "==", so the two spaces never
// collide. A wildcard-first filter is built in the ordinary space and cannot
// reach the "$" space; a filter that starts with a literal "$" level is built in
// the "$" space.
//
// The QoS 1/2 log is read by one consumer per tenant (d.<tenant>.>) that wants
// every message, "$" topics included, and matches MQTT filters itself after
// delivery. So its "$" topics stay under the plain tenant token.
//
// # Limits
//
// A topic is capped at 1024 bytes and a client ID at 256 before encoding. The
// worst-case topic of N bytes encodes to 2N+1 bytes, so 1024 bytes stay well
// inside nats-server's max_control_line of 4096.
package subject

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/xraph/tether"
)

const (
	// MaxTopicBytes is the longest topic or topic filter the encoder accepts.
	MaxTopicBytes = 1024

	// MaxClientIDBytes is the longest client ID the encoder accepts.
	MaxClientIDBytes = 256

	marker       = '='
	dollarMarker = "=="
	levelSep     = "/"
	tokenSep     = "."

	qos0Prefix = "f."
	logPrefix  = "d."
)

var b64 = base64.RawURLEncoding.Strict()

func invalid(format string, args ...any) error {
	return fmt.Errorf("subject: %s: %w", fmt.Sprintf(format, args...), tether.ErrInvalidInput)
}

// CheckTopic reports whether topic (or a topic filter) is within the size limit.
func CheckTopic(topic string) error {
	if len(topic) > MaxTopicBytes {
		return invalid("topic is %d bytes, over the %d byte limit", len(topic), MaxTopicBytes)
	}

	return nil
}

// CheckClientID reports whether clientID is within the size limit.
func CheckClientID(clientID string) error {
	if len(clientID) > MaxClientIDBytes {
		return invalid("client id is %d bytes, over the %d byte limit", len(clientID), MaxClientIDBytes)
	}

	return nil
}

func checkTenant(tenant string) error {
	if tenant == "" {
		return invalid("empty tenant")
	}

	return nil
}

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

	for i := range len(level) {
		if !passthroughByte(level[i]) {
			return false
		}
	}

	return true
}

// EncodeLevel turns one MQTT level, or a client ID, into one NATS token. It
// accepts any string and does not apply the size limits.
func EncodeLevel(level string) string {
	if passesThrough(level) {
		return level
	}

	return string(marker) + b64.EncodeToString([]byte(level))
}

// DecodeLevel is the exact inverse of EncodeLevel. It rejects every token that
// EncodeLevel would not produce, so decoding is injective. Errors wrap
// tether.ErrInvalidInput.
func DecodeLevel(token string) (string, error) {
	if token == "" {
		return "", invalid("empty token")
	}

	if token[0] != marker {
		for i := range len(token) {
			if !passthroughByte(token[i]) {
				return "", invalid("token holds byte %q", token[i])
			}
		}

		return token, nil
	}

	// encoding/base64 silently skips "\r" and "\n", so check the alphabet first.
	for i := 1; i < len(token); i++ {
		if c := token[i]; !passthroughByte(c) || c == marker {
			return "", invalid("token holds byte %q after the marker", c)
		}
	}

	raw, err := b64.DecodeString(token[1:])
	if err != nil {
		return "", invalid("token is not canonical base64url (%v)", err)
	}

	level := string(raw)
	if passesThrough(level) {
		return "", invalid("token encodes %q, which passes through", level)
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

// topicName builds prefix + tenant token + levels. It returns "" when the tenant
// is empty or the topic is over the limit.
func topicName(prefix, tenant, topic string, dollarSpace bool) string {
	if checkTenant(tenant) != nil || CheckTopic(topic) != nil {
		return ""
	}

	return prefix + tenantToken(tenant, dollarSpace) + tokenSep + encodeLevels(topic)
}

// filterNames is the filter-side twin of topicName. A trailing "#" gives two
// names, "a" and "a.>", because MQTT "a/#" also matches "a" and NATS ">" needs
// one more token. A lone "#" gives only "T.>": it has no parent topic.
//
// The filter must be valid MQTT ("+" and "#" only as whole levels, "#" last) and
// have any "$share/<group>/" prefix stripped. A wildcard anywhere else is
// encoded as literal text. It returns nil when the tenant is empty or the filter
// is over the limit.
func filterNames(prefix, tenant, filter string) []string {
	if checkTenant(tenant) != nil || CheckTopic(filter) != nil {
		return nil
	}

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

	head := prefix + tenantToken(tenant, dollar(filter))
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

// QoS0 returns the core NATS subject a QoS 0 message on topic is published to:
// "f.<tenant>.<levels>", or "f.==<tenant>.<levels>" when the topic's first level
// starts with "$". It returns "" for an empty tenant or a topic over
// MaxTopicBytes; call CheckTopic first to tell why.
func QoS0(tenant, topic string) string {
	return topicName(qos0Prefix, tenant, topic, dollar(topic))
}

// QoS0Filters returns the subjects to subscribe to for an MQTT filter on the
// QoS 0 path. A trailing "#" gives two subjects, the parent and "parent.>". A
// filter that starts with "+" or "#" cannot reach "$" topics; one that starts
// with a literal "$" level lives in the "==<tenant>" space. It returns nil for
// an empty tenant or a filter over MaxTopicBytes.
func QoS0Filters(tenant, filter string) []string {
	return filterNames(qos0Prefix, tenant, filter)
}

// Log returns the JetStream subject a QoS 1/2 message on topic is stored under:
// "d.<tenant>.<levels>". Unlike QoS0, "$" topics stay under the plain tenant
// token so one filter, [LogTenantAll], reads everything a tenant published. It
// returns "" for an empty tenant or a topic over MaxTopicBytes.
func Log(tenant, topic string) string {
	return topicName(logPrefix, tenant, topic, false)
}

// LogTenantAll returns the one subject filter that carries every logged message
// of a tenant: "d.<tenant>.>". It returns "" for an empty tenant.
func LogTenantAll(tenant string) string {
	if checkTenant(tenant) != nil {
		return ""
	}

	return logPrefix + EncodeLevel(tenant) + tokenSep + ">"
}

// SessionKey returns the key of a client's record in the sessions bucket:
// "<tenant>.<client id as one token>". It returns "" for an empty tenant or a
// client ID over MaxClientIDBytes.
func SessionKey(tenant, clientID string) string {
	if checkTenant(tenant) != nil || CheckClientID(clientID) != nil {
		return ""
	}

	return EncodeLevel(tenant) + tokenSep + EncodeLevel(clientID)
}

// RetainedKey returns the key of a retained message in the retained bucket:
// "<tenant>.<levels>", or "==<tenant>.<levels>" for a "$" topic. It returns ""
// for an empty tenant or a topic over MaxTopicBytes.
func RetainedKey(tenant, topic string) string {
	return topicName("", tenant, topic, dollar(topic))
}

// RetainedKeyFilter returns the KV key filters (watch and list patterns) for an
// MQTT filter, built like [QoS0Filters]: two for a trailing "#", and the same
// "$" rule. It returns nil for an empty tenant or a filter over MaxTopicBytes.
func RetainedKeyFilter(tenant, filter string) []string {
	return filterNames("", tenant, filter)
}

// decode is the inverse of topicName for a name without its prefix. With
// dollarAware, a tenant token starting with "==" marks a "$" topic and the two
// must agree; without it, "==" is not a valid tenant token at all.
func decode(name string, dollarAware bool) (tenant, topic string, err error) {
	tenantTok, rest, ok := strings.Cut(name, tokenSep)
	if !ok {
		return "", "", invalid("no topic tokens in %q", name)
	}

	dollarSpace := false

	if dollarAware && strings.HasPrefix(tenantTok, dollarMarker) {
		dollarSpace = true
		tenantTok = tenantTok[len(dollarMarker):]
	}

	tenant, err = DecodeLevel(tenantTok)
	if err != nil {
		return "", "", err
	}

	if cerr := checkTenant(tenant); cerr != nil {
		return "", "", cerr
	}

	toks := strings.Split(rest, tokenSep)
	levels := make([]string, len(toks))

	for i, tok := range toks {
		if levels[i], err = DecodeLevel(tok); err != nil {
			return "", "", err
		}

		// "/" is the level separator, so no level of a topic holds one. Joining
		// such a level back would split it in two and the name would not
		// reproduce.
		if strings.Contains(levels[i], levelSep) {
			return "", "", invalid("level holds %q", levelSep)
		}
	}

	topic = strings.Join(levels, levelSep)

	if cerr := CheckTopic(topic); cerr != nil {
		return "", "", cerr
	}

	if dollarAware && dollar(topic) != dollarSpace {
		return "", "", invalid("tenant token is in the wrong space for topic %q", topic)
	}

	return tenant, topic, nil
}

// DecodeLog is the exact inverse of [Log]. It rejects anything Log could not
// have produced. Errors wrap tether.ErrInvalidInput.
func DecodeLog(subj string) (tenant, topic string, err error) {
	name, ok := strings.CutPrefix(subj, logPrefix)
	if !ok {
		return "", "", invalid("log subject %q does not start with %q", subj, logPrefix)
	}

	return decode(name, false)
}

// DecodeQoS0 is the exact inverse of [QoS0]: it takes the "f." prefix off and
// reads the tenant and topic back, "==<tenant>" space included. It rejects
// anything QoS0 could not have produced. Errors wrap tether.ErrInvalidInput.
func DecodeQoS0(subj string) (tenant, topic string, err error) {
	name, ok := strings.CutPrefix(subj, qos0Prefix)
	if !ok {
		return "", "", invalid("qos 0 subject %q does not start with %q", subj, qos0Prefix)
	}

	return decode(name, true)
}

// DecodeRetainedKey is the exact inverse of [RetainedKey]. It rejects anything
// RetainedKey could not have produced. Errors wrap tether.ErrInvalidInput.
func DecodeRetainedKey(key string) (tenant, topic string, err error) {
	return decode(key, true)
}
