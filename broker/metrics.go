package broker

import "github.com/mochi-mqtt/server/v2/packets"

// Metrics receives the broker's node-wide counters. None of them carries a
// tenant: per-tenant numbers are in Stats and the tenant's $SYS topics, so
// the metric series stay bounded however many tenants there are.
//
// The broker calls these from mochi's hooks, on the connection's goroutine,
// so an implementation must be safe for concurrent use and quick.
type Metrics interface {
	// ConnectionOpened and ConnectionClosed bracket each connection that
	// holds a tenant connection slot. A connection that takes over a
	// client ID at the tenant's limit inherits the old connection's slot
	// and reports neither, so opened minus closed is the number of live
	// connections.
	ConnectionOpened()
	ConnectionClosed()

	// MessageIn counts a client PUBLISH that passed the ACL and the rate
	// limit; MessageOut counts a PUBLISH written to a network client.
	MessageIn()
	MessageOut()

	// AuthFailure counts a refused CONNECT, with one of the AuthReason
	// constants.
	AuthFailure(reason string)

	// ACLDenied counts an ACL check that failed, with ACLOpPublish or
	// ACLOpSubscribe (a SUBSCRIBE or a delivery).
	ACLDenied(op string)
}

// Reasons AuthFailure reports, one per CONNACK reason the broker refuses
// with.
const (
	AuthReasonBadCredentials  = "bad_credentials"
	AuthReasonNotAuthorized   = "not_authorized"
	AuthReasonUnavailable     = "unavailable"
	AuthReasonRateLimited     = "rate_limited"
	AuthReasonConnectionLimit = "connection_limit"
	AuthReasonInvalidWill     = "invalid_will"
	AuthReasonOther           = "other"
)

// Operations ACLDenied reports.
const (
	ACLOpPublish   = "publish"
	ACLOpSubscribe = "subscribe"
)

// noMetrics is the Metrics a broker without one reports to.
type noMetrics struct{}

func (noMetrics) ConnectionOpened()  {}
func (noMetrics) ConnectionClosed()  {}
func (noMetrics) MessageIn()         {}
func (noMetrics) MessageOut()        {}
func (noMetrics) AuthFailure(string) {}
func (noMetrics) ACLDenied(string)   {}

// authReason is the AuthFailure reason for a refusal with code.
func authReason(code packets.Code) string {
	switch code {
	case packets.ErrBadUsernameOrPassword:
		return AuthReasonBadCredentials
	case packets.ErrNotAuthorized:
		return AuthReasonNotAuthorized
	case packets.ErrServerUnavailable:
		return AuthReasonUnavailable
	case packets.ErrConnectionRateExceeded:
		return AuthReasonRateLimited
	case packets.ErrQuotaExceeded:
		return AuthReasonConnectionLimit
	case packets.ErrTopicNameInvalid:
		return AuthReasonInvalidWill
	default:
		return AuthReasonOther
	}
}

// aclOp is the ACLDenied operation for a check.
func aclOp(write bool) string {
	if write {
		return ACLOpPublish
	}

	return ACLOpSubscribe
}
