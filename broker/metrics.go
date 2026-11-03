package broker

import (
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
)

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

// ClusterMetrics receives a cell node's counters. A Deps.Metrics that also
// implements it gets them when the broker runs with a cluster; the broker
// finds it with a type assertion, so a Metrics written before clustering
// keeps working. None carries a tenant. Like Metrics, the methods run on hot
// paths and other goroutines, so they must be quick and safe for concurrent
// use.
type ClusterMetrics interface {
	// ClusterFenced reports that the node was fenced (true: it drops its
	// clients and refuses new ones) or unfenced (false).
	ClusterFenced(fenced bool)

	// LogAppended reports one QoS 1 or 2 append to the cell's log that
	// committed: client publishes, host publishes and wills alike, with
	// the time from the send to the commit (cluster.Log.OnAppend).
	LogAppended(d time.Duration)

	// CASLost counts a session record write that lost its compare-and-set
	// to another writer (cluster.Sessions.OnLostCAS): contested CONNECTs,
	// takeovers and the dead-node reclaims racing each other.
	CASLost()

	// ReplayMessage counts a message a replay sent to a session that came
	// back (or moved here) behind the tenant's reader.
	ReplayMessage()
}

// Reasons AuthFailure reports, one per CONNACK reason the broker refuses
// with, and AuthReasonNotEntitled for a tenant Deps.Entitlement refused
// (0x97, like AuthReasonConnectionLimit).
const (
	AuthReasonBadCredentials  = "bad_credentials"
	AuthReasonNotAuthorized   = "not_authorized"
	AuthReasonUnavailable     = "unavailable"
	AuthReasonRateLimited     = "rate_limited"
	AuthReasonConnectionLimit = "connection_limit"
	AuthReasonNotEntitled     = "not_entitled"
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

// noClusterMetrics is the ClusterMetrics of a broker whose Metrics is not one.
type noClusterMetrics struct{}

func (noClusterMetrics) ClusterFenced(bool)        {}
func (noClusterMetrics) LogAppended(time.Duration) {}
func (noClusterMetrics) CASLost()                  {}
func (noClusterMetrics) ReplayMessage()            {}

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
