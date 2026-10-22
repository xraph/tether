package broker

import (
	"fmt"
	"math"
	"time"

	"github.com/xraph/tether"
)

// Defaults for [DefaultConfig]. New also uses them for zero values.
const (
	DefaultTCPAddress       = ":1883"
	DefaultMaxSessionExpiry = 7 * 24 * time.Hour
	DefaultMaxPacketSize    = 1 << 20
	DefaultMaxTopicLength   = 1024
	DefaultMaxInflight      = 1024
)

// TLSConfig is an MQTT over TLS listener.
type TLSConfig struct {
	// Address is the host:port to listen on.
	Address string `yaml:"address" json:"address"`

	// CertFile and KeyFile are PEM files holding the server certificate
	// (chain) and its private key.
	CertFile string `yaml:"cert_file" json:"cert_file"`
	KeyFile  string `yaml:"key_file" json:"key_file"`
}

// Config configures the broker's listeners and protocol limits.
type Config struct {
	// TCPAddress is the plain MQTT listener. Empty turns it off.
	TCPAddress string `yaml:"tcp_address" json:"tcp_address"`

	// TLS is the MQTT over TLS listener. Nil turns it off.
	TLS *TLSConfig `yaml:"tls" json:"tls"`

	// WebSocketAddress is the MQTT over WebSocket listener (subprotocol
	// "mqtt", any path). Empty turns it off.
	WebSocketAddress string `yaml:"websocket_address" json:"websocket_address"`

	// DataDir is where sessions, subscriptions, inflight and retained
	// messages persist (a Badger database). Empty keeps them in memory only.
	DataDir string `yaml:"data_dir" json:"data_dir"`

	// MaxSessionExpiry caps the session expiry a client may ask for. Zero
	// means DefaultMaxSessionExpiry.
	MaxSessionExpiry time.Duration `yaml:"max_session_expiry" json:"max_session_expiry"`

	// MaxPacketSize is the largest packet the broker accepts, in bytes. Zero
	// means DefaultMaxPacketSize.
	MaxPacketSize uint32 `yaml:"max_packet_size" json:"max_packet_size"`

	// MaxTopicLength is the longest topic name or filter a client may send,
	// in bytes, before the tenant prefix is added. Zero means
	// DefaultMaxTopicLength.
	MaxTopicLength int `yaml:"max_topic_length" json:"max_topic_length"`

	// MaxInflight is how many QoS 1 and 2 messages the broker keeps in
	// flight for one client, outbound queue of an offline session included.
	// Past it, further messages for that client are dropped. Zero means
	// DefaultMaxInflight (1024; mochi's own default is 8192).
	MaxInflight uint16 `yaml:"max_inflight" json:"max_inflight"`
}

// DefaultConfig returns a config with a TCP listener on :1883, a 7 day
// session expiry cap, a 1 MiB packet limit, a 1 KiB topic limit, 1024
// in-flight messages per client and no persistence.
func DefaultConfig() Config {
	return Config{
		TCPAddress:       DefaultTCPAddress,
		MaxSessionExpiry: DefaultMaxSessionExpiry,
		MaxPacketSize:    DefaultMaxPacketSize,
		MaxTopicLength:   DefaultMaxTopicLength,
		MaxInflight:      DefaultMaxInflight,
	}
}

// withDefaults fills zero limits with the defaults and checks the rest.
func (c Config) withDefaults() (Config, error) {
	if c.MaxSessionExpiry == 0 {
		c.MaxSessionExpiry = DefaultMaxSessionExpiry
	}

	if c.MaxPacketSize == 0 {
		c.MaxPacketSize = DefaultMaxPacketSize
	}

	if c.MaxTopicLength == 0 {
		c.MaxTopicLength = DefaultMaxTopicLength
	}

	if c.MaxInflight == 0 {
		c.MaxInflight = DefaultMaxInflight
	}

	switch {
	case c.MaxSessionExpiry < 0:
		return c, invalid("max_session_expiry must not be negative")
	case c.MaxTopicLength < 0:
		return c, invalid("max_topic_length must not be negative")
	case c.TCPAddress == "" && c.TLS == nil && c.WebSocketAddress == "":
		return c, invalid("no listener configured")
	case c.TLS != nil && (c.TLS.Address == "" || c.TLS.CertFile == "" || c.TLS.KeyFile == ""):
		return c, invalid("tls needs address, cert_file and key_file")
	}

	return c, nil
}

// sessionExpirySeconds is MaxSessionExpiry in whole seconds, clamped to what
// MQTT can carry.
func (c Config) sessionExpirySeconds() uint32 {
	return durationSeconds(c.MaxSessionExpiry)
}

// durationSeconds is d in whole seconds, clamped to what MQTT can carry.
func durationSeconds(d time.Duration) uint32 {
	s := d / time.Second

	switch {
	case s < 0:
		return 0
	case s > math.MaxUint32:
		return math.MaxUint32
	}

	return uint32(s)
}

func invalid(msg string) error {
	return fmt.Errorf("tether/broker: %w: %s", tether.ErrInvalidInput, msg)
}
