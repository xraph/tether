// Package cluster runs Tether broker nodes as one cell over NATS and
// JetStream. This file holds the configuration and the names every node of a
// cell agrees on.
package cluster

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

// Modes for Config.Mode.
const (
	// ModeEmbedded starts a nats-server with JetStream inside the broker
	// process.
	ModeEmbedded = "embedded"
	// ModeExternal connects to a NATS deployment you run yourself.
	ModeExternal = "external"
)

// Defaults for DefaultConfig.
const (
	DefaultHeartbeat = 2 * time.Second
	DefaultLease     = 10 * time.Second
	// DefaultLogMaxAge is how long the QoS 1 and 2 log keeps a message: the
	// broker's default session expiry cap, 7 days.
	DefaultLogMaxAge = 7 * 24 * time.Hour
)

// maxReplicas is the ceiling for stream and bucket replicas the cell asks for.
const maxReplicas = 3

// Config is the cluster section of the broker configuration.
type Config struct {
	// Enabled turns clustering on. With it off nothing in this package runs and
	// the other fields are not checked.
	Enabled bool `yaml:"enabled" json:"enabled"`
	// NodeID names this node inside the cell. It becomes a KV key and a subject
	// token, so it is 1 to 64 bytes of [-_a-zA-Z0-9].
	NodeID string `yaml:"node_id" json:"node_id"`
	// Cell names the group of nodes that share sessions. Lowercase letters and
	// digits, 1 to 16 bytes. It is part of every stream and bucket name.
	Cell string `yaml:"cell" json:"cell"`
	// Mode is "embedded" (the default) or "external".
	Mode string `yaml:"mode" json:"mode"`
	// URLs are the NATS servers to connect to in external mode.
	URLs []string `yaml:"urls" json:"urls"`
	// Embedded configures the in-process server in embedded mode.
	Embedded EmbeddedConfig `yaml:"embedded" json:"embedded"`
	// Heartbeat is how often a node renews its lease and announces itself.
	Heartbeat time.Duration `yaml:"heartbeat" json:"heartbeat"`
	// Lease is how long a node's claim on its sessions lasts without a renew.
	// It must exceed twice the heartbeat.
	Lease time.Duration `yaml:"lease" json:"lease"`
	// Credentials is the path of a NATS credentials file (external mode).
	Credentials string `yaml:"credentials" json:"credentials"`
	// Replicas is the replica count for the log stream, the control stream and
	// the buckets. 0 means automatic: see Conn.Replicas. 1 to 3 pins it.
	Replicas int `yaml:"replicas" json:"replicas"`
	// NoDiscovery keeps the connection on the servers it was given: it ignores
	// the server URLs a NATS cluster advertises and tries the given ones in
	// order. Leave it off in production. A node that reaches NATS through a
	// proxy (the fault proxy in tests) sets it, or nats.go reconnects around
	// the proxy. With it on, the automatic replica count sees only the given
	// URLs, so set Replicas.
	NoDiscovery bool `yaml:"no_discovery" json:"no_discovery"`
	// LogMaxAge is how long the QoS 1 and 2 log keeps a message, the MaxAge
	// the cell's log stream is provisioned with. An offline session that
	// stays away longer loses what it missed past that age, whatever its
	// session expiry says. 0 means DefaultLogMaxAge.
	LogMaxAge time.Duration `yaml:"log_max_age" json:"log_max_age"`
}

// EmbeddedConfig configures the nats-server that embedded mode runs.
type EmbeddedConfig struct {
	// ServerName names the server inside a NATS cluster. It defaults to the
	// node ID.
	ServerName string `yaml:"server_name" json:"server_name"`
	// ClusterName names the NATS cluster. Leave it empty for a single server.
	ClusterName string `yaml:"cluster_name" json:"cluster_name"`
	// Listen is the client listen address, "host:port". The default is
	// "127.0.0.1:-1" (loopback, random port): the broker connects in-process,
	// so the port only matters for tools you point at the server.
	Listen string `yaml:"listen" json:"listen"`
	// ClusterListen is the route listen address, "host:port". Required when
	// ClusterName or Routes is set.
	ClusterListen string `yaml:"cluster_listen" json:"cluster_listen"`
	// Routes are the route URLs ("nats://host:port") of the other servers. List
	// every other server: JetStream sizes its meta group from the route count.
	Routes []string `yaml:"routes" json:"routes"`
	// StoreDir is the JetStream storage directory.
	StoreDir string `yaml:"store_dir" json:"store_dir"`
	// User and Password, when set, are required of every client and every
	// route of the embedded server: the broker's own connection, any tool
	// you point at Listen, and the other servers of the cell, which must be
	// given the same pair. They go into each route URL that carries none.
	User     string `yaml:"user" json:"user"`
	Password string `yaml:"password" json:"password"`
	// Insecure lets Listen or ClusterListen bind an address other than
	// loopback with no User and Password. Anyone who reaches such a port can
	// then read and write every tenant's traffic, or join the cell as a
	// route. Without it, Validate refuses that config.
	Insecure bool `yaml:"insecure" json:"insecure"`
}

// DefaultConfig returns a disabled embedded configuration with the default
// timings.
func DefaultConfig() Config {
	return Config{
		Mode:      ModeEmbedded,
		Heartbeat: DefaultHeartbeat,
		Lease:     DefaultLease,
		LogMaxAge: DefaultLogMaxAge,
	}
}

// Validate checks the configuration. A disabled configuration is always valid.
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	if err := checkNodeID(c.NodeID); err != nil {
		return err
	}

	if err := checkCell(c.Cell); err != nil {
		return err
	}

	switch c.Mode {
	case ModeEmbedded:
		if err := c.Embedded.validate(); err != nil {
			return err
		}
	case ModeExternal:
		if len(c.URLs) == 0 {
			return errors.New("cluster: external mode needs at least one url")
		}

		for _, u := range c.URLs {
			if strings.TrimSpace(u) == "" {
				return errors.New("cluster: empty url")
			}
		}
	default:
		return fmt.Errorf("cluster: mode %q must be %q or %q", c.Mode, ModeEmbedded, ModeExternal)
	}

	if c.Heartbeat <= 0 {
		return errors.New("cluster: heartbeat must be positive")
	}

	if c.Lease <= 2*c.Heartbeat {
		return fmt.Errorf("cluster: lease %s must exceed twice the heartbeat (%s)", c.Lease, 2*c.Heartbeat)
	}

	if c.Replicas < 0 || c.Replicas > maxReplicas {
		return fmt.Errorf("cluster: replicas %d must be 0 (auto) to %d", c.Replicas, maxReplicas)
	}

	if c.LogMaxAge < 0 {
		return fmt.Errorf("cluster: log_max_age %s must not be negative", c.LogMaxAge)
	}

	return nil
}

// LogAge is LogMaxAge, or DefaultLogMaxAge when that is 0.
func (c Config) LogAge() time.Duration {
	if c.LogMaxAge == 0 {
		return DefaultLogMaxAge
	}

	return c.LogMaxAge
}

func (e EmbeddedConfig) validate() error {
	if e.StoreDir == "" {
		return errors.New("cluster: embedded mode needs embedded.store_dir")
	}

	if (e.ClusterName != "" || len(e.Routes) > 0) && e.ClusterListen == "" {
		return errors.New("cluster: embedded.cluster_listen is required with cluster_name or routes")
	}

	if len(e.Routes) > 0 && e.ClusterName == "" {
		return errors.New("cluster: embedded.cluster_name is required with routes")
	}

	if (e.User == "") != (e.Password == "") {
		return errors.New("cluster: embedded.user and embedded.password go together")
	}

	return e.checkExposure()
}

// checkExposure refuses a listen address other than loopback when the server
// would take anyone: no User and Password, and no Insecure.
func (e EmbeddedConfig) checkExposure() error {
	if e.User != "" || e.Insecure {
		return nil
	}

	addrs := map[string]string{"embedded.listen": e.Listen}
	if e.ClusterName != "" {
		addrs["embedded.cluster_listen"] = e.ClusterListen
	}

	for _, key := range []string{"embedded.listen", "embedded.cluster_listen"} {
		addr, ok := addrs[key]
		if !ok || addr == "" {
			continue
		}

		if !loopback(addr) {
			return fmt.Errorf("cluster: %s %q is not a loopback address: set embedded.user and embedded.password "+
				"(or embedded.insecure: true to accept anyone who reaches it)", key, addr)
		}
	}

	return nil
}

// loopback reports whether the host of "host:port" is a loopback address. An
// empty host binds every interface, so it is not; neither is an address that
// does not parse.
func loopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}

	if strings.EqualFold(host, "localhost") {
		return true
	}

	ip := net.ParseIP(host)

	return ip != nil && ip.IsLoopback()
}

func checkNodeID(id string) error {
	if id == "" {
		return errors.New("cluster: node_id is required")
	}

	if len(id) > 64 {
		return fmt.Errorf("cluster: node_id is %d bytes, the limit is 64", len(id))
	}

	for i := range len(id) {
		if !tokenByte(id[i]) {
			return fmt.Errorf("cluster: node_id %q may only hold letters, digits, '-' and '_'", id)
		}
	}

	return nil
}

func checkCell(cell string) error {
	if cell == "" {
		return errors.New("cluster: cell is required")
	}

	if len(cell) > 16 {
		return fmt.Errorf("cluster: cell is %d bytes, the limit is 16", len(cell))
	}

	for i := range len(cell) {
		c := cell[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return fmt.Errorf("cluster: cell %q may only hold lowercase letters and digits", cell)
		}
	}

	return nil
}

func tokenByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_'
}

// LogStream is the QoS 1 and 2 message log stream.
func (c Config) LogStream() string { return "TETHER_LOG_" + strings.ToUpper(c.Cell) }

// ControlStream is the stream for control messages between nodes.
func (c Config) ControlStream() string { return "TETHER_CTL_" + strings.ToUpper(c.Cell) }

// SessionsBucket is the KV bucket of session ownership and state.
func (c Config) SessionsBucket() string { return "tether-sessions-" + strings.ToLower(c.Cell) }

// RetainedBucket is the KV bucket of retained messages.
func (c Config) RetainedBucket() string { return "tether-retained-" + strings.ToLower(c.Cell) }

// MembersBucket is the KV bucket of live nodes, keyed by node ID.
func (c Config) MembersBucket() string { return "tether-members-" + strings.ToLower(c.Cell) }
