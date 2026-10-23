// Package cluster runs Tether broker nodes as one cell over NATS and
// JetStream. This file holds the configuration and the names every node of a
// cell agrees on.
package cluster

import (
	"errors"
	"fmt"
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
}

// DefaultConfig returns a disabled embedded configuration with the default
// timings.
func DefaultConfig() Config {
	return Config{
		Mode:      ModeEmbedded,
		Heartbeat: DefaultHeartbeat,
		Lease:     DefaultLease,
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

	return nil
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

	return nil
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
