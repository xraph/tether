// Command tether runs the Tether MQTT broker as a standalone server: a Forge
// app with grove (SQLite or PostgreSQL), keysmith, warden, the Tether
// extension and the Forge dashboard, plus ledger when metering or
// entitlements are on and chronicle when audit is on.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"

	chronicleext "github.com/xraph/chronicle/extension"
	"github.com/xraph/forge"
	dashboard "github.com/xraph/forge/extensions/dashboard"
	"github.com/xraph/grove"
	keysmithext "github.com/xraph/keysmith/extension"
	ledgerext "github.com/xraph/ledger/extension"
	"github.com/xraph/vessel"
	wardenext "github.com/xraph/warden/extension"

	"github.com/xraph/tether"
	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/extension"
)

// Defaults for the keys this binary adds on top of the extension's config.
const (
	defaultHTTPAddress = ":8080"
	defaultSQLitePath  = "./data/tether.db"
	defaultDataDir     = "./data/sessions"

	// defaultConfigFile is read when --config is not given and the file
	// exists in the working directory.
	defaultConfigFile = "tether.yaml"

	// defaultNATSDir is where an embedded cell node keeps its JetStream
	// data when cluster.embedded.store_dir is empty, under a directory
	// named for the node, so nodes on one host never share one.
	defaultNATSDir = "./data/nats"

	// nodeIDEnv names the environment variable that sets the node's ID in
	// its cell, as --node-id does. The flag wins over it, and it wins over
	// tether.cluster.node_id in the file.
	nodeIDEnv = "TETHER_NODE_ID"

	// maxEmbeddedNodes is the largest cell embedded mode runs: the node and
	// two routes. Past it, run NATS yourself (mode: external), because
	// every broker restart would otherwise be JetStream peer churn.
	maxEmbeddedNodes = 3
)

// config is the binary's config file: the extension's own section under
// "tether", and the keys that only a standalone process needs under "server".
type config struct {
	Server serverConfig     `yaml:"server"`
	Tether extension.Config `yaml:"tether"`
}

// serverConfig is the process-level part of the config.
type serverConfig struct {
	// HTTPAddress is where the admin API, the device publish endpoint and the
	// dashboard listen.
	HTTPAddress string `yaml:"http_address"`

	// SQLitePath is the SQLite file that holds tenants, devices, keys and
	// roles when the database driver is sqlite. Its directory is created
	// when missing.
	SQLitePath string `yaml:"sqlite_path"`

	// Database picks SQLite (the default) or PostgreSQL for the same data.
	Database databaseConfig `yaml:"database"`
}

// defaultConfig is the config of a process started with no config file: HTTP
// on :8080, MQTT on :1883, and everything on disk under ./data.
func defaultConfig() config {
	cfg := extension.DefaultConfig()
	cfg.Broker.DataDir = defaultDataDir

	return config{
		Server: serverConfig{
			HTTPAddress: defaultHTTPAddress,
			SQLitePath:  defaultSQLitePath,
			Database:    databaseConfig{Driver: driverSQLite},
		},
		Tether: cfg,
	}
}

// loadConfig reads the YAML file at path over the defaults, so a key the file
// omits keeps its default. An empty path gives the defaults. A missing file is
// an error when the operator named it (explicit) and not otherwise.
func loadConfig(path string, explicit bool) (config, error) {
	cfg := defaultConfig()

	if path == "" {
		return cfg, nil
	}

	raw, err := os.ReadFile(filepath.Clean(path))
	if errors.Is(err, os.ErrNotExist) && !explicit {
		return cfg, nil
	}

	if err != nil {
		return config{}, fmt.Errorf("tether: read config: %w", err)
	}

	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return config{}, fmt.Errorf("tether: parse config %s: %w", path, err)
	}

	return cfg, nil
}

// applyCluster sets the node ID from --node-id or TETHER_NODE_ID (the flag
// first), defaults an embedded node's store directory to ./data/nats/<node
// id>, and refuses an embedded cell of more than three nodes. A config with
// clustering off is left alone, apart from the node ID.
func applyCluster(cfg *config, flagNodeID, envNodeID string) error {
	c := &cfg.Tether.Cluster

	switch {
	case flagNodeID != "":
		c.NodeID = flagNodeID
	case envNodeID != "":
		c.NodeID = envNodeID
	}

	if !c.Enabled || c.Mode != cluster.ModeEmbedded {
		return nil
	}

	if n := 1 + len(c.Embedded.Routes); n > maxEmbeddedNodes {
		return fmt.Errorf("tether: an embedded cell runs up to %d nodes and this one has %d (itself and %d routes); "+
			"run NATS yourself and set tether.cluster.mode: external", maxEmbeddedNodes, n, n-1)
	}

	if c.Embedded.StoreDir == "" && c.NodeID != "" {
		c.Embedded.StoreDir = filepath.Join(defaultNATSDir, c.NodeID)
	}

	return nil
}

// newApp builds the Forge app: a grove database (SQLite or PostgreSQL) in the container,
// keysmith and warden with their own REST routes off, ledger and chronicle
// when the config needs them (productExtensions), Tether, and the dashboard.
// It does not start it.
//
// The admin API is guarded by the bearer token. With an empty token it needs
// insecure, and then the dashboard intents are open too. The returned func
// closes the database and must run after the app has stopped.
func newApp(cfg config, token string, insecure bool, appOpts ...forge.AppOption) (forge.App, func(), error) {
	if token == "" && !insecure {
		return nil, nil, errNoAdminToken
	}

	if err := cfg.Server.validateDatabase(); err != nil {
		return nil, nil, err
	}

	dirs := []string{cfg.Tether.Broker.DataDir, natsDir(cfg)}

	// The SQLite file's directory only matters on SQLite. A PostgreSQL node
	// keeps nothing of the database on disk.
	if !cfg.Server.usesPostgres() {
		dirs = append([]string{filepath.Dir(cfg.Server.SQLitePath)}, dirs...)
	}

	for _, dir := range dirs {
		if dir == "" {
			continue
		}

		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, nil, fmt.Errorf("tether: create data dir: %w", err)
		}
	}

	db, err := openDB(cfg.Server)
	if err != nil {
		return nil, nil, err
	}

	app := forge.New(append([]forge.AppOption{
		forge.WithAppName("tether"),
		forge.WithAppVersion(tether.Version),
		forge.WithHTTPAddress(cfg.Server.HTTPAddress),
		// Environment variables are for the admin token, the node ID and
		// the database DSN only (run reads them): the config file is the
		// one place everything else is set.
		forge.WithEnableEnvConfig(false),
		forge.WithEnableAppScopedConfig(false),
	}, appOpts...)...)

	if err := vessel.Provide(app.Container(), func() (*grove.DB, error) { return db, nil }); err != nil {
		_ = db.Close()

		return nil, nil, fmt.Errorf("tether: register database: %w", err)
	}

	extOpts := []extension.Option{extension.WithConfig(cfg.Tether)}

	if token != "" {
		extOpts = append(extOpts, extension.WithAdminMiddleware(adminAuth(token)))
	} else {
		extOpts = append(extOpts, extension.WithInsecureAdminRoutes())
	}

	if insecure {
		extOpts = append(extOpts, extension.WithInsecureDashboard())
	}

	ext := extension.New(extOpts...)

	exts := make([]forge.Extension, 0, 6)
	exts = append(exts, ext.Keysmith(keysmithext.WithDisableRoutes()), ext.Warden(wardenext.WithDisableRoutes()))
	exts = append(exts, productExtensions(cfg.Tether)...)
	exts = append(exts, ext, dashboard.NewExtension())

	for _, x := range exts {
		if err := app.RegisterExtension(x); err != nil {
			_ = db.Close()

			return nil, nil, fmt.Errorf("tether: register %s: %w", x.Name(), err)
		}
	}

	return app, func() { _ = db.Close() }, nil
}

// productExtensions returns ledger's extension when metering or entitlements
// are on, and chronicle's when audit is on, both with their REST routes off.
// Each finds the binary's grove database in the container, so it keeps its
// tables next to Tether's, on SQLite or PostgreSQL alike. A part that is off
// gets nothing: a ledger or chronicle in the app never turns a part on.
func productExtensions(cfg extension.Config) []forge.Extension {
	var out []forge.Extension

	if cfg.Metering.Enabled || cfg.Entitlements.Enabled {
		out = append(out, ledgerext.New(ledgerext.WithDisableRoutes()))
	}

	if cfg.Audit.Enabled {
		out = append(out, chronicleext.New(chronicleext.WithDisableRoutes()))
	}

	return out
}

// natsDir is the embedded NATS store directory to create, "" when the node
// runs no embedded server.
func natsDir(cfg config) string {
	c := cfg.Tether.Cluster
	if !c.Enabled || c.Mode != cluster.ModeEmbedded {
		return ""
	}

	return c.Embedded.StoreDir
}

// run is the whole program, with its inputs passed in so a test can drive it:
// the arguments after the program name, the environment, and where output goes:
// stdout for --version, stderr for usage and flag errors. It returns when the
// app has shut down on SIGINT or SIGTERM, or when it fails.
func run(args []string, getenv func(string) string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("tether", flag.ContinueOnError)
	fs.SetOutput(stderr)

	configPath := fs.String("config", "", "path to the YAML config file (default ./"+defaultConfigFile+" if it exists)")
	insecure := fs.Bool("insecure-admin", false, "leave the admin API and the dashboard open when "+adminTokenEnv+" is empty")
	showVersion := fs.Bool("version", false, "print the version and exit")
	nodeID := fs.String("node-id", "", "this node's ID in its cell (tether.cluster.node_id); "+nodeIDEnv+" sets it too")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *showVersion {
		_, err := fmt.Fprintln(stdout, "tether", tether.Version)

		return err
	}

	token := getenv(adminTokenEnv)
	if token == "" && !*insecure {
		return errNoAdminToken
	}

	path, explicit := *configPath, *configPath != ""
	if !explicit {
		path = defaultConfigFile
	}

	cfg, err := loadConfig(path, explicit)
	if err != nil {
		return err
	}

	if cerr := applyCluster(&cfg, *nodeID, getenv(nodeIDEnv)); cerr != nil {
		return cerr
	}

	if derr := applyDatabase(&cfg, getenv(databaseDSNEnv)); derr != nil {
		return derr
	}

	app, closeDB, err := newApp(cfg, token, *insecure, configSource(path)...)
	if err != nil {
		return err
	}

	defer closeDB()

	return app.Run()
}

// configSource points Forge's own config loading (the dashboard's section, for
// one) at the same file the binary read, or turns discovery off when there is
// none.
func configSource(path string) []forge.AppOption {
	abs, err := filepath.Abs(path)
	if err != nil {
		return []forge.AppOption{forge.WithEnableConfigAutoDiscovery(false)}
	}

	if _, err := os.Stat(abs); err != nil {
		return []forge.AppOption{forge.WithEnableConfigAutoDiscovery(false)}
	}

	return []forge.AppOption{
		forge.WithConfigSearchPaths(filepath.Dir(abs)),
		forge.WithConfigBaseNames(filepath.Base(abs)),
		forge.WithConfigLocalNames("tether.local.yaml"),
	}
}

func main() {
	if err := run(os.Args[1:], os.Getenv, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
