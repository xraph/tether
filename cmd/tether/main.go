// Command tether runs the Tether MQTT broker as a standalone server: a Forge
// app with grove (SQLite), keysmith, warden, the Tether extension and the
// Forge dashboard.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"

	"github.com/xraph/forge"
	dashboard "github.com/xraph/forge/extensions/dashboard"
	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/sqlitedriver"
	keysmithext "github.com/xraph/keysmith/extension"
	"github.com/xraph/vessel"
	wardenext "github.com/xraph/warden/extension"

	"github.com/xraph/tether"
	"github.com/xraph/tether/extension"
	"github.com/xraph/tether/store/sqlite"
)

// Defaults for the keys this binary adds on top of the extension's config.
const (
	defaultHTTPAddress = ":8080"
	defaultSQLitePath  = "./data/tether.db"
	defaultDataDir     = "./data/sessions"

	// defaultConfigFile is read when --config is not given and the file
	// exists in the working directory.
	defaultConfigFile = "tether.yaml"
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
	// roles. Its directory is created when missing.
	SQLitePath string `yaml:"sqlite_path"`
}

// defaultConfig is the config of a process started with no config file: HTTP
// on :8080, MQTT on :1883, and everything on disk under ./data.
func defaultConfig() config {
	cfg := extension.DefaultConfig()
	cfg.Broker.DataDir = defaultDataDir

	return config{
		Server: serverConfig{HTTPAddress: defaultHTTPAddress, SQLitePath: defaultSQLitePath},
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

	raw, err := os.ReadFile(path)
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

// newApp builds the Forge app: a grove SQLite database in the container,
// keysmith and warden with their own REST routes off, Tether, and the
// dashboard. It does not start it.
//
// The admin API is guarded by the bearer token. With an empty token it needs
// insecure, and then the dashboard intents are open too. The returned func
// closes the database and must run after the app has stopped.
func newApp(cfg config, token string, insecure bool, appOpts ...forge.AppOption) (forge.App, func(), error) {
	if token == "" && !insecure {
		return nil, nil, errNoAdminToken
	}

	if cfg.Server.SQLitePath == "" {
		return nil, nil, errors.New("tether: server.sqlite_path is empty")
	}

	for _, dir := range []string{filepath.Dir(cfg.Server.SQLitePath), cfg.Tether.Broker.DataDir} {
		if dir == "" {
			continue
		}

		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, nil, fmt.Errorf("tether: create data dir: %w", err)
		}
	}

	db, err := openDB(cfg.Server.SQLitePath)
	if err != nil {
		return nil, nil, err
	}

	app := forge.New(append([]forge.AppOption{
		forge.WithAppName("tether"),
		forge.WithAppVersion(tether.Version),
		forge.WithHTTPAddress(cfg.Server.HTTPAddress),
		// Environment variables are for the admin token only: the config
		// file is the one place everything else is set.
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

	for _, x := range []forge.Extension{
		ext.Keysmith(keysmithext.WithDisableRoutes()),
		ext.Warden(wardenext.WithDisableRoutes()),
		ext,
		dashboard.NewExtension(),
	} {
		if err := app.RegisterExtension(x); err != nil {
			_ = db.Close()

			return nil, nil, fmt.Errorf("tether: register %s: %w", x.Name(), err)
		}
	}

	return app, func() { _ = db.Close() }, nil
}

// openDB opens the grove SQLite database at path.
func openDB(path string) (*grove.DB, error) {
	drv := sqlitedriver.New()

	if err := drv.Open(context.Background(), sqlite.DSN(path)); err != nil {
		return nil, fmt.Errorf("tether: open sqlite %s: %w", path, err)
	}

	db, err := grove.Open(drv)
	if err != nil {
		return nil, fmt.Errorf("tether: open grove: %w", err)
	}

	return db, nil
}

// run is the whole program, with its inputs passed in so a test can drive it:
// the arguments after the program name, the environment, and where usage goes.
// It returns when the app has shut down on SIGINT or SIGTERM, or when it
// fails.
func run(args []string, getenv func(string) string, stderr io.Writer) error {
	fs := flag.NewFlagSet("tether", flag.ContinueOnError)
	fs.SetOutput(stderr)

	configPath := fs.String("config", "", "path to the YAML config file (default ./"+defaultConfigFile+" if it exists)")
	insecure := fs.Bool("insecure-admin", false, "leave the admin API and the dashboard open when "+adminTokenEnv+" is empty")
	showVersion := fs.Bool("version", false, "print the version and exit")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *showVersion {
		_, err := fmt.Fprintln(stderr, "tether", tether.Version)

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
	if err := run(os.Args[1:], os.Getenv, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
