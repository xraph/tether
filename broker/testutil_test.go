package broker

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	pahov3 "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/tenant"
)

// waitTimeout bounds every wait in these tests. Nothing sleeps: tests wait on
// channels and fail when this runs out.
const waitTimeout = 5 * time.Second

// Will is the last will a 3.1.1 test client registers.
type Will struct {
	Topic   string
	Payload string
	QoS     byte
}

// fakePerms is a PermLoader keyed by username.
type fakePerms struct {
	rules map[string]acl.Rules
}

func (f fakePerms) Load(_ context.Context, ident *auth.Identity) (acl.Rules, []string, error) {
	r, ok := f.rules[ident.Username]
	if !ok {
		return acl.Rules{}, nil, nil
	}

	return r, []string{"role_" + ident.Username}, nil
}

// testLogger returns a logger that writes nothing unless TETHER_TEST_LOG is set.
func testLogger() *slog.Logger {
	if os.Getenv("TETHER_TEST_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}

	return slog.New(slog.DiscardHandler)
}

// testConfig is the default config listening on a random loopback port.
func testConfig() Config {
	cfg := DefaultConfig()
	cfg.TCPAddress = "127.0.0.1:0"

	return cfg
}

// newTenant returns an enabled tenant with the given limits.
func newTenant(limits tenant.Limits) *tenant.Tenant {
	return &tenant.Tenant{ID: id.NewTenantID(), Name: "test", Enabled: true, Limits: limits}
}

// ident is an identity of tn that logs in with username user.
func ident(tn *tenant.Tenant, user string) *auth.Identity {
	return &auth.Identity{Tenant: tn, PrincipalKind: auth.PrincipalKindAPIKey, PrincipalID: "akey_" + user, Username: user}
}

// startBroker starts a broker on a random loopback port with static identities
// keyed by username and rules keyed by username.
func startBroker(t *testing.T, ids map[string]*auth.Identity, rules map[string]acl.Rules) *Broker {
	t.Helper()

	return startBrokerWith(t, testConfig(), ids, rules, nil)
}

// startBrokerWith is startBroker with a config and an optional hook on the
// broker before it starts (for the ACL spy).
func startBrokerWith(t *testing.T, cfg Config, ids map[string]*auth.Identity, rules map[string]acl.Rules, before func(*Broker)) *Broker {
	t.Helper()

	b, err := New(cfg, Deps{
		Auth:   auth.NewStatic(ids),
		Perms:  fakePerms{rules: rules},
		Logger: testLogger(),
	})
	require.NoError(t, err)

	if before != nil {
		before(b)
	}

	require.NoError(t, b.Start(t.Context()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()

		_ = b.Stop(ctx)
	})

	return b
}

// message is one received PUBLISH.
type message struct {
	Topic   string
	Payload string
}

// connect311 connects a 3.1.1 client to addr ("tcp://", "ssl://" or "ws://"
// URL, or a bare host:port meaning tcp) and returns it with its error.
func connect311(t *testing.T, addr, clientID, user string, will *Will) (pahov3.Client, error) {
	t.Helper()

	return connect311TLS(t, addr, clientID, user, will, nil)
}

func connect311TLS(t *testing.T, addr, clientID, user string, will *Will, tlsCfg *tls.Config) (pahov3.Client, error) {
	t.Helper()

	if _, _, err := net.SplitHostPort(addr); err == nil {
		addr = "tcp://" + addr
	}

	opts := pahov3.NewClientOptions().
		AddBroker(addr).
		SetClientID(clientID).
		SetUsername(user).
		SetPassword("secret").
		SetProtocolVersion(4).
		SetCleanSession(true).
		SetAutoReconnect(false).
		SetConnectRetry(false).
		SetConnectTimeout(waitTimeout)

	if tlsCfg != nil {
		opts.SetTLSConfig(tlsCfg)
	}

	if will != nil {
		opts.SetWill(will.Topic, will.Payload, will.QoS, false)
	}

	c := pahov3.NewClient(opts)
	tok := c.Connect()

	if !tok.WaitTimeout(waitTimeout) {
		return c, errors.New("connect timed out")
	}

	if err := tok.Error(); err != nil {
		return c, err
	}

	t.Cleanup(func() { c.Disconnect(0) })

	return c, nil
}

// subscribe311 subscribes c to filter and returns a channel of received messages.
func subscribe311(t *testing.T, c pahov3.Client, filter string) <-chan message {
	t.Helper()

	ch := make(chan message, 16)
	tok := c.Subscribe(filter, 1, func(_ pahov3.Client, m pahov3.Message) {
		ch <- message{Topic: m.Topic(), Payload: string(m.Payload())}
	})
	require.True(t, tok.WaitTimeout(waitTimeout), "subscribe timed out")
	require.NoError(t, tok.Error())

	return ch
}

// publish311 publishes at QoS 1 and waits for the PUBACK.
func publish311(t *testing.T, c pahov3.Client, name, payload string) {
	t.Helper()

	tok := c.Publish(name, 1, false, payload)
	require.True(t, tok.WaitTimeout(waitTimeout), "publish timed out")
	require.NoError(t, tok.Error())
}

// client5 is a 5.0 test client, the messages it receives and the DISCONNECT
// packets the server sends it.
type client5 struct {
	*paho.Client
	msgs        <-chan message
	disconnects <-chan *paho.Disconnect
}

// connect5 connects a 5.0 client over TCP. The connack is returned even when
// the connection is refused, so tests can read its reason code. A persistent
// client (cleanStart false) asks for a one-hour session expiry.
func connect5(t *testing.T, addr, clientID, user string, cleanStart bool) (*client5, *paho.Connack, error) {
	t.Helper()

	expiry := uint32(0)
	if !cleanStart {
		expiry = 3600
	}

	return dial5(t, addr, &paho.Connect{
		ClientID:     clientID,
		Username:     user,
		UsernameFlag: true,
		Password:     []byte("secret"),
		PasswordFlag: true,
		CleanStart:   cleanStart,
		KeepAlive:    30,
		Properties:   &paho.ConnectProperties{SessionExpiryInterval: &expiry},
	})
}

// dial5 sends cp over a new TCP connection to addr.
func dial5(t *testing.T, addr string, cp *paho.Connect) (*client5, *paho.Connack, error) {
	t.Helper()

	conn, err := dial(t, addr)
	require.NoError(t, err)

	msgs := make(chan message, 16)
	discs := make(chan *paho.Disconnect, 1)
	c := paho.NewClient(paho.ClientConfig{
		Conn: conn,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) {
				msgs <- message{Topic: pr.Packet.Topic, Payload: string(pr.Packet.Payload)}

				return true, nil
			},
		},
		OnServerDisconnect: func(d *paho.Disconnect) {
			select {
			case discs <- d:
			default:
			}
		},
	})

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	ca, err := c.Connect(ctx, cp)
	if err != nil {
		return nil, ca, err
	}

	t.Cleanup(func() { _ = c.Disconnect(&paho.Disconnect{ReasonCode: 0}) })

	return &client5{Client: c, msgs: msgs, disconnects: discs}, ca, nil
}

// subscribe5 subscribes at QoS 1 and returns the SUBACK reason codes.
func subscribe5(t *testing.T, c *client5, filters ...string) []byte {
	t.Helper()

	subs := make([]paho.SubscribeOptions, 0, len(filters))
	for _, f := range filters {
		subs = append(subs, paho.SubscribeOptions{Topic: f, QoS: 1})
	}

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	// paho returns the SUBACK with an error when any filter was refused.
	sa, err := c.Subscribe(ctx, &paho.Subscribe{Subscriptions: subs})
	if sa != nil {
		return sa.Reasons
	}

	require.NoError(t, err)

	return nil
}

// publish5 publishes at QoS 1 and returns the PUBACK reason code.
func publish5(t *testing.T, c *client5, name, payload string) byte {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	resp, err := c.Publish(ctx, &paho.Publish{Topic: name, QoS: 1, Payload: []byte(payload)})
	if resp != nil {
		return resp.ReasonCode
	}

	require.NoError(t, err)

	return 0
}

// recv waits for one message on ch.
func recv(t *testing.T, ch <-chan message) message {
	t.Helper()

	select {
	case m := <-ch:
		return m
	case <-time.After(waitTimeout):
		t.Fatal("no message received")

		return message{}
	}
}

// aclSpy records every OnACLCheck the broker sees.
type aclSpy struct {
	mu     sync.Mutex
	checks []aclCheck
}

type aclCheck struct {
	Topic string
	Write bool
}

func (s *aclSpy) observe(_ string, name string, write bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.checks = append(s.checks, aclCheck{Topic: name, Write: write})
}

func (s *aclSpy) all() []aclCheck {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]aclCheck(nil), s.checks...)
}

// selfSigned writes a self-signed certificate for 127.0.0.1 into dir and
// returns the cert and key paths and a pool trusting it.
func selfSigned(t *testing.T, dir string) (certFile, keyFile string, pool *x509.CertPool) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "tether-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600))

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	pool = x509.NewCertPool()
	pool.AddCert(cert)

	return certFile, keyFile, pool
}

// freeAddr returns a loopback address with a port that was free a moment ago,
// for the WebSocket listener, which binds its own address.
func freeAddr(t *testing.T) string {
	t.Helper()

	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := l.Addr().String()
	require.NoError(t, l.Close())

	return addr
}

// waitListening dials addr until it accepts or waitTimeout runs out.
func waitListening(t *testing.T, addr string) {
	t.Helper()

	deadline := time.After(waitTimeout)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()

	for {
		c, err := dial(t, addr)
		if err == nil {
			_ = c.Close()

			return
		}

		select {
		case <-deadline:
			t.Fatalf("%s never started listening: %v", addr, err)
		case <-tick.C:
		}
	}
}

// dial opens a TCP connection to addr.
func dial(t *testing.T, addr string) (net.Conn, error) {
	t.Helper()

	d := &net.Dialer{Timeout: waitTimeout}

	return d.DialContext(t.Context(), "tcp", addr)
}
