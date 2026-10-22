package extension_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/forge"
	keysmithext "github.com/xraph/keysmith/extension"
	"github.com/xraph/warden/assignment"
	wardenext "github.com/xraph/warden/extension"
	wid "github.com/xraph/warden/id"

	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/extension"
	"github.com/xraph/tether/scope"
)

const (
	jwtIssuer   = "https://auth.example.com"
	jwtAudience = "tether"
	jwtUser     = "ausr_alice"
)

// idp is a JWKS endpoint and the key that signs for it.
type idp struct {
	key *rsa.PrivateKey
	srv *httptest.Server
}

func newIDP(t *testing.T) *idp {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	body, err := json.Marshal(map[string]any{"keys": []any{map[string]any{
		"kty": "RSA", "use": "sig", "alg": "RS256", "kid": "k1",
		"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	}}})
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(body) }))
	t.Cleanup(srv.Close)

	return &idp{key: key, srv: srv}
}

func (p *idp) config() auth.JWTConfig {
	return auth.JWTConfig{JWKSURL: p.srv.URL, Issuer: jwtIssuer, Audience: []string{jwtAudience}}
}

// token signs a token for the test tenant, with exp in ttl. extra overrides or
// adds claims; a nil value deletes one.
func (p *idp) token(t *testing.T, sub string, ttl time.Duration, extra map[string]any) string {
	t.Helper()

	now := time.Now()
	claims := jwt.MapClaims{
		"iss": jwtIssuer, "aud": []string{jwtAudience}, "sub": sub,
		"iat": now.Unix(), "exp": now.Add(ttl).Unix(),
		"app_id": testApp, "org_id": testOrg,
	}

	for k, v := range extra {
		if v == nil {
			delete(claims, k)
		} else {
			claims[k] = v
		}
	}

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = "k1"

	s, err := tok.SignedString(p.key)
	require.NoError(t, err)

	return s
}

// client5 is a connected MQTT 5.0 client that records server disconnects.
type client5 struct {
	*paho.Client
	disconnects chan *paho.Disconnect
}

// connect5 connects over TCP with username and password. The CONNACK comes
// back even when the broker refuses, so tests can read its reason code.
func (e *env) connect5(clientID, password string) (*client5, *paho.Connack, error) {
	e.t.Helper()

	conn, err := (&net.Dialer{Timeout: waitTimeout}).DialContext(e.t.Context(), "tcp", e.ext.Broker().Addr().String())
	require.NoError(e.t, err)

	discs := make(chan *paho.Disconnect, 1)
	c := paho.NewClient(paho.ClientConfig{
		Conn: conn,
		OnServerDisconnect: func(d *paho.Disconnect) {
			select {
			case discs <- d:
			default:
			}
		},
	})

	ctx, cancel := context.WithTimeout(e.t.Context(), waitTimeout)
	defer cancel()

	ca, err := c.Connect(ctx, &paho.Connect{
		ClientID: clientID, Username: jwtUser, UsernameFlag: true, Password: []byte(password), PasswordFlag: true,
		CleanStart: true, KeepAlive: 30,
	})
	if err != nil {
		_ = conn.Close()

		return nil, ca, err
	}

	e.t.Cleanup(func() { _ = c.Disconnect(&paho.Disconnect{ReasonCode: 0}) })

	return &client5{Client: c, disconnects: discs}, ca, nil
}

func (c *client5) subscribeReason(t *testing.T, filter string) byte {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	sa, err := c.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: 1}}})
	if sa != nil {
		return sa.Reasons[0]
	}

	require.NoError(t, err)

	return 0
}

func (c *client5) publishReason(t *testing.T, topic string) byte {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	resp, err := c.Publish(ctx, &paho.Publish{Topic: topic, QoS: 1, Payload: []byte("x")})
	if resp != nil {
		return resp.ReasonCode
	}

	require.NoError(t, err)

	return 0
}

// assignUser gives the test user (jwtUser) the role with slug, the way authsome's warden
// bridge does, and tells warden's plugins.
func (e *env) assignUser(slug string) {
	e.t.Helper()

	ctx := scope.With(e.t.Context(), testApp, testOrg)

	r, err := e.wd.Store().GetRoleBySlug(ctx, testOrg, "", slug)
	require.NoError(e.t, err)

	a := &assignment.Assignment{
		ID: wid.NewAssignmentID(), TenantID: testOrg, RoleID: r.ID,
		SubjectKind: "user", SubjectID: jwtUser, CreatedAt: time.Now().UTC(),
	}
	require.NoError(e.t, e.wd.Store().CreateAssignment(ctx, a))

	e.wd.InvalidateTenant(ctx, testOrg)
	e.wd.Plugins().EmitRoleAssigned(ctx, a)
}

func startWithJWT(t *testing.T, p *idp) *env {
	t.Helper()

	return startApp(t, newApp(t), false,
		extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(loopback()), extension.WithJWT(p.config()))
}

func TestJWTUserConnectsWithWardenGrantsAndLiveRoleChanges(t *testing.T) {
	p := newIDP(t)
	e := startWithJWT(t, p)
	tid := e.tenant()

	e.role(tid, "member", []string{"users/%u/up"}, []string{"users/%u/down"})
	e.role(tid, "extra", nil, []string{"extra/#"})
	e.assignUser("member")

	c, ca, err := e.connect5("alice-1", p.token(t, jwtUser, time.Hour, nil))
	require.NoError(t, err)
	require.Equal(t, byte(0), ca.ReasonCode)

	assert.Equal(t, byte(1), c.subscribeReason(t, "users/"+jwtUser+"/down"), "granted by the user's role")
	assert.Equal(t, byte(0), c.publishReason(t, "users/"+jwtUser+"/up"))
	assert.Equal(t, byte(0x87), c.subscribeReason(t, "extra/#"), "not granted yet")
	assert.Equal(t, byte(0x87), c.publishReason(t, "users/someone-else/up"), "%u is the user's own ID")

	// A role assigned to the user while connected reaches the live connection.
	e.assignUser("extra")

	assert.Equal(t, byte(1), c.subscribeReason(t, "extra/#"))
}

func TestJWTUserWithNoRolesConnectsAndCanDoNothing(t *testing.T) {
	p := newIDP(t)
	e := startWithJWT(t, p)
	e.tenant()

	c, _, err := e.connect5("alice-1", p.token(t, jwtUser, time.Hour, nil))
	require.NoError(t, err)

	assert.Equal(t, byte(0x87), c.subscribeReason(t, "anything"))
}

func TestJWTConnectionEndsAtExpiry(t *testing.T) {
	p := newIDP(t)
	e := startWithJWT(t, p)
	tid := e.tenant()

	e.role(tid, "member", []string{"up"}, nil)
	e.assignUser("member")

	start := time.Now()

	c, _, err := e.connect5("alice-1", p.token(t, jwtUser, 3*time.Second, nil))
	require.NoError(t, err)

	select {
	case d := <-c.disconnects:
		assert.Equal(t, byte(0xA0), d.ReasonCode, "maximum connect time")
		assert.Less(t, time.Since(start), 4*time.Second, "at exp, not later")
	case <-time.After(waitTimeout):
		require.FailNow(t, "the connection outlived the token")
	}
}

func TestJWTRefusals(t *testing.T) {
	p := newIDP(t)
	e := startWithJWT(t, p)
	e.tenant()

	cases := map[string]struct {
		token string
		code  byte
	}{
		"no org_id":       {p.token(t, jwtUser, time.Hour, map[string]any{"org_id": nil}), 0x87},
		"unknown org":     {p.token(t, jwtUser, time.Hour, map[string]any{"org_id": "aorg_other"}), 0x87},
		"agent token":     {p.token(t, jwtUser, time.Hour, map[string]any{"pk": "agent"}), 0x87},
		"wrong issuer":    {p.token(t, jwtUser, time.Hour, map[string]any{"iss": "https://evil.example.com"}), 0x86},
		"wrong audience":  {p.token(t, jwtUser, time.Hour, map[string]any{"aud": "other"}), 0x86},
		"expired":         {p.token(t, jwtUser, -time.Hour, nil), 0x86},
		"not a token":     {"eyJ.not.atoken", 0x86},
		"foreign subject": {p.token(t, "ausr_bob", time.Hour, nil), 0x86},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, ca, err := e.connect5("c-"+name, tc.token)
			require.Error(t, err)
			require.NotNil(t, ca)
			assert.Equal(t, tc.code, ca.ReasonCode)
		})
	}
}

func TestJWTLeavesDeviceKeysWorking(t *testing.T) {
	p := newIDP(t)
	e := startWithJWT(t, p)
	tid := e.tenant()

	e.role(tid, "sensor", []string{"up/%c"}, []string{"down/%c"})
	d := e.device(tid, "sensor")

	c, err := e.connect(d, "pump-1")
	require.NoError(t, err)

	c.subscribe(t, "down/pump-1")
	c.publish(t, "up/pump-1", "hello")
}

func TestJWTConfigFromFile(t *testing.T) {
	p := newIDP(t)

	dir := t.TempDir()
	yaml := "extensions:\n  tether:\n    broker:\n      tcp_address: 127.0.0.1:0\n" +
		"    jwt:\n      jwks_url: " + p.srv.URL + "\n      issuer: " + jwtIssuer + "\n" +
		"      audience: [" + jwtAudience + "]\n      algorithms: [RS256]\n      refresh_interval: 5m\n      leeway: 10s\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(yaml), 0o600))

	app := newAppWith(t, forge.WithConfigSearchPaths(dir), forge.WithEnableAppScopedConfig(false))
	e := startApp(t, app, false, extension.WithInsecureAdminRoutes())
	tid := e.tenant()

	e.role(tid, "member", nil, []string{"hello"})
	e.assignUser("member")

	c, _, err := e.connect5("alice-1", p.token(t, jwtUser, time.Hour, nil))
	require.NoError(t, err)
	assert.Equal(t, byte(1), c.subscribeReason(t, "hello"))
}

func TestJWTWithoutConfigRefusesTokens(t *testing.T) {
	p := newIDP(t)
	e := startApp(t, newApp(t), false, extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(loopback()))
	e.tenant()

	_, ca, err := e.connect5("alice-1", p.token(t, jwtUser, time.Hour, nil))
	require.Error(t, err)
	assert.Equal(t, byte(0x86), ca.ReasonCode, "a token is just a wrong key when JWT is off")
}

func TestJWTBadConfigFailsStart(t *testing.T) {
	app := newApp(t)
	ext := extension.New(extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(loopback()),
		extension.WithJWT(auth.JWTConfig{Issuer: jwtIssuer, Audience: []string{jwtAudience}}))

	err := boot(t, app, ext.Keysmith(keysmithext.WithDisableRoutes()), ext.Warden(wardenext.WithDisableRoutes()), ext)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "jwks_url")
}
