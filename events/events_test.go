package events_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/keysmith"
	kmemory "github.com/xraph/keysmith/store/memory"
	"github.com/xraph/warden"
	"github.com/xraph/warden/role"
	wmemory "github.com/xraph/warden/store/memory"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/broker"
	"github.com/xraph/tether/events"
	"github.com/xraph/tether/perm"
	"github.com/xraph/tether/scope"
	"github.com/xraph/tether/service"
	"github.com/xraph/tether/store/memory"
	"github.com/xraph/tether/tenant"
)

const (
	testApp = "aapp_1"
	testOrg = "aorg_1"

	// waitTimeout bounds every wait. Nothing sleeps.
	waitTimeout = 5 * time.Second
)

// env is a started broker with real keysmith and warden engines whose
// plugin hooks reach the broker through the events package, wired the way
// the extension wires them.
type env struct {
	b       *broker.Broker
	addr    string
	tenant  *tenant.Tenant
	devices *service.Devices
	roles   *perm.Roles
	bridge  *service.WardenRoles
	wd      *warden.Engine
	wctx    context.Context
}

func newEnv(t *testing.T) *env {
	t.Helper()

	st := memory.New()
	t.Cleanup(func() { _ = st.Close() })

	tenants := service.NewTenants(st)

	tn, err := tenants.Create(t.Context(), service.CreateTenantInput{AppID: testApp, ExternalID: testOrg, Name: "Acme"})
	require.NoError(t, err)

	ks := events.NewKeysmithPlugin()

	km, err := keysmith.NewEngine(keysmith.WithStore(kmemory.New()), keysmith.WithExtension(ks))
	require.NoError(t, err)

	ks.MarkWired()

	wp := events.NewWardenPlugin(nil)

	wd, err := warden.NewEngine(warden.WithStore(wmemory.New()), warden.WithPlugin(wp))
	require.NoError(t, err)

	authn := auth.NewKeysmith(km, tenants, st)

	cfg := broker.DefaultConfig()
	cfg.TCPAddress = "127.0.0.1:0"

	b, err := broker.New(cfg, broker.Deps{Auth: authn, Perms: perm.NewLoader(wd)})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()

		_ = b.Stop(ctx)
	})

	reactor := events.NewReactor(authn.Evict, b)
	ks.Bind(reactor)
	wp.Bind(reactor)

	bridge := service.NewWardenRoles(wd)

	return &env{
		b:       b,
		addr:    b.Addr().String(),
		tenant:  tn,
		devices: service.NewDevices(st, tenants, km, bridge),
		roles:   perm.NewRoles(wd),
		bridge:  bridge,
		wd:      wd,
		wctx:    scope.With(t.Context(), testApp, testOrg),
	}
}

// role creates a role in the tenant's root namespace with grants.
func (e *env) role(t *testing.T, slug string, rules acl.Rules) {
	t.Helper()

	require.NoError(t, e.wd.Store().CreateRole(e.wctx, &role.Role{
		TenantID: testOrg, AppID: testApp, Name: slug, Slug: slug,
		Metadata: map[string]any{perm.MetadataKey: map[string]any{
			"publish": nonNil(rules.Publish), "subscribe": nonNil(rules.Subscribe),
		}},
	}))
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}

	return s
}

// provision makes a device holding roles.
func (e *env) provision(t *testing.T, in service.ProvisionInput) *service.Credentials {
	t.Helper()

	if in.Name == "" {
		in.Name = "device"
	}

	c, err := e.devices.Provision(t.Context(), e.tenant.ID, in)
	require.NoError(t, err)

	return c
}

// message is one received PUBLISH.
type message struct {
	Topic   string
	Payload string
}

// client is a 5.0 test client with what it receives.
type client struct {
	*paho.Client
	msgs        <-chan message
	disconnects <-chan *paho.Disconnect
}

// connect connects a 5.0 client with clean start. The CONNACK comes back
// even when the connection is refused.
func connect(t *testing.T, addr, clientID, username, password string) (*client, *paho.Connack, error) {
	t.Helper()

	d := &net.Dialer{Timeout: waitTimeout}

	conn, err := d.DialContext(t.Context(), "tcp", addr)
	require.NoError(t, err)

	msgs := make(chan message, 1024)
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

	ca, err := c.Connect(ctx, &paho.Connect{
		ClientID:     clientID,
		Username:     username,
		UsernameFlag: true,
		Password:     []byte(password),
		PasswordFlag: true,
		CleanStart:   true,
		KeepAlive:    30,
	})
	if err != nil {
		_ = conn.Close()

		return nil, ca, err
	}

	t.Cleanup(func() { _ = c.Disconnect(&paho.Disconnect{ReasonCode: 0}) })

	return &client{Client: c, msgs: msgs, disconnects: discs}, ca, nil
}

func mustConnect(t *testing.T, addr, clientID string, creds *service.Credentials) *client {
	t.Helper()

	c, _, err := connect(t, addr, clientID, creds.Username, creds.Password)
	require.NoError(t, err)

	return c
}

// subscribe subscribes at QoS 1 and returns the SUBACK reason codes.
func subscribe(t *testing.T, c *client, filters ...string) []byte {
	t.Helper()

	subs := make([]paho.SubscribeOptions, 0, len(filters))
	for _, f := range filters {
		subs = append(subs, paho.SubscribeOptions{Topic: f, QoS: 1})
	}

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	sa, err := c.Subscribe(ctx, &paho.Subscribe{Subscriptions: subs})
	if sa != nil {
		return sa.Reasons
	}

	require.NoError(t, err)

	return nil
}

// publish publishes at QoS 1 and returns the PUBACK reason code.
func publish(t *testing.T, c *client, name, payload string) byte {
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

// disconnectedWithin waits up to d for the server's DISCONNECT and returns
// its reason code.
func disconnectedWithin(t *testing.T, c *client, d time.Duration) byte {
	t.Helper()

	select {
	case disc := <-c.disconnects:
		return disc.ReasonCode
	case <-time.After(d):
		t.Fatalf("not disconnected within %s", d)

		return 0
	}
}

func assertConnected(t *testing.T, c *client) {
	t.Helper()

	select {
	case d := <-c.disconnects:
		t.Fatalf("disconnected with reason %#x", d.ReasonCode)
	default:
	}
}

func TestRevokeDisconnectsClient(t *testing.T) {
	e := newEnv(t)
	e.role(t, "dev", acl.Rules{Subscribe: []string{"cmd/#"}})
	creds := e.provision(t, service.ProvisionInput{Roles: []string{"dev"}})

	c := mustConnect(t, e.addr, "dev-1", creds)
	require.Equal(t, []byte{1}, subscribe(t, c, "cmd/#"))

	require.NoError(t, e.devices.Revoke(t.Context(), e.tenant.ID, creds.Device.ID, "stolen"))

	assert.Equal(t, byte(0x98), disconnectedWithin(t, c, time.Second), "administrative action")

	_, ca, err := connect(t, e.addr, "dev-1", creds.Username, creds.Password)
	require.Error(t, err)
	require.NotNil(t, ca)
	assert.Equal(t, byte(0x86), ca.ReasonCode, "the revoked key is not served from the validation cache")
}

func TestRotateKeepsConnection(t *testing.T) {
	e := newEnv(t)
	e.role(t, "dev", acl.Rules{Publish: []string{"up"}})
	creds := e.provision(t, service.ProvisionInput{Roles: []string{"dev"}})

	c := mustConnect(t, e.addr, "dev-1", creds)

	rotated, err := e.devices.Rotate(t.Context(), e.tenant.ID, creds.Device.ID, time.Hour)
	require.NoError(t, err)
	require.NotEqual(t, creds.Password, rotated.Password)

	// The reaction ran inside Rotate. A DISCONNECT would have been written
	// before Rotate returned, and this PUBACK comes after it.
	assert.Equal(t, byte(0), publish(t, c, "up", "still here"))
	assertConnected(t, c)

	mustConnect(t, e.addr, "dev-2", creds)   // the old password, inside the grace window
	mustConnect(t, e.addr, "dev-3", rotated) // and the new one
}

func TestRoleChangeDropsSubscription(t *testing.T) {
	e := newEnv(t)
	e.role(t, "dev", acl.Rules{Subscribe: []string{"cmd/#", "ok/#"}})
	e.role(t, "pub", acl.Rules{Publish: []string{"#"}})

	devCreds := e.provision(t, service.ProvisionInput{Roles: []string{"dev"}})
	pubCreds := e.provision(t, service.ProvisionInput{Roles: []string{"pub"}})

	c := mustConnect(t, e.addr, "dev-1", devCreds)
	require.Equal(t, []byte{1, 1}, subscribe(t, c, "cmd/#", "ok/#"))

	p := mustConnect(t, e.addr, "pub-1", pubCreds)

	require.NoError(t, e.roles.SetRules(t.Context(), e.tenant, "dev", acl.Rules{Subscribe: []string{"ok/#"}}))

	assert.Equal(t, 1, e.b.Stats(e.tenant.ID.String()).Subscriptions, "cmd/# is gone from the session")

	// One publisher, QoS 1: by the time ok/s is fanned out, cmd/x's fan-out
	// is over, so ok/s must be the first thing the subscriber gets.
	require.Equal(t, byte(0), publish(t, p, "cmd/x", "secret"))
	require.Equal(t, byte(0), publish(t, p, "ok/s", "sentinel"))
	assert.Equal(t, message{Topic: "ok/s", Payload: "sentinel"}, recv(t, c.msgs))

	assert.Equal(t, []byte{0x87}, subscribe(t, c, "cmd/#"), "the grant is gone for new subscriptions too")
	assertConnected(t, c)
}

func TestRoleAssignmentGrantsLiveClient(t *testing.T) {
	e := newEnv(t)
	e.role(t, "dev", acl.Rules{Subscribe: []string{"cmd/#"}})
	creds := e.provision(t, service.ProvisionInput{})

	c := mustConnect(t, e.addr, "dev-1", creds)
	require.Equal(t, []byte{0x87}, subscribe(t, c, "cmd/#"))

	// RoleAssigned reaches the broker as SubjectChanged("api_key", key ID).
	require.NoError(t, e.bridge.AssignRoleToAPIKey(e.wctx, testOrg, creds.Username, "dev"))

	assert.Equal(t, []byte{1}, subscribe(t, c, "cmd/#"))
}

func TestRoleUnassignmentDropsSubscription(t *testing.T) {
	e := newEnv(t)
	e.role(t, "dev", acl.Rules{Subscribe: []string{"cmd/#"}})
	creds := e.provision(t, service.ProvisionInput{Roles: []string{"dev"}})

	c := mustConnect(t, e.addr, "dev-1", creds)
	require.Equal(t, []byte{1}, subscribe(t, c, "cmd/#"))
	require.Equal(t, 1, e.b.Stats(e.tenant.ID.String()).Subscriptions)

	require.NoError(t, e.devices.SetRoles(t.Context(), e.tenant.ID, creds.Device.ID, nil))

	assert.Equal(t, 0, e.b.Stats(e.tenant.ID.String()).Subscriptions)
	assert.Equal(t, []byte{0x87}, subscribe(t, c, "cmd/#"))
	assertConnected(t, c)
}

// Controller ruling R12: adding a role must not take away the subscriptions
// of a role the device keeps. SetRoles applies only the difference, so no
// reload ever sees the device without role a.
func TestSetRolesKeepsSubscriptionOfKeptRole(t *testing.T) {
	e := newEnv(t)
	e.role(t, "a", acl.Rules{Subscribe: []string{"cmd/#"}})
	e.role(t, "b", acl.Rules{Subscribe: []string{"extra/#"}})
	e.role(t, "pub", acl.Rules{Publish: []string{"#"}})

	creds := e.provision(t, service.ProvisionInput{Roles: []string{"a"}})
	pubCreds := e.provision(t, service.ProvisionInput{Roles: []string{"pub"}})

	c := mustConnect(t, e.addr, "dev-1", creds)
	require.Equal(t, []byte{1}, subscribe(t, c, "cmd/#"))

	p := mustConnect(t, e.addr, "pub-1", pubCreds)

	require.NoError(t, e.devices.SetRoles(t.Context(), e.tenant.ID, creds.Device.ID, []string{"a", "b"}))

	assert.Equal(t, 1, e.b.Stats(e.tenant.ID.String()).Subscriptions, "cmd/# survived")

	require.Equal(t, byte(0), publish(t, p, "cmd/x", "still"))
	assert.Equal(t, message{Topic: "cmd/x", Payload: "still"}, recv(t, c.msgs))

	assert.Equal(t, []byte{1}, subscribe(t, c, "extra/#"), "b's grant is live")
	assertConnected(t, c)
}

func TestKeyExpiryDisconnects(t *testing.T) {
	e := newEnv(t)
	e.role(t, "dev", acl.Rules{Subscribe: []string{"cmd/#"}})

	start := time.Now()
	exp := start.Add(time.Second)
	creds := e.provision(t, service.ProvisionInput{Roles: []string{"dev"}, ExpiresAt: &exp})

	c := mustConnect(t, e.addr, "dev-1", creds)

	assert.Equal(t, byte(0xA0), disconnectedWithin(t, c, 2*time.Second-time.Since(start)), "maximum connect time")
	assert.False(t, time.Now().Before(exp), "not before the key expired")
}
