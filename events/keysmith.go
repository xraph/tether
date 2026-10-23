package events

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/xraph/keysmith/key"
	"github.com/xraph/keysmith/rotation"
)

// pluginName is the name both plugins register under.
const pluginName = "tether"

// KeysmithPlugin is a keysmith plugin that reports key changes to a Handler.
//
// Keysmith takes plugins only when the engine is built
// (keysmith.WithExtension), usually before the broker that reacts to them
// exists, so the plugin is created first and bound to its Handler later with
// Bind. Events that arrive before Bind are dropped.
//
// Rotation reports KeyRotated(id, rotatedAt, graceEnds): the previous secret
// keeps working until the grace window ends. rotatedAt is when the hook ran,
// which is after keysmith stored the new secret. A rotation with no grace
// window (keysmith.WithGrace(0), what a compromised key gets) reports
// KeyInvalidated(id, true) instead, because the previous secret is dead at
// once and a connection made with it cannot be told from one made with the
// new secret. A rotation hook with no record reports KeyInvalidated(id,
// false). Revocation, suspension and expiry report KeyInvalidated(id, true).
// Every hook returns nil and recovers a panic from the Handler, because
// keysmith stops dispatching at the first plugin error and would fail the
// caller's operation.
type KeysmithPlugin struct {
	h     atomic.Pointer[handlerRef]
	wired atomic.Bool
}

// handlerRef boxes a Handler for an atomic pointer.
type handlerRef struct{ h Handler }

// NewKeysmithPlugin returns an unbound plugin.
func NewKeysmithPlugin() *KeysmithPlugin {
	return &KeysmithPlugin{}
}

// Name implements keysmith's plugin.Plugin.
func (p *KeysmithPlugin) Name() string { return pluginName }

// Bind sets the Handler that receives events from now on. A nil Handler
// unbinds.
func (p *KeysmithPlugin) Bind(h Handler) {
	if h == nil {
		p.h.Store(nil)

		return
	}

	p.h.Store(&handlerRef{h: h})
}

// MarkWired records that the plugin was registered with a keysmith engine.
// The host calls it after keysmith.WithExtension(p), so code that receives
// the plugin later can check Wired instead of trusting that it was.
func (p *KeysmithPlugin) MarkWired() { p.wired.Store(true) }

// Wired reports whether MarkWired was called.
func (p *KeysmithPlugin) Wired() bool { return p.wired.Load() }

// OnKeyRotated implements keysmith's plugin.KeyRotated.
func (p *KeysmithPlugin) OnKeyRotated(ctx context.Context, k *key.Key, rec *rotation.Record) error {
	switch {
	case rec == nil:
		p.invalidated(ctx, k, false)
	case rec.GraceTTL <= 0:
		p.invalidated(ctx, k, true)
	default:
		p.rotated(ctx, k, rec)
	}

	return nil
}

// rotated reports a rotation with a grace window. The window ends at
// rec.GraceEnds, or GraceTTL after rec.CreatedAt when keysmith left it unset.
//
// The rotation time it reports is now, when the hook runs, not
// rec.CreatedAt. Keysmith makes the record before it stores the new secret,
// so a CONNECT with the old secret (or one served from Tether's auth cache,
// which the reactor evicts only after this) can complete between the two.
// Cutting off at the hook catches that connection. The price is that a
// connection made with the new secret in the same gap gets the grace end as
// well, and simply reconnects then.
func (p *KeysmithPlugin) rotated(ctx context.Context, k *key.Key, rec *rotation.Record) {
	ref := p.h.Load()
	if ref == nil || k == nil || k.ID.IsNil() {
		return
	}

	defer recoverHandler()

	now := time.Now()

	ends := rec.GraceEnds
	if ends.IsZero() {
		from := rec.CreatedAt
		if from.IsZero() {
			from = now
		}

		ends = from.Add(rec.GraceTTL)
	}

	ref.h.KeyRotated(ctx, k.ID.String(), now, ends)
}

// OnKeyRevoked implements keysmith's plugin.KeyRevoked.
func (p *KeysmithPlugin) OnKeyRevoked(ctx context.Context, k *key.Key, _ string) error {
	p.invalidated(ctx, k, true)

	return nil
}

// OnKeySuspended implements keysmith's plugin.KeySuspended.
func (p *KeysmithPlugin) OnKeySuspended(ctx context.Context, k *key.Key) error {
	p.invalidated(ctx, k, true)

	return nil
}

// OnKeyExpired implements keysmith's plugin.KeyExpired. Keysmith fires it
// from ValidateKey when it finds the key expired.
func (p *KeysmithPlugin) OnKeyExpired(ctx context.Context, k *key.Key) error {
	p.invalidated(ctx, k, true)

	return nil
}

func (p *KeysmithPlugin) invalidated(ctx context.Context, k *key.Key, disconnect bool) {
	ref := p.h.Load()
	if ref == nil || k == nil || k.ID.IsNil() {
		return
	}

	defer recoverHandler()

	ref.h.KeyInvalidated(ctx, k.ID.String(), disconnect)
}

// recoverHandler stops a panic from a Handler before it reaches the engine
// that dispatched the hook, and logs it through slog's default logger: the
// plugins have no logger of their own, and a panic here is a bug.
func recoverHandler() {
	if r := recover(); r != nil {
		slog.Error("tether/events: handler panicked", slog.Any("panic", r))
	}
}
