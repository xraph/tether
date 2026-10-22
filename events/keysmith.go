package events

import (
	"context"
	"log/slog"
	"sync/atomic"

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
// Rotation reports KeyInvalidated(id, false): the previous secret keeps
// working for the rotation's grace window. A rotation with no grace window
// (keysmith.WithGrace(0), what a compromised key gets) reports
// KeyInvalidated(id, true) instead, because the previous secret is dead at
// once and a connection made with it cannot be told from one made with the
// new secret. Revocation, suspension and expiry report
// KeyInvalidated(id, true). Every hook returns nil and
// recovers a panic from the Handler, because keysmith stops dispatching at
// the first plugin error and would fail the caller's operation.
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
	p.invalidated(ctx, k, rec != nil && rec.GraceTTL <= 0)

	return nil
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
