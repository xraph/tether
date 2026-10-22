package extension

import (
	"slices"

	"github.com/xraph/forge"
	keysmithext "github.com/xraph/keysmith/extension"
	wardenext "github.com/xraph/warden/extension"
)

// Keysmith returns the keysmith extension with Tether's keysmith plugin
// registered, built from opts. Register it with the app instead of calling
// keysmithext.New yourself: keysmith takes plugins only when its engine is
// built, and without the plugin a revoked device key would keep its live
// MQTT connections. Start fails with ErrKeysmithNotWired when the app's
// keysmith was built some other way, unless WithoutKeysmithEvents is set.
func (e *Extension) Keysmith(opts ...keysmithext.ExtOption) forge.Extension {
	ext := keysmithext.New(append(slices.Clip(opts), keysmithext.WithHookExtension(e.ksPlugin))...)
	e.ksPlugin.MarkWired()

	return ext
}

// Warden returns the warden extension with Tether's warden plugin
// registered, built from opts. Registering the plugin at construction always
// works; registering it at Start (what happens when the app built warden
// some other way) needs the engine to have a plugin registry, which warden
// v1.8.0 creates only for an engine with a plugin, a decision cache or the
// audit log. Without one Start fails with ErrWardenNotWired, unless
// WithoutWardenEvents is set.
func (e *Extension) Warden(opts ...wardenext.Option) forge.Extension {
	ext := wardenext.New(append(slices.Clip(opts), wardenext.WithPlugin(e.wdPlugin))...)
	e.wdPlugin.MarkWired()

	return ext
}
