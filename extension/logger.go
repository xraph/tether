package extension

import (
	"context"
	"log/slog"
	"slices"

	"github.com/xraph/forge"
)

// slogHandler hands slog records to a forge logger, so the broker, mochi and
// the API (which take an *slog.Logger) log where the rest of the app does.
// Levels map to forge's Debug, Info, Warn and Error; the forge logger does
// the filtering. Groups become dotted key prefixes.
type slogHandler struct {
	l      forge.Logger
	fields []forge.Field
	prefix string
}

// newSlogLogger returns an *slog.Logger writing to l.
func newSlogLogger(l forge.Logger) *slog.Logger {
	return slog.New(&slogHandler{l: l})
}

// Enabled implements slog.Handler.
func (h *slogHandler) Enabled(context.Context, slog.Level) bool { return true }

// Handle implements slog.Handler.
func (h *slogHandler) Handle(_ context.Context, r slog.Record) error {
	fields := slices.Grow(slices.Clone(h.fields), r.NumAttrs())

	r.Attrs(func(a slog.Attr) bool {
		fields = appendAttr(fields, h.prefix, a)

		return true
	})

	switch {
	case r.Level >= slog.LevelError:
		h.l.Error(r.Message, fields...)
	case r.Level >= slog.LevelWarn:
		h.l.Warn(r.Message, fields...)
	case r.Level >= slog.LevelInfo:
		h.l.Info(r.Message, fields...)
	default:
		h.l.Debug(r.Message, fields...)
	}

	return nil
}

// WithAttrs implements slog.Handler.
func (h *slogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	fields := slices.Clone(h.fields)
	for _, a := range attrs {
		fields = appendAttr(fields, h.prefix, a)
	}

	return &slogHandler{l: h.l, fields: fields, prefix: h.prefix}
}

// WithGroup implements slog.Handler.
func (h *slogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}

	return &slogHandler{l: h.l, fields: h.fields, prefix: h.prefix + name + "."}
}

// appendAttr appends a as forge fields, flattening groups into prefixed keys.
func appendAttr(fields []forge.Field, prefix string, a slog.Attr) []forge.Field {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return fields
	}

	if a.Value.Kind() == slog.KindGroup {
		inner := prefix
		if a.Key != "" {
			inner += a.Key + "."
		}

		for _, g := range a.Value.Group() {
			fields = appendAttr(fields, inner, g)
		}

		return fields
	}

	return append(fields, forge.F(prefix+a.Key, a.Value.Any()))
}
