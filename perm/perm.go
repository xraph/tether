// Package perm reads and writes a role's MQTT topic grants in warden.
//
// Grants live in the role's metadata under [MetadataKey], as
//
//	{"tether": {"publish": ["devices/%c/up"], "subscribe": ["broadcast/#"]}}
//
// where "%c" and "%u" are the placeholders [acl.Compile] expands. [Loader]
// resolves the roles an API key holds, inherited ones included, and merges
// their grants for the broker. [Roles] is the management side: it reads and
// writes the grants of one role.
//
// Every warden call here runs under [scope.With], with the tenant's external ID
// as warden's tenant ID and the roles in the tenant's root namespace.
package perm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"slices"
	"time"

	"github.com/xraph/warden"
	"github.com/xraph/warden/role"

	"github.com/xraph/tether"
	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/scope"
	"github.com/xraph/tether/tenant"
	"github.com/xraph/tether/topic"
)

// MetadataKey is the role metadata key that holds the topic grants.
const MetadataKey = "tether"

const (
	publishKey   = "publish"
	subscribeKey = "subscribe"

	// rootNamespace is warden's tenant root namespace path.
	rootNamespace = ""
)

// RulesFromMetadata reads the topic grants out of a role's metadata.
//
// A missing [MetadataKey] gives empty rules and no error, and so does a
// missing or null "publish" or "subscribe". Anything else that is not a map of
// string lists is an error wrapping [tether.ErrInvalidInput] that names
// roleSlug.
//
// A list is accepted as []string, as []any holding only strings (what the
// sqlite and postgres stores return after a JSON round trip), or as any other
// slice or array of strings by reflection (bson.A from mongo). The map may be
// any map type with string keys for the same reason. The result shares no
// memory with md, and md is never modified.
//
// The roleSlug argument is an addition to the single-argument form in the
// plan: a bare metadata map cannot say which role it came from.
func RulesFromMetadata(roleSlug string, md map[string]any) (acl.Rules, error) {
	raw, ok := md[MetadataKey]
	if !ok {
		return acl.Rules{}, nil
	}

	inner, ok := asMap(raw)
	if !ok {
		return acl.Rules{}, fmt.Errorf("%w: role %q: metadata %q must be an object, got %T",
			tether.ErrInvalidInput, roleSlug, MetadataKey, raw)
	}

	pub, err := asStrings(inner[publishKey])
	if err != nil {
		return acl.Rules{}, fmt.Errorf("%w: role %q: metadata %q.%s: %w",
			tether.ErrInvalidInput, roleSlug, MetadataKey, publishKey, err)
	}

	sub, err := asStrings(inner[subscribeKey])
	if err != nil {
		return acl.Rules{}, fmt.Errorf("%w: role %q: metadata %q.%s: %w",
			tether.ErrInvalidInput, roleSlug, MetadataKey, subscribeKey, err)
	}

	return acl.Rules{Publish: pub, Subscribe: sub}, nil
}

// asMap returns v as a map[string]any. It accepts any map type whose keys are
// strings, so a named type such as bson.M works.
func asMap(v any) (map[string]any, bool) {
	if m, ok := v.(map[string]any); ok {
		return m, true
	}

	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Map || rv.Type().Key().Kind() != reflect.String {
		return nil, false
	}

	out := make(map[string]any, rv.Len())

	for it := rv.MapRange(); it.Next(); {
		out[it.Key().String()] = it.Value().Interface()
	}

	return out, true
}

// asStrings returns a fresh []string from a list value, nil for a nil value or
// an empty list. The error reads as the tail of a sentence.
func asStrings(v any) ([]string, error) {
	switch list := v.(type) {
	case nil:
		return nil, nil
	case []string:
		return slices.Clone(list), nil
	}

	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, fmt.Errorf("must be a list of strings, got %T", v)
	}

	var out []string

	for i := range rv.Len() {
		e := rv.Index(i)
		if e.Kind() == reflect.Interface {
			e = e.Elem()
		}

		if e.Kind() != reflect.String {
			return nil, fmt.Errorf("must be a list of strings, item %d is %s", i, describe(e))
		}

		out = append(out, e.String())
	}

	return out, nil
}

func describe(v reflect.Value) string {
	if !v.IsValid() {
		return "null"
	}

	return v.Type().String()
}

// Loader resolves an API key's topic grants from warden. It implements the
// broker's PermLoader. It is safe for concurrent use.
type Loader struct {
	eng *warden.Engine
	log *slog.Logger
}

// LoaderOption configures a [Loader].
type LoaderOption func(*Loader)

// WithLogger sets where [Loader.Load] reports roles it skipped because their
// metadata is malformed. The default discards them.
func WithLogger(l *slog.Logger) LoaderOption {
	return func(ld *Loader) {
		if l != nil {
			ld.log = l
		}
	}
}

// NewLoader returns a Loader that reads roles through eng.
func NewLoader(eng *warden.Engine, opts ...LoaderOption) *Loader {
	l := &Loader{eng: eng, log: slog.New(slog.DiscardHandler)}

	for _, o := range opts {
		o(l)
	}

	return l
}

// Load returns the merged topic grants of every role the identity's API key
// holds, inherited roles included, and the IDs of those roles.
//
// Roles are resolved in the identity's tenant by warden's SubjectRoles, which
// walks namespace ancestors and parent roles and leaves out expired and
// resource-scoped assignments. The role IDs cover every resolved role, whether
// or not it carries grants.
//
// A role whose [MetadataKey] metadata is malformed is skipped, so one bad role
// cannot lock a device out of the grants of its others. Its slug is logged at
// warn level when a logger is set. An identity that is nil, has no tenant, or
// is not an API key principal with an ID is an error wrapping
// [tether.ErrInvalidInput], as is any warden failure (wrapped as is).
func (l *Loader) Load(ctx context.Context, ident *auth.Identity) (acl.Rules, []string, error) {
	switch {
	case ident == nil || ident.Tenant == nil:
		return acl.Rules{}, nil, fmt.Errorf("%w: perm: identity with a tenant is required", tether.ErrInvalidInput)
	case ident.PrincipalKind != auth.PrincipalKindAPIKey || ident.PrincipalID == "":
		return acl.Rules{}, nil, fmt.Errorf("%w: perm: identity must be an API key principal with an ID, got kind %q",
			tether.ErrInvalidInput, ident.PrincipalKind)
	}

	t := ident.Tenant

	_, all, err := l.eng.SubjectRoles(scope.With(ctx, t.AppID, t.ExternalID), warden.SubjectAPIKey, ident.PrincipalID)
	if err != nil {
		return acl.Rules{}, nil, fmt.Errorf("tether/perm: load roles of %s: %w", ident.PrincipalID, err)
	}

	var (
		rules   = make([]acl.Rules, 0, len(all))
		roleIDs = make([]string, 0, len(all))
	)

	for _, r := range all {
		roleIDs = append(roleIDs, r.ID.String())

		rr, err := RulesFromMetadata(r.Slug, r.Metadata)
		if err != nil {
			l.log.Warn("tether/perm: skipping role with malformed grants",
				slog.String("role", r.Slug), slog.String("tenant", t.ExternalID), slog.Any("error", err))

			continue
		}

		rules = append(rules, rr)
	}

	return acl.Merge(rules...), roleIDs, nil
}

// Roles reads and writes the topic grants of roles in the tenant root
// namespace. It is safe for concurrent use, but a write is a read-modify-write
// of the role's metadata, so two concurrent writes to the same role can lose
// one of them.
type Roles struct {
	eng *warden.Engine
}

// NewRoles returns a Roles that works through eng.
func NewRoles(eng *warden.Engine) *Roles {
	return &Roles{eng: eng}
}

// GetRules returns the grants of the role with slug roleSlug in t. A role that
// does not exist is [tether.ErrNotFound], and one with malformed grants is
// [tether.ErrInvalidInput].
func (r *Roles) GetRules(ctx context.Context, t *tenant.Tenant, roleSlug string) (acl.Rules, error) {
	got, err := r.get(ctx, t, roleSlug)
	if err != nil {
		return acl.Rules{}, err
	}

	return RulesFromMetadata(got.Slug, got.Metadata)
}

// SetRules replaces the grants of the role with slug roleSlug in t. Every
// filter must pass topic.Valid as a filter, or the call fails with an error
// wrapping [tether.ErrInvalidInput] that names it, and nothing is written. A
// role that does not exist is [tether.ErrNotFound]. Other metadata keys on the
// role are kept.
//
// Warden's store writes fire no plugin hooks and do not touch its decision
// cache, so SetRules emits OnRoleUpdated itself and clears the tenant's cached
// decisions.
func (r *Roles) SetRules(ctx context.Context, t *tenant.Tenant, roleSlug string, rules acl.Rules) error {
	for _, f := range slices.Concat(rules.Publish, rules.Subscribe) {
		if err := topic.Valid(f, true, 0); err != nil {
			return fmt.Errorf("tether/perm: grant %q: %w", f, err)
		}
	}

	existing, err := r.get(ctx, t, roleSlug)
	if err != nil {
		return err
	}

	// The store may hand back the map it holds, so write to a copy.
	md := maps.Clone(existing.Metadata)
	if md == nil {
		md = make(map[string]any, 1)
	}

	md[MetadataKey] = map[string]any{
		publishKey:   nonNil(rules.Publish),
		subscribeKey: nonNil(rules.Subscribe),
	}

	existing.Metadata = md
	existing.UpdatedAt = time.Now().UTC()

	ctx = scope.With(ctx, t.AppID, t.ExternalID)

	if err := r.eng.Store().UpdateRole(ctx, existing); err != nil {
		return fmt.Errorf("tether/perm: update role %q: %w", roleSlug, err)
	}

	if p := r.eng.Plugins(); p != nil {
		p.EmitRoleUpdated(ctx, existing)
	}

	r.eng.InvalidateTenant(ctx, t.ExternalID)

	return nil
}

// get loads the role, mapping warden's not-found to tether's.
func (r *Roles) get(ctx context.Context, t *tenant.Tenant, roleSlug string) (*role.Role, error) {
	if t == nil || roleSlug == "" {
		return nil, fmt.Errorf("%w: perm: tenant and role slug are required", tether.ErrInvalidInput)
	}

	got, err := r.eng.Store().GetRoleBySlug(scope.With(ctx, t.AppID, t.ExternalID), t.ExternalID, rootNamespace, roleSlug)
	if err != nil {
		if errors.Is(err, warden.ErrNotFound) {
			return nil, fmt.Errorf("tether/perm: role %q: %w", roleSlug, tether.ErrNotFound)
		}

		return nil, fmt.Errorf("tether/perm: get role %q: %w", roleSlug, err)
	}

	return got, nil
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}

	return slices.Clone(s)
}
