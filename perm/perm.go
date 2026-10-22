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
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/xraph/warden"
	wid "github.com/xraph/warden/id"
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

// Roles manages the roles of a tenant in its root namespace and their topic
// grants: create, list and delete a role, and read and replace its grants.
// It is safe for concurrent use, but a write to a role's grants is a
// read-modify-write of its metadata, so two concurrent writes to the same role
// can lose one of them.
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

// maxSlugLen is the longest role slug Create accepts.
const maxSlugLen = 63

// slugPattern is what Create accepts as a slug: lower case letters, digits,
// underscores and hyphens, starting with a letter or digit. It is a path
// segment of the REST API, so it needs no escaping.
var slugPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

// listPage is how many roles List asks warden for at a time.
const listPage = 200

// RoleInput is what [Roles.Create] takes. Slug is required and Name defaults
// to it. ParentSlug names a role of the same tenant whose grants this one
// inherits. Rules are the topic grants, which may be empty.
type RoleInput struct {
	Slug       string
	Name       string
	ParentSlug string
	Rules      acl.Rules
}

// RoleInfo is a role as the admin API shows it. The grants are empty for a role
// whose grants metadata is malformed, which is also what the broker grants it.
type RoleInfo struct {
	Slug       string `json:"slug"`
	Name       string `json:"name"`
	ParentSlug string `json:"parent_slug,omitempty"`

	// Rules is embedded, so the JSON has "publish" and "subscribe" at the
	// top level, the same shape the topics routes use.
	acl.Rules
}

// Create makes a role in t's root namespace and returns it. The slug must be
// 1 to 63 characters of a-z, 0-9, "_" and "-", starting with a letter or
// digit; every filter must pass topic.Valid as a filter; a parent that does
// not exist is an error. All of those wrap [tether.ErrInvalidInput], and
// nothing is written. A slug the tenant already uses is [tether.ErrConflict].
//
// Warden's store writes fire no plugin hooks and do not touch its decision
// cache, so Create emits OnRoleCreated itself and clears the tenant's cached
// decisions.
func (r *Roles) Create(ctx context.Context, t *tenant.Tenant, in RoleInput) (*RoleInfo, error) {
	if t == nil {
		return nil, fmt.Errorf("%w: perm: tenant is required", tether.ErrInvalidInput)
	}

	slug := strings.TrimSpace(in.Slug)

	switch {
	case slug == "":
		return nil, fmt.Errorf("%w: perm: role slug is required", tether.ErrInvalidInput)
	case len(slug) > maxSlugLen || !slugPattern.MatchString(slug):
		return nil, fmt.Errorf("%w: perm: role slug %q must be 1 to %d characters of a-z, 0-9, _ and -, starting with a letter or digit",
			tether.ErrInvalidInput, slug, maxSlugLen)
	}

	for _, f := range slices.Concat(in.Rules.Publish, in.Rules.Subscribe) {
		if err := topic.Valid(f, true, 0); err != nil {
			return nil, fmt.Errorf("tether/perm: grant %q: %w", f, err)
		}
	}

	ctx = scope.With(ctx, t.AppID, t.ExternalID)

	if in.ParentSlug != "" {
		if _, err := r.eng.Store().GetRoleBySlug(ctx, t.ExternalID, rootNamespace, in.ParentSlug); err != nil {
			if errors.Is(err, warden.ErrNotFound) {
				return nil, fmt.Errorf("%w: perm: parent role %q does not exist", tether.ErrInvalidInput, in.ParentSlug)
			}

			return nil, fmt.Errorf("tether/perm: get parent role %q: %w", in.ParentSlug, err)
		}
	}

	name := strings.TrimSpace(in.Name)
	if name == "" {
		name = slug
	}

	now := time.Now().UTC()
	rl := &role.Role{
		ID:         wid.NewRoleID(),
		TenantID:   t.ExternalID,
		AppID:      t.AppID,
		Name:       name,
		Slug:       slug,
		ParentSlug: in.ParentSlug,
		Metadata: map[string]any{MetadataKey: map[string]any{
			publishKey:   nonNil(in.Rules.Publish),
			subscribeKey: nonNil(in.Rules.Subscribe),
		}},
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := r.eng.Store().CreateRole(ctx, rl); err != nil {
		if errors.Is(err, warden.ErrAlreadyExists) {
			return nil, fmt.Errorf("%w: perm: role %q already exists", tether.ErrConflict, slug)
		}

		return nil, fmt.Errorf("tether/perm: create role %q: %w", slug, err)
	}

	if p := r.eng.Plugins(); p != nil {
		p.EmitRoleCreated(ctx, rl)
	}

	r.eng.InvalidateTenant(ctx, t.ExternalID)

	return info(rl), nil
}

// List returns every role in t's root namespace, sorted by slug.
func (r *Roles) List(ctx context.Context, t *tenant.Tenant) ([]RoleInfo, error) {
	if t == nil {
		return nil, fmt.Errorf("%w: perm: tenant is required", tether.ErrInvalidInput)
	}

	ctx = scope.With(ctx, t.AppID, t.ExternalID)
	root := rootNamespace
	out := []RoleInfo{}

	for offset := 0; ; offset += listPage {
		page, err := r.eng.Store().ListRoles(ctx, &role.ListFilter{
			TenantID: t.ExternalID, NamespacePath: &root, Limit: listPage, Offset: offset,
		})
		if err != nil {
			return nil, fmt.Errorf("tether/perm: list roles: %w", err)
		}

		for _, rl := range page {
			out = append(out, *info(rl))
		}

		if len(page) < listPage {
			break
		}
	}

	slices.SortFunc(out, func(a, b RoleInfo) int { return strings.Compare(a.Slug, b.Slug) })

	return out, nil
}

// Delete removes the role with slug roleSlug from t. A role that does not
// exist is [tether.ErrNotFound]. A role still assigned to an API key, or still
// the parent of another role, is [tether.ErrConflict] and nothing changes:
// take it off its devices first. A system role is a conflict too.
//
// Like Create, it emits the warden plugin hook (OnRoleDeleted) and clears the
// tenant's cached decisions. The checks and the delete are separate warden
// calls, so an assignment made in between is not caught.
func (r *Roles) Delete(ctx context.Context, t *tenant.Tenant, roleSlug string) error {
	rl, err := r.get(ctx, t, roleSlug)
	if err != nil {
		return err
	}

	ctx = scope.With(ctx, t.AppID, t.ExternalID)

	assigned, err := r.eng.Store().ListSubjectsForRole(ctx, t.ExternalID, rl.ID)
	if err != nil {
		return fmt.Errorf("tether/perm: list assignments of role %q: %w", roleSlug, err)
	}

	if len(assigned) > 0 {
		return fmt.Errorf("%w: perm: role %q is assigned to %d subject(s)", tether.ErrConflict, roleSlug, len(assigned))
	}

	children, err := r.eng.Store().ListChildRoles(ctx, t.ExternalID, roleSlug)
	if err != nil {
		return fmt.Errorf("tether/perm: list children of role %q: %w", roleSlug, err)
	}

	if len(children) > 0 {
		return fmt.Errorf("%w: perm: role %q is the parent of %d role(s)", tether.ErrConflict, roleSlug, len(children))
	}

	if err := r.eng.Store().DeleteRole(ctx, t.ExternalID, rl.ID); err != nil {
		switch {
		case errors.Is(err, warden.ErrNotFound):
			return fmt.Errorf("tether/perm: role %q: %w", roleSlug, tether.ErrNotFound)
		case errors.Is(err, warden.ErrSystemRoleImmutable):
			return fmt.Errorf("%w: perm: role %q is a system role", tether.ErrConflict, roleSlug)
		}

		return fmt.Errorf("tether/perm: delete role %q: %w", roleSlug, err)
	}

	if p := r.eng.Plugins(); p != nil {
		p.EmitRoleDeleted(ctx, rl.ID)
	}

	r.eng.InvalidateTenant(ctx, t.ExternalID)

	return nil
}

// info converts a warden role. Malformed grants read as none.
func info(rl *role.Role) *RoleInfo {
	rules, err := RulesFromMetadata(rl.Slug, rl.Metadata)
	if err != nil {
		rules = acl.Rules{}
	}

	return &RoleInfo{
		Slug:       rl.Slug,
		Name:       rl.Name,
		ParentSlug: rl.ParentSlug,
		Rules:      acl.Rules{Publish: nonNil(rules.Publish), Subscribe: nonNil(rules.Subscribe)},
	}
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
