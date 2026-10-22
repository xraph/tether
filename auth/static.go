package auth

import (
	"context"
	"fmt"
)

type static struct {
	identities map[string]*Identity
}

// NewStatic returns an Authenticator that looks identities up by username and
// ignores the password. It is for tests and examples, never for production.
// Callers get a copy, so they cannot change the map's identities.
func NewStatic(identities map[string]*Identity) Authenticator {
	cp := make(map[string]*Identity, len(identities))
	for u, ident := range identities {
		if ident != nil {
			cp[u] = copyIdentity(ident)
		}
	}

	return &static{identities: cp}
}

// Authenticate implements Authenticator.
func (s *static) Authenticate(_ context.Context, c Credentials) (*Identity, error) {
	ident, ok := s.identities[c.Username]
	if !ok {
		return nil, fmt.Errorf("%w: unknown username", ErrBadCredentials)
	}

	return copyIdentity(ident), nil
}
