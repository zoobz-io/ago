package ago

// Identity represents the caller of a tool invocation.
// Parallel to rocco.Identity — tools need caller identity for
// authorization, audit logging, and scoping.
type Identity interface {
	ID() string
	TenantID() string
	Email() string
	Scopes() []string
	Roles() []string
	HasScope(scope string) bool
	HasRole(role string) bool
	Stats() map[string]int
}

// NoIdentity is the zero-value identity for unauthenticated tool calls.
type NoIdentity struct{}

// ID returns an empty string.
func (NoIdentity) ID() string { return "" }

// TenantID returns an empty string.
func (NoIdentity) TenantID() string { return "" }

// Email returns an empty string.
func (NoIdentity) Email() string { return "" }

// Scopes returns nil.
func (NoIdentity) Scopes() []string { return nil }

// Roles returns nil.
func (NoIdentity) Roles() []string { return nil }

// HasScope returns false.
func (NoIdentity) HasScope(string) bool { return false }

// HasRole returns false.
func (NoIdentity) HasRole(string) bool { return false }

// Stats returns nil.
func (NoIdentity) Stats() map[string]int { return nil }
