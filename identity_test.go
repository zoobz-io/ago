package ago_test

import (
	"testing"

	"github.com/zoobz-io/ago"
)

func TestNoIdentity(t *testing.T) {
	var id ago.Identity = ago.NoIdentity{}

	if id.ID() != "" {
		t.Errorf("expected empty ID, got %q", id.ID())
	}
	if id.TenantID() != "" {
		t.Errorf("expected empty TenantID, got %q", id.TenantID())
	}
	if id.Email() != "" {
		t.Errorf("expected empty Email, got %q", id.Email())
	}
	if id.Scopes() != nil {
		t.Errorf("expected nil Scopes, got %v", id.Scopes())
	}
	if id.Roles() != nil {
		t.Errorf("expected nil Roles, got %v", id.Roles())
	}
	if id.HasScope("admin") {
		t.Error("expected HasScope to return false")
	}
	if id.HasRole("admin") {
		t.Error("expected HasRole to return false")
	}
	if id.Stats() != nil {
		t.Errorf("expected nil Stats, got %v", id.Stats())
	}
}
