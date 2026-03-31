package ago_test

import (
	"context"
	"testing"

	"github.com/zoobz-io/ago"
)

func TestInvocationMetadata(t *testing.T) {
	inv := &ago.Invocation{
		Context: context.Background(),
	}

	// SetMeta should initialize the map if nil.
	inv.SetMeta("key", "value")

	v, ok := inv.GetMeta("key")
	if !ok {
		t.Fatal("GetMeta should find the key")
	}
	if v != "value" {
		t.Errorf("expected 'value', got %v", v)
	}

	_, ok = inv.GetMeta("missing")
	if ok {
		t.Error("GetMeta should return false for missing key")
	}
}

func TestInvocationGetMetaNilMap(t *testing.T) {
	inv := &ago.Invocation{
		Context: context.Background(),
	}

	_, ok := inv.GetMeta("anything")
	if ok {
		t.Error("GetMeta should return false on nil map")
	}
}
