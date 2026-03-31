package ago_test

import (
	"context"
	"testing"

	"github.com/zoobz-io/ago"
)

func TestTypedInput(t *testing.T) {
	type Input struct {
		Name string `json:"name"`
	}

	inv := &ago.Invocation{
		Context:  context.Background(),
		Input:    Input{Name: "test"},
		Metadata: make(map[string]any),
	}

	input, ok := ago.TypedInput[Input](inv)
	if !ok {
		t.Fatal("TypedInput should succeed")
	}
	if input.Name != "test" {
		t.Errorf("expected name 'test', got %q", input.Name)
	}
}

func TestTypedInputWrongType(t *testing.T) {
	inv := &ago.Invocation{
		Context:  context.Background(),
		Input:    "not a struct",
		Metadata: make(map[string]any),
	}

	_, ok := ago.TypedInput[struct{}](inv)
	if ok {
		t.Error("TypedInput should fail on wrong type")
	}
}

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
