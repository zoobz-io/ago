package ago

import (
	"context"
	"testing"

	"github.com/zoobz-io/capitan"
	"github.com/zoobz-io/pipz"
)

type TestPayload struct {
	ID string `json:"id"`
}

func TestCorrelate(t *testing.T) {
	signal := capitan.NewSignal("test", "Test")
	flow := NewFlow(TestPayload{ID: "1"}, signal)

	correlate := Correlate[TestPayload](pipz.NewIdentity("correlate", ""))

	result, err := correlate.Process(context.Background(), flow)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.CorrelationID == "" {
		t.Error("expected CorrelationID to be generated")
	}
}

func TestCorrelate_ExistingID(t *testing.T) {
	signal := capitan.NewSignal("test", "Test")
	flow := NewFlow(TestPayload{ID: "1"}, signal)
	flow.CorrelationID = "existing-id"

	correlate := Correlate[TestPayload](pipz.NewIdentity("correlate", ""))

	result, err := correlate.Process(context.Background(), flow)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.CorrelationID != "existing-id" {
		t.Errorf("expected 'existing-id', got %q", result.CorrelationID)
	}
}

func TestCorrelateFrom(t *testing.T) {
	signal := capitan.NewSignal("test", "Test")
	flow := NewFlow(TestPayload{ID: "1"}, signal)

	correlate := CorrelateFrom[TestPayload](pipz.NewIdentity("correlate", ""), "parent-corr")

	result, err := correlate.Process(context.Background(), flow)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.CorrelationID == "" {
		t.Error("expected CorrelationID to be generated")
	}
	if result.CausationID != "parent-corr" {
		t.Errorf("expected CausationID 'parent-corr', got %q", result.CausationID)
	}
}
