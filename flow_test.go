package ago

import (
	"context"
	"testing"

	"github.com/zoobz-io/capitan"
)

type Order struct {
	ID    string  `json:"id"`
	Total float64 `json:"total"`
}

func TestNewFlow(t *testing.T) {
	signal := capitan.NewSignal("order.created", "Order created")
	order := Order{ID: "123", Total: 99.99}

	flow := NewFlow(order, signal)

	if flow.Payload.ID != "123" {
		t.Errorf("expected ID '123', got %q", flow.Payload.ID)
	}
	if flow.Payload.Total != 99.99 {
		t.Errorf("expected Total 99.99, got %f", flow.Payload.Total)
	}
	if flow.Signal.Name() != "order.created" {
		t.Errorf("expected signal 'order.created', got %q", flow.Signal.Name())
	}
	if flow.Severity != capitan.SeverityInfo {
		t.Errorf("expected SeverityInfo, got %v", flow.Severity)
	}
	if flow.Timestamp.IsZero() {
		t.Error("expected non-zero timestamp")
	}
}

func TestNewFromEvent(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	signal := capitan.NewSignal("order.created", "Order created")
	orderKey := capitan.NewKey[Order]("order", "test.Order")
	order := Order{ID: "456", Total: 50.0}

	var flow *Flow[Order]
	c.Hook(signal, func(_ context.Context, e *capitan.Event) {
		flow = NewFromEvent(e, orderKey)
	})

	ctx := context.Background()
	c.Emit(ctx, signal, orderKey.Field(order))
	c.Shutdown()

	if flow == nil {
		t.Fatal("expected flow to be created from event")
	}
	if flow.Payload.ID != "456" {
		t.Errorf("expected ID '456', got %q", flow.Payload.ID)
	}
}

func TestFlow_SetGet(t *testing.T) {
	signal := capitan.NewSignal("test", "Test")
	flow := NewFlow(Order{ID: "1"}, signal)

	customerKey := capitan.NewStringKey("customer_name")
	flow.Set(customerKey.Field("John Doe"))

	name, ok := From(flow, customerKey)
	if !ok {
		t.Fatal("expected field to exist")
	}
	if name != "John Doe" {
		t.Errorf("expected 'John Doe', got %q", name)
	}
}

func TestFlow_Fields(t *testing.T) {
	signal := capitan.NewSignal("test", "Test")
	flow := NewFlow(Order{ID: "1"}, signal)

	flow.Set(capitan.NewStringKey("field1").Field("value1"))
	flow.Set(capitan.NewIntKey("field2").Field(42))

	fields := flow.Fields()
	if len(fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(fields))
	}
}

func TestFlow_Errors(t *testing.T) {
	signal := capitan.NewSignal("test", "Test")
	flow := NewFlow(Order{ID: "1"}, signal)

	if flow.HasErrors() {
		t.Error("expected no errors initially")
	}

	flow.AddError(ErrNotFound)
	flow.AddError(ErrNotFound) // Use same error type for both

	if !flow.HasErrors() {
		t.Error("expected errors after adding")
	}
	if len(flow.Errors) != 2 {
		t.Errorf("expected 2 errors, got %d", len(flow.Errors))
	}
}

func TestFlow_Clone(t *testing.T) {
	signal := capitan.NewSignal("test", "Test")
	flow := NewFlow(Order{ID: "1", Total: 100.0}, signal)
	flow.CorrelationID = "corr-123"
	flow.Metadata["key"] = "value"
	flow.Set(capitan.NewStringKey("extra").Field("data"))
	flow.AddError(ErrNotFound)

	clone := flow.Clone()

	// Verify clone has same values
	if clone.Payload.ID != "1" {
		t.Errorf("clone payload mismatch")
	}
	if clone.CorrelationID != "corr-123" {
		t.Errorf("clone correlation mismatch")
	}
	if clone.Metadata["key"] != "value" {
		t.Errorf("clone metadata mismatch")
	}

	// Verify modifications don't affect original
	clone.Metadata["key"] = "modified"
	if flow.Metadata["key"] != "value" {
		t.Error("modifying clone affected original")
	}
}

func TestFlow_CorrelationFields(t *testing.T) {
	signal := capitan.NewSignal("test", "Test")
	flow := NewFlow(Order{ID: "1"}, signal)
	flow.CorrelationID = "corr-abc"
	flow.CausationID = "cause-xyz"

	if flow.CorrelationID != "corr-abc" {
		t.Errorf("correlation mismatch")
	}
	if flow.CausationID != "cause-xyz" {
		t.Errorf("causation mismatch")
	}
}
