package ago

import (
	"context"
	"testing"

	"github.com/zoobz-io/capitan"
	"github.com/zoobz-io/pipz"
)

func TestTag(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")

	tag := Tag[Order](pipz.NewIdentity("add-version", ""), "version", "1.0.0")

	flow := NewFlow(Order{ID: "order-1"}, signal)
	result, err := tag.Process(ctx, flow)

	if err != nil {
		t.Fatalf("Tag failed: %v", err)
	}
	if result.Metadata["version"] != "1.0.0" {
		t.Errorf("expected version '1.0.0', got %q", result.Metadata["version"])
	}
}

func TestTag_InitializesMetadata(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")

	tag := Tag[Order](pipz.NewIdentity("add-tag", ""), "key", "value")

	flow := NewFlow(Order{ID: "order-1"}, signal)
	flow.Metadata = nil // Ensure metadata is nil

	result, err := tag.Process(ctx, flow)

	if err != nil {
		t.Fatalf("Tag failed: %v", err)
	}
	if result.Metadata == nil {
		t.Error("expected Metadata to be initialized")
	}
	if result.Metadata["key"] != "value" {
		t.Errorf("expected key 'value', got %q", result.Metadata["key"])
	}
}

func TestTag_OverwritesExisting(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")

	tag := Tag[Order](pipz.NewIdentity("overwrite", ""), "key", "new-value")

	flow := NewFlow(Order{ID: "order-1"}, signal)
	flow.Metadata = map[string]string{"key": "old-value"}

	result, err := tag.Process(ctx, flow)

	if err != nil {
		t.Fatalf("Tag failed: %v", err)
	}
	if result.Metadata["key"] != "new-value" {
		t.Errorf("expected key 'new-value', got %q", result.Metadata["key"])
	}
}

func TestTagFrom(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")

	tagFrom := TagFrom[Order](pipz.NewIdentity("add-order-id", ""), "order_id", func(o Order) string {
		return o.ID
	})

	flow := NewFlow(Order{ID: "order-123", Total: 100.0}, signal)
	result, err := tagFrom.Process(ctx, flow)

	if err != nil {
		t.Fatalf("TagFrom failed: %v", err)
	}
	if result.Metadata["order_id"] != "order-123" {
		t.Errorf("expected order_id 'order-123', got %q", result.Metadata["order_id"])
	}
}

func TestTagFrom_InitializesMetadata(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")

	tagFrom := TagFrom[Order](pipz.NewIdentity("add-id", ""), "id", func(o Order) string {
		return o.ID
	})

	flow := NewFlow(Order{ID: "order-456"}, signal)
	flow.Metadata = nil

	result, err := tagFrom.Process(ctx, flow)

	if err != nil {
		t.Fatalf("TagFrom failed: %v", err)
	}
	if result.Metadata == nil {
		t.Error("expected Metadata to be initialized")
	}
	if result.Metadata["id"] != "order-456" {
		t.Errorf("expected id 'order-456', got %q", result.Metadata["id"])
	}
}

func TestTagFrom_ComplexExtraction(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")

	tagFrom := TagFrom[Order](pipz.NewIdentity("add-total-category", ""), "category", func(o Order) string {
		if o.Total > 100.0 {
			return "high-value"
		}
		return "standard"
	})

	// High value order
	flow := NewFlow(Order{ID: "order-1", Total: 500.0}, signal)
	result, _ := tagFrom.Process(ctx, flow)
	if result.Metadata["category"] != "high-value" {
		t.Errorf("expected category 'high-value', got %q", result.Metadata["category"])
	}

	// Standard order
	flow = NewFlow(Order{ID: "order-2", Total: 50.0}, signal)
	result, _ = tagFrom.Process(ctx, flow)
	if result.Metadata["category"] != "standard" {
		t.Errorf("expected category 'standard', got %q", result.Metadata["category"])
	}
}
