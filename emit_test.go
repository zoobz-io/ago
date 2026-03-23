package ago

import (
	"context"
	"sync"
	"testing"

	"github.com/zoobz-io/capitan"
	"github.com/zoobz-io/pipz"
)

func TestEmit_Basic(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()
	orderSignal := capitan.NewSignal("order.created", "Order created")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	var emittedSignal string
	var capturedOrder Order
	var mu sync.Mutex

	c.Hook(orderSignal, func(_ context.Context, e *capitan.Event) {
		mu.Lock()
		emittedSignal = e.Signal().Name()
		order, ok := orderKey.From(e)
		if ok {
			capturedOrder = order
		}
		mu.Unlock()
	})

	emit := NewEmit[Order](pipz.NewIdentity("emit-order", ""), orderSignal, orderKey).WithCapitan(c)

	flow := NewFlow(Order{ID: "order-123", Total: 99.99}, orderSignal)
	_, err := emit.Process(ctx, flow)

	if err != nil {
		t.Fatalf("Emit failed: %v", err)
	}

	c.Shutdown()

	mu.Lock()
	defer mu.Unlock()

	if emittedSignal != "order.created" {
		t.Errorf("expected signal 'order.created', got %q", emittedSignal)
	}
	if capturedOrder.ID != "order-123" {
		t.Errorf("expected order ID 'order-123', got %q", capturedOrder.ID)
	}
	if capturedOrder.Total != 99.99 {
		t.Errorf("expected order Total 99.99, got %v", capturedOrder.Total)
	}
}

func TestEmit_WithCorrelationAndCausation(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()
	orderSignal := capitan.NewSignal("order.created", "Order created")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	var capturedCorrID, capturedCauseID string
	var mu sync.Mutex

	c.Hook(orderSignal, func(_ context.Context, e *capitan.Event) {
		mu.Lock()
		capturedCorrID, _ = CorrelationKey.From(e)
		capturedCauseID, _ = CausationKey.From(e)
		mu.Unlock()
	})

	emit := NewEmit[Order](pipz.NewIdentity("emit-order", ""), orderSignal, orderKey).WithCapitan(c)

	flow := NewFlow(Order{ID: "order-123"}, orderSignal)
	flow.CorrelationID = "corr-456"
	flow.CausationID = "cause-789"

	_, err := emit.Process(ctx, flow)
	if err != nil {
		t.Fatalf("Emit failed: %v", err)
	}

	c.Shutdown()

	mu.Lock()
	defer mu.Unlock()

	if capturedCorrID != "corr-456" {
		t.Errorf("expected correlation_id 'corr-456', got %q", capturedCorrID)
	}
	if capturedCauseID != "cause-789" {
		t.Errorf("expected causation_id 'cause-789', got %q", capturedCauseID)
	}
}

func TestEmit_WithFlowFields(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()
	orderSignal := capitan.NewSignal("order.created", "Order created")
	orderKey := capitan.NewKey[Order]("order", "test.Order")
	statusKey := capitan.NewStringKey("status")

	var capturedStatus string
	var mu sync.Mutex

	c.Hook(orderSignal, func(_ context.Context, e *capitan.Event) {
		mu.Lock()
		capturedStatus, _ = statusKey.From(e)
		mu.Unlock()
	})

	emit := NewEmit[Order](pipz.NewIdentity("emit-order", ""), orderSignal, orderKey).WithCapitan(c)

	flow := NewFlow(Order{ID: "order-123"}, orderSignal)
	flow.Set(statusKey.Field("pending"))

	_, err := emit.Process(ctx, flow)
	if err != nil {
		t.Fatalf("Emit failed: %v", err)
	}

	c.Shutdown()

	mu.Lock()
	defer mu.Unlock()

	if capturedStatus != "pending" {
		t.Errorf("expected status 'pending', got %q", capturedStatus)
	}
}

func TestEmit_Name(t *testing.T) {
	orderSignal := capitan.NewSignal("order.created", "Order created")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	emit := NewEmit[Order](pipz.NewIdentity("my-emit", ""), orderSignal, orderKey)

	if emit.Identity().Name() != "my-emit" {
		t.Errorf("expected name 'my-emit', got %q", emit.Identity().Name())
	}
}

func TestEmit_Close(t *testing.T) {
	orderSignal := capitan.NewSignal("order.created", "Order created")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	emit := NewEmit[Order](pipz.NewIdentity("my-emit", ""), orderSignal, orderKey)

	if err := emit.Close(); err != nil {
		t.Errorf("expected nil error from Close, got %v", err)
	}
}

func TestEmit_GlobalCapitan(t *testing.T) {
	// Test that emit works with nil capitan (uses global)
	ctx := context.Background()
	orderSignal := capitan.NewSignal("order.global", "Order global")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	emit := NewEmit[Order](pipz.NewIdentity("emit-order", ""), orderSignal, orderKey)
	// No WithCapitan call - uses global

	flow := NewFlow(Order{ID: "order-123"}, orderSignal)

	// Should not panic
	_, err := emit.Process(ctx, flow)
	if err != nil {
		t.Fatalf("Emit failed: %v", err)
	}
}

func TestEmit_Build(t *testing.T) {
	orderSignal := capitan.NewSignal("order.created", "Order created")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	emit := NewEmit[Order](pipz.NewIdentity("my-emit", ""), orderSignal, orderKey)
	built := emit.Build()

	if built == nil {
		t.Error("expected Build to return non-nil Chainable")
	}
}
