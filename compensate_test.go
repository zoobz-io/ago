package ago

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/zoobzio/capitan"
)

func TestCompensate_Basic(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := NewMemoryStore()
	ctx := context.Background()

	// Define signals
	reserveInventory := capitan.NewSignal("inventory.reserve", "Reserve inventory")
	releaseInventory := capitan.NewSignal("inventory.release", "Release inventory")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	// Track compensation signals
	var compensationEmitted bool
	var mu sync.Mutex
	c.Hook(releaseInventory, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		compensationEmitted = true
		mu.Unlock()
	})

	// Create and execute saga step
	step := NewSagaStep[Order]("reserve", store, orderKey, reserveInventory, releaseInventory).
		WithCapitan(c)

	flow := NewFlow(Order{ID: "order-1", Total: 100.0}, reserveInventory)
	flow.CorrelationID = "saga-456"

	_, _ = step.Process(ctx, flow)

	// Now run compensation
	compensate := NewCompensate[Order]("compensate", store, orderKey).WithCapitan(c)
	_, err := compensate.Process(ctx, flow)
	if err != nil {
		t.Fatalf("compensation failed: %v", err)
	}

	c.Shutdown()

	// Verify compensation was emitted
	mu.Lock()
	defer mu.Unlock()
	if !compensationEmitted {
		t.Error("expected compensation signal to be emitted")
	}

	// Verify saga state was updated
	state, _ := store.GetSaga(ctx, "saga-456")
	if state.Status != SagaStatusFailed {
		t.Errorf("expected Failed status after compensation, got %v", state.Status)
	}
}

func TestCompensate_MultiStep(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := NewMemoryStore()
	ctx := context.Background()

	// Define signals for 3-step saga
	step1Execute := capitan.NewSignal("step1.execute", "Step 1")
	step1Comp := capitan.NewSignal("step1.compensate", "Step 1 rollback")
	step2Execute := capitan.NewSignal("step2.execute", "Step 2")
	step2Comp := capitan.NewSignal("step2.compensate", "Step 2 rollback")
	step3Execute := capitan.NewSignal("step3.execute", "Step 3")
	step3Comp := capitan.NewSignal("step3.compensate", "Step 3 rollback")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	// Create steps
	s1 := NewSagaStep[Order]("step1", store, orderKey, step1Execute, step1Comp).WithCapitan(c)
	s2 := NewSagaStep[Order]("step2", store, orderKey, step2Execute, step2Comp).WithCapitan(c)
	s3 := NewSagaStep[Order]("step3", store, orderKey, step3Execute, step3Comp).WithCapitan(c)

	flow := NewFlow(Order{ID: "multi-1"}, step1Execute)
	flow.CorrelationID = "multi-saga"

	// Execute all steps
	flow, _ = s1.Process(ctx, flow)
	flow, _ = s2.Process(ctx, flow)
	_, _ = s3.Process(ctx, flow)

	// Verify 3 compensations registered
	state, _ := store.GetSaga(ctx, "multi-saga")
	if len(state.Compensations) != 3 {
		t.Errorf("expected 3 compensations, got %d", len(state.Compensations))
	}

	// Track compensation order
	var compOrder []string
	var mu sync.Mutex
	c.Hook(step1Comp, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		compOrder = append(compOrder, "step1")
		mu.Unlock()
	})
	c.Hook(step2Comp, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		compOrder = append(compOrder, "step2")
		mu.Unlock()
	})
	c.Hook(step3Comp, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		compOrder = append(compOrder, "step3")
		mu.Unlock()
	})

	// Run compensation
	compensate := NewCompensate[Order]("compensate", store, orderKey).WithCapitan(c)
	_, _ = compensate.Process(ctx, flow)

	c.Shutdown()

	// Verify reverse order: step3, step2, step1
	mu.Lock()
	defer mu.Unlock()
	if len(compOrder) != 3 {
		t.Fatalf("expected 3 compensations, got %d", len(compOrder))
	}
	if compOrder[0] != "step3" || compOrder[1] != "step2" || compOrder[2] != "step1" {
		t.Errorf("expected [step3, step2, step1], got %v", compOrder)
	}
}

func TestCompensate_Idempotency(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := NewMemoryStore()
	ctx := context.Background()

	reserveInventory := capitan.NewSignal("inventory.reserve", "Reserve inventory")
	releaseInventory := capitan.NewSignal("inventory.release", "Release inventory")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	// Count compensation signal emissions
	var compCount int
	var mu sync.Mutex
	c.Hook(releaseInventory, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		compCount++
		mu.Unlock()
	})

	// Create and execute saga step
	step := NewSagaStep[Order]("reserve", store, orderKey, reserveInventory, releaseInventory).
		WithCapitan(c)

	flow := NewFlow(Order{ID: "order-1", Total: 100.0}, reserveInventory)
	flow.CorrelationID = "comp-idem-789"

	_, _ = step.Process(ctx, flow)

	// Run compensation multiple times (simulating retries)
	compensate := NewCompensate[Order]("compensate", store, orderKey).WithCapitan(c)
	_, _ = compensate.Process(ctx, flow)
	_, _ = compensate.Process(ctx, flow)
	_, _ = compensate.Process(ctx, flow)

	c.Shutdown()

	// Verify compensation signal emitted only once
	mu.Lock()
	defer mu.Unlock()
	if compCount != 1 {
		t.Errorf("expected compensation emitted once, got %d", compCount)
	}
}

func TestCompensate_IdempotencyKey(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := NewMemoryStore()
	ctx := context.Background()

	reserveInventory := capitan.NewSignal("inventory.reserve", "Reserve inventory")
	releaseInventory := capitan.NewSignal("inventory.release", "Release inventory")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	// Capture idempotency key from compensation signal
	var capturedKey string
	var mu sync.Mutex
	c.Hook(releaseInventory, func(_ context.Context, e *capitan.Event) {
		mu.Lock()
		capturedKey, _ = IdempotencyKey.From(e)
		mu.Unlock()
	})

	step := NewSagaStep[Order]("reserve", store, orderKey, reserveInventory, releaseInventory).
		WithCapitan(c)

	flow := NewFlow(Order{ID: "order-1", Total: 100.0}, reserveInventory)
	flow.CorrelationID = "comp-key-test"

	_, _ = step.Process(ctx, flow)

	compensate := NewCompensate[Order]("compensate", store, orderKey).WithCapitan(c)
	_, _ = compensate.Process(ctx, flow)

	c.Shutdown()

	// Verify idempotency key format includes "compensate:" prefix
	mu.Lock()
	defer mu.Unlock()
	expected := "comp-key-test:compensate:reserve"
	if capturedKey != expected {
		t.Errorf("expected idempotency key %q, got %q", expected, capturedKey)
	}
}

func TestCompensate_Name(t *testing.T) {
	store := NewMemoryStore()
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	compensate := NewCompensate[Order]("my-compensate", store, orderKey)

	if compensate.Name() != "my-compensate" {
		t.Errorf("expected name 'my-compensate', got %q", compensate.Name())
	}
}

func TestCompensate_Close(t *testing.T) {
	store := NewMemoryStore()
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	compensate := NewCompensate[Order]("my-compensate", store, orderKey)

	if err := compensate.Close(); err != nil {
		t.Errorf("expected nil error from Close, got %v", err)
	}
}

func TestCompensate_NotFound(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := NewMemoryStore()
	ctx := context.Background()

	orderKey := capitan.NewKey[Order]("order", "test.Order")

	compensate := NewCompensate[Order]("compensate", store, orderKey).WithCapitan(c)

	flow := NewFlow(Order{ID: "order-1"}, capitan.NewSignal("test", "Test"))
	flow.CorrelationID = "nonexistent-saga"

	_, err := compensate.Process(ctx, flow)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}
