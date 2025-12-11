package ago

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/zoobzio/capitan"
)

func TestSagaStep_ExecuteAndCompensate(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := NewMemoryStore()
	ctx := context.Background()

	// Define signals
	reserveInventory := capitan.NewSignal("inventory.reserve", "Reserve inventory")
	releaseInventory := capitan.NewSignal("inventory.release", "Release inventory")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	// Track emitted signals
	var emitted []string
	var mu sync.Mutex
	c.Observe(func(_ context.Context, e *capitan.Event) {
		mu.Lock()
		emitted = append(emitted, e.Signal().Name())
		mu.Unlock()
	})

	// Create saga step
	step := NewSagaStep[Order]("reserve", store, orderKey, reserveInventory, releaseInventory).
		WithCapitan(c)

	// Create flow with correlation
	flow := NewFlow(Order{ID: "order-1", Total: 100.0}, reserveInventory)
	flow.CorrelationID = "saga-123"

	// Execute step
	result, err := step.Process(ctx, flow)
	if err != nil {
		t.Fatalf("saga step failed: %v", err)
	}

	c.Shutdown()

	// Verify saga state was created
	state, err := store.GetSaga(ctx, "saga-123")
	if err != nil {
		t.Fatalf("failed to get saga state: %v", err)
	}
	if state.Status != SagaStatusRunning {
		t.Errorf("expected Running, got %v", state.Status)
	}
	if len(state.Compensations) != 1 {
		t.Errorf("expected 1 compensation, got %d", len(state.Compensations))
	}
	if state.Compensations[0].StepName != "reserve" {
		t.Errorf("expected step name 'reserve', got %q", state.Compensations[0].StepName)
	}

	// Verify signals were emitted
	mu.Lock()
	defer mu.Unlock()
	found := false
	for _, sig := range emitted {
		if sig == "inventory.reserve" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected inventory.reserve signal to be emitted")
	}

	_ = result // silence unused
}

func TestSagaStep_Idempotency(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := NewMemoryStore()
	ctx := context.Background()

	reserveInventory := capitan.NewSignal("inventory.reserve", "Reserve inventory")
	releaseInventory := capitan.NewSignal("inventory.release", "Release inventory")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	// Count how many times the signal is emitted
	var emitCount int
	var mu sync.Mutex
	c.Hook(reserveInventory, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		emitCount++
		mu.Unlock()
	})

	step := NewSagaStep[Order]("reserve", store, orderKey, reserveInventory, releaseInventory).
		WithCapitan(c)

	flow := NewFlow(Order{ID: "order-1", Total: 100.0}, reserveInventory)
	flow.CorrelationID = "idem-123"

	// Execute step first time
	_, err := step.Process(ctx, flow)
	if err != nil {
		t.Fatalf("first execution failed: %v", err)
	}

	// Execute step second time (simulating retry/duplicate)
	_, err = step.Process(ctx, flow)
	if err != nil {
		t.Fatalf("second execution failed: %v", err)
	}

	// Execute step third time
	_, err = step.Process(ctx, flow)
	if err != nil {
		t.Fatalf("third execution failed: %v", err)
	}

	c.Shutdown()

	// Verify signal was emitted only once
	mu.Lock()
	defer mu.Unlock()
	if emitCount != 1 {
		t.Errorf("expected signal emitted once, got %d", emitCount)
	}

	// Verify only one compensation registered
	state, _ := store.GetSaga(ctx, "idem-123")
	if len(state.Compensations) != 1 {
		t.Errorf("expected 1 compensation, got %d", len(state.Compensations))
	}
}

func TestSagaStep_IdempotencyKey(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := NewMemoryStore()
	ctx := context.Background()

	reserveInventory := capitan.NewSignal("inventory.reserve", "Reserve inventory")
	releaseInventory := capitan.NewSignal("inventory.release", "Release inventory")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	// Capture the idempotency key from the emitted event
	var capturedKey string
	var mu sync.Mutex
	c.Hook(reserveInventory, func(_ context.Context, e *capitan.Event) {
		mu.Lock()
		capturedKey, _ = IdempotencyKey.From(e)
		mu.Unlock()
	})

	step := NewSagaStep[Order]("reserve", store, orderKey, reserveInventory, releaseInventory).
		WithCapitan(c)

	flow := NewFlow(Order{ID: "order-1", Total: 100.0}, reserveInventory)
	flow.CorrelationID = "key-test-456"

	_, _ = step.Process(ctx, flow)
	c.Shutdown()

	// Verify idempotency key format
	mu.Lock()
	defer mu.Unlock()
	expected := "key-test-456:reserve"
	if capturedKey != expected {
		t.Errorf("expected idempotency key %q, got %q", expected, capturedKey)
	}
}

func TestSagaStep_WithTimeout(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := NewMemoryStore()
	ctx := context.Background()

	reserveInventory := capitan.NewSignal("inventory.reserve", "Reserve inventory")
	releaseInventory := capitan.NewSignal("inventory.release", "Release inventory")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	// Create saga step with timeout
	step := NewSagaStep[Order]("reserve", store, orderKey, reserveInventory, releaseInventory).
		WithCapitan(c).
		WithTimeout(5 * time.Minute)

	flow := NewFlow(Order{ID: "order-1", Total: 100.0}, reserveInventory)
	flow.CorrelationID = "timeout-test"

	_, err := step.Process(ctx, flow)
	if err != nil {
		t.Fatalf("saga step failed: %v", err)
	}

	c.Shutdown()

	// Verify timeout was stored
	state, err := store.GetSaga(ctx, "timeout-test")
	if err != nil {
		t.Fatalf("get saga failed: %v", err)
	}

	if state.Timeout != 5*time.Minute {
		t.Errorf("expected timeout 5m, got %v", state.Timeout)
	}
}

func TestSagaStep_Name(t *testing.T) {
	store := NewMemoryStore()
	orderKey := capitan.NewKey[Order]("order", "test.Order")
	execSignal := capitan.NewSignal("exec", "Execute")
	compSignal := capitan.NewSignal("comp", "Compensate")

	step := NewSagaStep[Order]("my-step", store, orderKey, execSignal, compSignal)

	if step.Name() != "my-step" {
		t.Errorf("expected name 'my-step', got %q", step.Name())
	}
}

func TestSagaStep_Close(t *testing.T) {
	store := NewMemoryStore()
	orderKey := capitan.NewKey[Order]("order", "test.Order")
	execSignal := capitan.NewSignal("exec", "Execute")
	compSignal := capitan.NewSignal("comp", "Compensate")

	step := NewSagaStep[Order]("my-step", store, orderKey, execSignal, compSignal)

	if err := step.Close(); err != nil {
		t.Errorf("expected nil error from Close, got %v", err)
	}
}

func TestSagaStep_SkipsWhenCompensating(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := NewMemoryStore()
	ctx := context.Background()

	execSignal := capitan.NewSignal("exec", "Execute")
	compSignal := capitan.NewSignal("comp", "Compensate")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	// Pre-create saga in compensating state
	_ = store.SetSaga(ctx, "compensating-saga", &SagaState{
		CorrelationID: "compensating-saga",
		Status:        SagaStatusCompensating,
		CurrentStep:   1,
		Compensations: []CompensationRecord{},
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	})

	var emitCount int
	c.Hook(execSignal, func(_ context.Context, _ *capitan.Event) {
		emitCount++
	})

	step := NewSagaStep[Order]("step", store, orderKey, execSignal, compSignal).WithCapitan(c)

	flow := NewFlow(Order{ID: "order"}, execSignal)
	flow.CorrelationID = "compensating-saga"

	_, err := step.Process(ctx, flow)
	if err != nil {
		t.Fatalf("step failed: %v", err)
	}

	c.Shutdown()

	// Should not emit when saga is compensating
	if emitCount != 0 {
		t.Errorf("expected 0 emissions for compensating saga, got %d", emitCount)
	}
}
