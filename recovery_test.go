package ago

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/zoobz-io/capitan"
)

func TestRecoverSagas_CompensatesRunningWithTimeout(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := NewMemoryStore()
	ctx := context.Background()

	reserveInventory := capitan.NewSignal("inventory.reserve", "Reserve inventory")
	releaseInventory := capitan.NewSignal("inventory.release", "Release inventory")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	// Track compensation signals
	var compCount int
	var mu sync.Mutex
	c.Hook(releaseInventory, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		compCount++
		mu.Unlock()
	})

	// Create saga step with timeout (not expired, but will be recovered anyway)
	step := NewSagaStep[Order]("reserve", store, orderKey, reserveInventory, releaseInventory).
		WithCapitan(c).
		WithTimeout(1 * time.Hour)

	flow := NewFlow(Order{ID: "order-1", Total: 100.0}, reserveInventory)
	flow.CorrelationID = "running-with-timeout-test"

	_, _ = step.Process(ctx, flow)

	// Run recovery - compensates ALL incomplete sagas (simulating restart)
	_ = RecoverSagas[Order](ctx, store, orderKey, c)

	c.Shutdown()

	mu.Lock()
	defer mu.Unlock()
	// Recovery compensates all incomplete sagas, regardless of timeout
	if compCount != 1 {
		t.Errorf("expected 1 compensation, got %d", compCount)
	}

	// Saga should be failed (compensated)
	state, _ := store.GetSaga(ctx, "running-with-timeout-test")
	if state.Status != SagaStatusFailed {
		t.Errorf("expected status Failed, got %s", state.Status)
	}

	// Error should NOT be "saga timeout exceeded" since it wasn't expired
	if state.Error == errSagaTimeoutExceeded {
		t.Error("non-expired saga should not have timeout error")
	}
}

func TestRecoverSagas_CompensatesExpired(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := NewMemoryStore()
	ctx := context.Background()

	releaseInventory := capitan.NewSignal("inventory.release", "Release inventory")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	// Track compensation signals
	var compCount int
	var mu sync.Mutex
	c.Hook(releaseInventory, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		compCount++
		mu.Unlock()
	})

	// Manually create an expired saga (created in the past with short timeout)
	expiredState := &SagaState{
		CorrelationID: "expired-saga-test",
		Status:        SagaStatusRunning,
		CurrentStep:   1,
		Compensations: []CompensationRecord{
			{
				StepName: "reserve",
				Signal:   releaseInventory,
				Data:     []byte(`{"ID":"order-1","Total":100}`),
			},
		},
		CreatedAt: time.Now().Add(-10 * time.Minute), // Created 10 mins ago
		UpdatedAt: time.Now().Add(-10 * time.Minute),
		Timeout:   1 * time.Minute, // 1 min timeout = expired
	}
	_ = store.SetSaga(ctx, "expired-saga-test", expiredState)

	// Run recovery - should compensate expired saga
	_ = RecoverSagas[Order](ctx, store, orderKey, c)

	c.Shutdown()

	mu.Lock()
	defer mu.Unlock()
	if compCount != 1 {
		t.Errorf("expected 1 compensation (expired saga), got %d", compCount)
	}

	// Saga should be failed now
	state, _ := store.GetSaga(ctx, "expired-saga-test")
	if state.Status != SagaStatusFailed {
		t.Errorf("expected status Failed, got %s", state.Status)
	}
	if state.Error != errSagaTimeoutExceeded {
		t.Errorf("expected error %q, got %q", errSagaTimeoutExceeded, state.Error)
	}
}

func TestRecoverSagas_NoIncomplete(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := NewMemoryStore()
	ctx := context.Background()

	orderKey := capitan.NewKey[Order]("order", "test.Order")

	// No sagas to recover
	err := RecoverSagas[Order](ctx, store, orderKey, c)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
}

func TestRecoverSagas_NilCapitan(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	releaseInventory := capitan.NewSignal("inventory.release", "Release inventory")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	// Create an incomplete saga
	_ = store.SetSaga(ctx, "nil-capitan-test", &SagaState{
		CorrelationID: "nil-capitan-test",
		Status:        SagaStatusRunning,
		CurrentStep:   1,
		Compensations: []CompensationRecord{
			{
				StepName: "reserve",
				Signal:   releaseInventory,
				Data:     []byte(`{"ID":"order-1","Total":100}`),
			},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	})

	// Should work with nil capitan (uses global)
	err := RecoverSagas[Order](ctx, store, orderKey, nil)
	if err != nil {
		t.Fatalf("recovery with nil capitan failed: %v", err)
	}

	// Saga should be failed
	state, _ := store.GetSaga(ctx, "nil-capitan-test")
	if state.Status != SagaStatusFailed {
		t.Errorf("expected status Failed, got %s", state.Status)
	}
}

func TestRecoverSagas_MultipleCompensations(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := NewMemoryStore()
	ctx := context.Background()

	step1Comp := capitan.NewSignal("step1.comp", "Step 1 rollback")
	step2Comp := capitan.NewSignal("step2.comp", "Step 2 rollback")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

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

	// Create saga with multiple compensations
	_ = store.SetSaga(ctx, "multi-comp-recovery", &SagaState{
		CorrelationID: "multi-comp-recovery",
		Status:        SagaStatusRunning,
		CurrentStep:   2,
		Compensations: []CompensationRecord{
			{
				StepName: "step1",
				Signal:   step1Comp,
				Data:     []byte(`{"ID":"order-1","Total":100}`),
			},
			{
				StepName: "step2",
				Signal:   step2Comp,
				Data:     []byte(`{"ID":"order-1","Total":100}`),
			},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	})

	_ = RecoverSagas[Order](ctx, store, orderKey, c)

	c.Shutdown()

	// Verify reverse order: step2, step1
	mu.Lock()
	defer mu.Unlock()
	if len(compOrder) != 2 {
		t.Fatalf("expected 2 compensations, got %d", len(compOrder))
	}
	if compOrder[0] != "step2" || compOrder[1] != "step1" {
		t.Errorf("expected [step2, step1], got %v", compOrder)
	}
}
