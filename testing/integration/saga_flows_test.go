package integration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/zoobz-io/ago"
	agotesting "github.com/zoobz-io/ago/testing"
	"github.com/zoobz-io/capitan"
	"github.com/zoobz-io/pipz"
)

// Order represents a test order for saga tests.
type Order struct {
	ID       string
	Customer string
	Amount   float64
	Status   string
}

func (o Order) Clone() Order {
	return o
}

// TestSagaFlow_SuccessfulExecution tests a complete saga that succeeds.
func TestSagaFlow_SuccessfulExecution(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	tracker := agotesting.NewSignalTracker()
	ctx := context.Background()

	// Define signals for 3-step order saga
	reserveInventory := capitan.NewSignal("inventory.reserve", "Reserve inventory")
	releaseInventory := capitan.NewSignal("inventory.release", "Release inventory")
	chargePayment := capitan.NewSignal("payment.charge", "Charge payment")
	refundPayment := capitan.NewSignal("payment.refund", "Refund payment")
	shipOrder := capitan.NewSignal("shipping.ship", "Ship order")
	cancelShipment := capitan.NewSignal("shipping.cancel", "Cancel shipment")

	orderKey := capitan.NewKey[Order]("order", "test.Order")

	// Track all signals
	agotesting.HookTracker(c, tracker,
		reserveInventory, releaseInventory,
		chargePayment, refundPayment,
		shipOrder, cancelShipment,
		ago.SagaStepCompleted,
	)

	// Build saga steps
	step1 := ago.NewSagaStep[Order](pipz.NewIdentity("reserve-inventory", ""), store, orderKey, reserveInventory, releaseInventory).
		WithCapitan(c)
	step2 := ago.NewSagaStep[Order](pipz.NewIdentity("charge-payment", ""), store, orderKey, chargePayment, refundPayment).
		WithCapitan(c)
	step3 := ago.NewSagaStep[Order](pipz.NewIdentity("ship-order", ""), store, orderKey, shipOrder, cancelShipment).
		WithCapitan(c)

	// Compose pipeline
	pipeline := pipz.NewSequence[*ago.Flow[Order]](pipz.NewIdentity("order-saga", ""),
		step1, step2, step3,
	)

	// Create and process flow
	order := Order{ID: "order-123", Customer: "alice", Amount: 99.99}
	flow := ago.NewFlow(order, reserveInventory)
	flow.CorrelationID = "saga-success-test"

	result, err := pipeline.Process(ctx, flow)
	if err != nil {
		t.Fatalf("saga failed: %v", err)
	}

	c.Shutdown()

	// Verify all steps executed
	if result.Payload.ID != "order-123" {
		t.Errorf("expected order ID 'order-123', got %q", result.Payload.ID)
	}

	// Verify signal order
	agotesting.AssertSignalOrder(t, tracker,
		"inventory.reserve", "ago.saga.step.completed",
		"payment.charge", "ago.saga.step.completed",
		"shipping.ship", "ago.saga.step.completed",
	)

	// Verify saga state
	state, err := store.GetSaga(ctx, "saga-success-test")
	if err != nil {
		t.Fatalf("failed to get saga state: %v", err)
	}
	if len(state.Compensations) != 3 {
		t.Errorf("expected 3 compensations registered, got %d", len(state.Compensations))
	}
}

// TestSagaFlow_FailureAndCompensation tests saga failure with rollback.
func TestSagaFlow_FailureAndCompensation(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	tracker := agotesting.NewSignalTracker()
	ctx := context.Background()

	// Define signals
	step1Execute := capitan.NewSignal("step1.execute", "Step 1")
	step1Comp := capitan.NewSignal("step1.compensate", "Step 1 rollback")
	step2Execute := capitan.NewSignal("step2.execute", "Step 2")
	step2Comp := capitan.NewSignal("step2.compensate", "Step 2 rollback")

	orderKey := capitan.NewKey[Order]("order", "test.Order")

	agotesting.HookTracker(c, tracker,
		step1Execute, step1Comp,
		step2Execute, step2Comp,
		ago.SagaCompensating, ago.SagaCompleted,
	)

	// Build steps
	step1 := ago.NewSagaStep[Order](pipz.NewIdentity("step1", ""), store, orderKey, step1Execute, step1Comp).
		WithCapitan(c)
	step2 := ago.NewSagaStep[Order](pipz.NewIdentity("step2", ""), store, orderKey, step2Execute, step2Comp).
		WithCapitan(c)

	// Failing step
	failingStep := pipz.Apply[*ago.Flow[Order]](pipz.NewIdentity("fail", ""), func(_ context.Context, f *ago.Flow[Order]) (*ago.Flow[Order], error) {
		return f, errors.New("simulated failure")
	})

	compensate := ago.NewCompensate[Order](pipz.NewIdentity("rollback", ""), store, orderKey).WithCapitan(c)

	// Process with failure handling
	flow := ago.NewFlow(Order{ID: "fail-order"}, step1Execute)
	flow.CorrelationID = "saga-fail-test"

	// Execute steps
	flow, _ = step1.Process(ctx, flow)
	flow, _ = step2.Process(ctx, flow)

	// Simulate failure
	_, err := failingStep.Process(ctx, flow)
	if err == nil {
		t.Fatal("expected failure")
	}

	// Run compensation
	_, err = compensate.Process(ctx, flow)
	if err != nil {
		t.Fatalf("compensation failed: %v", err)
	}

	c.Shutdown()

	// Verify compensation order (reverse of execution)
	names := tracker.SignalNames()

	// Find compensation signals
	var compSignals []string
	for _, name := range names {
		if name == "step1.compensate" || name == "step2.compensate" {
			compSignals = append(compSignals, name)
		}
	}

	if len(compSignals) != 2 {
		t.Fatalf("expected 2 compensation signals, got %d: %v", len(compSignals), compSignals)
	}
	if compSignals[0] != "step2.compensate" || compSignals[1] != "step1.compensate" {
		t.Errorf("expected [step2.compensate, step1.compensate], got %v", compSignals)
	}

	// Verify saga state
	state, _ := store.GetSaga(ctx, "saga-fail-test")
	if state.Status != ago.SagaStatusFailed {
		t.Errorf("expected status Failed, got %v", state.Status)
	}
}

// TestSagaFlow_IdempotentRetry tests that retrying a saga step is idempotent.
func TestSagaFlow_IdempotentRetry(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	ctx := context.Background()

	executeSignal := capitan.NewSignal("step.execute", "Execute")
	compSignal := capitan.NewSignal("step.compensate", "Compensate")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	// Count executions
	var executeCount int64
	var mu sync.Mutex
	c.Hook(executeSignal, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		executeCount++
		mu.Unlock()
	})

	step := ago.NewSagaStep[Order](pipz.NewIdentity("step", ""), store, orderKey, executeSignal, compSignal).
		WithCapitan(c)

	flow := ago.NewFlow(Order{ID: "idem-order"}, executeSignal)
	flow.CorrelationID = "saga-idempotent-test"

	// Execute multiple times (simulating retries)
	for i := 0; i < 5; i++ {
		_, err := step.Process(ctx, flow)
		if err != nil {
			t.Fatalf("execution %d failed: %v", i, err)
		}
	}

	c.Shutdown()

	// Verify only one execution
	mu.Lock()
	count := executeCount
	mu.Unlock()

	if count != 1 {
		t.Errorf("expected 1 execution, got %d", count)
	}

	// Verify only one compensation registered
	state, _ := store.GetSaga(ctx, "saga-idempotent-test")
	if len(state.Compensations) != 1 {
		t.Errorf("expected 1 compensation, got %d", len(state.Compensations))
	}
}

// TestSagaFlow_ConcurrentSagas tests multiple concurrent saga executions.
func TestSagaFlow_ConcurrentSagas(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	ctx := context.Background()

	executeSignal := capitan.NewSignal("concurrent.execute", "Execute")
	compSignal := capitan.NewSignal("concurrent.compensate", "Compensate")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	step := ago.NewSagaStep[Order](pipz.NewIdentity("step", ""), store, orderKey, executeSignal, compSignal).
		WithCapitan(c)

	numSagas := 10
	var wg sync.WaitGroup
	errors := make(chan error, numSagas)

	for i := 0; i < numSagas; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			flow := ago.NewFlow(Order{ID: "order-" + string(rune('0'+idx))}, executeSignal)
			flow.CorrelationID = "concurrent-saga-" + string(rune('0'+idx))
			_, err := step.Process(ctx, flow)
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	c.Shutdown()

	// Check for errors
	for err := range errors {
		t.Errorf("saga error: %v", err)
	}

	// Verify all sagas created
	sagas, _ := store.ListIncompleteSagas(ctx)
	if len(sagas) != numSagas {
		t.Errorf("expected %d sagas, got %d", numSagas, len(sagas))
	}
}

// TestSagaFlow_PartialCompensation tests compensation from failure point.
func TestSagaFlow_PartialCompensation(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	tracker := agotesting.NewSignalTracker()
	ctx := context.Background()

	// 3 steps, but step 3 will fail before executing
	step1Exec := capitan.NewSignal("step1.exec", "Step 1")
	step1Comp := capitan.NewSignal("step1.comp", "Step 1 rollback")
	step2Exec := capitan.NewSignal("step2.exec", "Step 2")
	step2Comp := capitan.NewSignal("step2.comp", "Step 2 rollback")

	orderKey := capitan.NewKey[Order]("order", "test.Order")

	agotesting.HookTracker(c, tracker,
		step1Exec, step1Comp,
		step2Exec, step2Comp,
	)

	step1 := ago.NewSagaStep[Order](pipz.NewIdentity("step1", ""), store, orderKey, step1Exec, step1Comp).
		WithCapitan(c)
	step2 := ago.NewSagaStep[Order](pipz.NewIdentity("step2", ""), store, orderKey, step2Exec, step2Comp).
		WithCapitan(c)
	compensate := ago.NewCompensate[Order](pipz.NewIdentity("rollback", ""), store, orderKey).WithCapitan(c)

	flow := ago.NewFlow(Order{ID: "partial-order"}, step1Exec)
	flow.CorrelationID = "saga-partial-test"

	// Execute only steps 1 and 2
	flow, _ = step1.Process(ctx, flow)
	flow, _ = step2.Process(ctx, flow)

	// Verify 2 compensations registered
	state, _ := store.GetSaga(ctx, "saga-partial-test")
	if len(state.Compensations) != 2 {
		t.Fatalf("expected 2 compensations, got %d", len(state.Compensations))
	}

	// Run compensation
	_, _ = compensate.Process(ctx, flow)

	c.Shutdown()

	// Verify only 2 compensation signals (not 3)
	agotesting.AssertSignalCount(t, tracker, "step1.comp", 1)
	agotesting.AssertSignalCount(t, tracker, "step2.comp", 1)
}

// TestSagaFlow_CompensationIdempotency tests that compensation retries are idempotent.
func TestSagaFlow_CompensationIdempotency(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	ctx := context.Background()

	execSignal := capitan.NewSignal("exec", "Execute")
	compSignal := capitan.NewSignal("comp", "Compensate")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	var compCount int64
	var mu sync.Mutex
	c.Hook(compSignal, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		compCount++
		mu.Unlock()
	})

	step := ago.NewSagaStep[Order](pipz.NewIdentity("step", ""), store, orderKey, execSignal, compSignal).
		WithCapitan(c)
	compensate := ago.NewCompensate[Order](pipz.NewIdentity("rollback", ""), store, orderKey).WithCapitan(c)

	flow := ago.NewFlow(Order{ID: "comp-idem"}, execSignal)
	flow.CorrelationID = "saga-comp-idem-test"

	// Execute step
	flow, _ = step.Process(ctx, flow)

	// Run compensation multiple times
	for i := 0; i < 5; i++ {
		_, _ = compensate.Process(ctx, flow)
	}

	c.Shutdown()

	mu.Lock()
	count := compCount
	mu.Unlock()

	if count != 1 {
		t.Errorf("expected 1 compensation, got %d", count)
	}
}

// TestSagaFlow_WithTimeout tests saga step with timeout wrapper.
func TestSagaFlow_WithTimeout(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	ctx := context.Background()

	execSignal := capitan.NewSignal("timeout.exec", "Execute")
	compSignal := capitan.NewSignal("timeout.comp", "Compensate")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	// Slow step that exceeds timeout
	slowStep := pipz.Apply[*ago.Flow[Order]](pipz.NewIdentity("slow", ""), func(ctx context.Context, f *ago.Flow[Order]) (*ago.Flow[Order], error) {
		select {
		case <-time.After(200 * time.Millisecond):
			return f, nil
		case <-ctx.Done():
			return f, ctx.Err()
		}
	})

	step := ago.NewSagaStep[Order](pipz.NewIdentity("step", ""), store, orderKey, execSignal, compSignal).
		WithCapitan(c)

	// Wrap in timeout
	pipeline := pipz.NewSequence[*ago.Flow[Order]](pipz.NewIdentity("timed-saga", ""),
		step,
		pipz.NewTimeout(pipz.NewIdentity("timeout", ""), slowStep, 50*time.Millisecond),
	)

	flow := ago.NewFlow(Order{ID: "timeout-order"}, execSignal)
	flow.CorrelationID = "saga-timeout-test"

	_, err := pipeline.Process(ctx, flow)
	if err == nil {
		t.Fatal("expected timeout error")
	}

	c.Shutdown()

	// Saga step should have executed before timeout
	state, _ := store.GetSaga(ctx, "saga-timeout-test")
	if state == nil {
		t.Fatal("saga state should exist")
	}
}
