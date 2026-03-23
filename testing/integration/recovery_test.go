package integration

import (
	"context"
	"sync"
	"testing"

	"github.com/zoobz-io/ago"
	agotesting "github.com/zoobz-io/ago/testing"
	"github.com/zoobz-io/capitan"
	"github.com/zoobz-io/pipz"
)

// TestRecoverSagas_IncompleteSagas tests recovery of incomplete sagas after restart.
func TestRecoverSagas_IncompleteSagas(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	tracker := agotesting.NewSignalTracker()
	ctx := context.Background()

	// Define signals
	execSignal := capitan.NewSignal("step.exec", "Execute")
	compSignal := capitan.NewSignal("step.comp", "Compensate")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	agotesting.HookTracker(c, tracker, compSignal, ago.SagaCompensating, ago.SagaCompleted)

	// Create saga step and execute (simulating pre-crash state)
	step := ago.NewSagaStep[Order](pipz.NewIdentity("step", ""), store, orderKey, execSignal, compSignal).
		WithCapitan(c)

	flow := ago.NewFlow(Order{ID: "recover-order"}, execSignal)
	flow.CorrelationID = "recovery-test-1"

	_, err := step.Process(ctx, flow)
	if err != nil {
		t.Fatalf("step failed: %v", err)
	}

	// Verify saga is incomplete (running)
	state, _ := store.GetSaga(ctx, "recovery-test-1")
	if state.Status != ago.SagaStatusRunning {
		t.Fatalf("expected Running status, got %v", state.Status)
	}

	// Simulate restart - call RecoverSagas
	err = ago.RecoverSagas[Order](ctx, store, orderKey, c)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	c.Shutdown()

	// Verify compensation was executed
	agotesting.AssertSignalCount(t, tracker, "step.comp", 1)

	// Verify saga is now failed (compensation complete)
	state, _ = store.GetSaga(ctx, "recovery-test-1")
	if state.Status != ago.SagaStatusFailed {
		t.Errorf("expected Failed status, got %v", state.Status)
	}
}

// TestRecoverSagas_MultipleSagas tests recovery of multiple incomplete sagas.
func TestRecoverSagas_MultipleSagas(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	tracker := agotesting.NewSignalTracker()
	ctx := context.Background()

	execSignal := capitan.NewSignal("multi.exec", "Execute")
	compSignal := capitan.NewSignal("multi.comp", "Compensate")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	agotesting.HookTracker(c, tracker, compSignal)

	step := ago.NewSagaStep[Order](pipz.NewIdentity("step", ""), store, orderKey, execSignal, compSignal).
		WithCapitan(c)

	// Create multiple incomplete sagas
	numSagas := 5
	for i := 0; i < numSagas; i++ {
		flow := ago.NewFlow(Order{ID: "multi-order"}, execSignal)
		flow.CorrelationID = "multi-recovery-" + string(rune('0'+i))
		_, _ = step.Process(ctx, flow)
	}

	// Verify all are incomplete
	incomplete, _ := store.ListIncompleteSagas(ctx)
	if len(incomplete) != numSagas {
		t.Fatalf("expected %d incomplete sagas, got %d", numSagas, len(incomplete))
	}

	// Recover all
	err := ago.RecoverSagas[Order](ctx, store, orderKey, c)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	c.Shutdown()

	// Verify all compensations executed
	agotesting.AssertSignalCount(t, tracker, "multi.comp", numSagas)

	// Verify all are now failed
	incomplete, _ = store.ListIncompleteSagas(ctx)
	if len(incomplete) != 0 {
		t.Errorf("expected 0 incomplete sagas, got %d", len(incomplete))
	}
}

// TestRecoverSagas_MultiStepCompensation tests recovery with multi-step sagas.
func TestRecoverSagas_MultiStepCompensation(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	ctx := context.Background()

	// 3-step saga
	step1Exec := capitan.NewSignal("step1.exec", "Step 1")
	step1Comp := capitan.NewSignal("step1.comp", "Step 1 rollback")
	step2Exec := capitan.NewSignal("step2.exec", "Step 2")
	step2Comp := capitan.NewSignal("step2.comp", "Step 2 rollback")
	step3Exec := capitan.NewSignal("step3.exec", "Step 3")
	step3Comp := capitan.NewSignal("step3.comp", "Step 3 rollback")

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
	c.Hook(step3Comp, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		compOrder = append(compOrder, "step3")
		mu.Unlock()
	})

	s1 := ago.NewSagaStep[Order](pipz.NewIdentity("step1", ""), store, orderKey, step1Exec, step1Comp).WithCapitan(c)
	s2 := ago.NewSagaStep[Order](pipz.NewIdentity("step2", ""), store, orderKey, step2Exec, step2Comp).WithCapitan(c)
	s3 := ago.NewSagaStep[Order](pipz.NewIdentity("step3", ""), store, orderKey, step3Exec, step3Comp).WithCapitan(c)

	flow := ago.NewFlow(Order{ID: "multi-step"}, step1Exec)
	flow.CorrelationID = "multi-step-recovery"

	// Execute all 3 steps
	flow, _ = s1.Process(ctx, flow)
	flow, _ = s2.Process(ctx, flow)
	_, _ = s3.Process(ctx, flow)

	// Recover (should compensate in reverse order)
	err := ago.RecoverSagas[Order](ctx, store, orderKey, c)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	c.Shutdown()

	// Verify compensation order: step3, step2, step1
	mu.Lock()
	defer mu.Unlock()

	if len(compOrder) != 3 {
		t.Fatalf("expected 3 compensations, got %d", len(compOrder))
	}
	if compOrder[0] != "step3" || compOrder[1] != "step2" || compOrder[2] != "step1" {
		t.Errorf("expected [step3, step2, step1], got %v", compOrder)
	}
}

// TestRecoverSagas_NoIncompleteSagas tests recovery when there are no incomplete sagas.
func TestRecoverSagas_NoIncompleteSagas(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	ctx := context.Background()

	orderKey := capitan.NewKey[Order]("order", "test.Order")

	// No sagas to recover
	err := ago.RecoverSagas[Order](ctx, store, orderKey, c)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	c.Shutdown()

	// Should complete without error
}

// TestRecoverSagas_CompletedSagasIgnored tests that completed sagas are not recovered.
func TestRecoverSagas_CompletedSagasIgnored(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	tracker := agotesting.NewSignalTracker()
	ctx := context.Background()

	execSignal := capitan.NewSignal("completed.exec", "Execute")
	compSignal := capitan.NewSignal("completed.comp", "Compensate")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	agotesting.HookTracker(c, tracker, compSignal)

	step := ago.NewSagaStep[Order](pipz.NewIdentity("step", ""), store, orderKey, execSignal, compSignal).
		WithCapitan(c)
	compensate := ago.NewCompensate[Order](pipz.NewIdentity("rollback", ""), store, orderKey).WithCapitan(c)

	flow := ago.NewFlow(Order{ID: "completed-order"}, execSignal)
	flow.CorrelationID = "completed-saga"

	// Execute and compensate (saga should be marked as failed/completed)
	flow, _ = step.Process(ctx, flow)
	_, _ = compensate.Process(ctx, flow)

	// Reset tracker
	tracker.Reset()

	// Now run recovery
	err := ago.RecoverSagas[Order](ctx, store, orderKey, c)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	c.Shutdown()

	// No compensations should have been triggered
	if tracker.Count() > 0 {
		t.Errorf("expected no signals from recovery, got %d", tracker.Count())
	}
}

// TestRecoverSagas_PartiallyCompensated tests recovery of saga that crashed mid-compensation.
func TestRecoverSagas_PartiallyCompensated(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	ctx := context.Background()

	step1Exec := capitan.NewSignal("partial.step1.exec", "Step 1")
	step1Comp := capitan.NewSignal("partial.step1.comp", "Step 1 rollback")
	step2Exec := capitan.NewSignal("partial.step2.exec", "Step 2")
	step2Comp := capitan.NewSignal("partial.step2.comp", "Step 2 rollback")

	orderKey := capitan.NewKey[Order]("order", "test.Order")

	var compCount int
	var mu sync.Mutex
	c.Hook(step1Comp, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		compCount++
		mu.Unlock()
	})
	c.Hook(step2Comp, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		compCount++
		mu.Unlock()
	})

	s1 := ago.NewSagaStep[Order](pipz.NewIdentity("step1", ""), store, orderKey, step1Exec, step1Comp).WithCapitan(c)
	s2 := ago.NewSagaStep[Order](pipz.NewIdentity("step2", ""), store, orderKey, step2Exec, step2Comp).WithCapitan(c)

	flow := ago.NewFlow(Order{ID: "partial-comp"}, step1Exec)
	flow.CorrelationID = "partial-comp-saga"

	// Execute both steps
	flow, _ = s1.Process(ctx, flow)
	_, _ = s2.Process(ctx, flow)

	// Manually set saga to "compensating" status (simulating crash mid-compensation)
	state, _ := store.GetSaga(ctx, "partial-comp-saga")
	state.Status = ago.SagaStatusCompensating
	_ = store.UpdateSaga(ctx, "partial-comp-saga", state)

	// Run recovery
	err := ago.RecoverSagas[Order](ctx, store, orderKey, c)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	c.Shutdown()

	// Both compensations should be executed (recovery re-runs all)
	mu.Lock()
	count := compCount
	mu.Unlock()

	if count != 2 {
		t.Errorf("expected 2 compensations, got %d", count)
	}

	// Saga should now be failed
	state, _ = store.GetSaga(ctx, "partial-comp-saga")
	if state.Status != ago.SagaStatusFailed {
		t.Errorf("expected Failed status, got %v", state.Status)
	}
}
