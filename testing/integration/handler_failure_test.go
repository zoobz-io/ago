package integration

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/zoobzio/ago"
	agotesting "github.com/zoobzio/ago/testing"
	"github.com/zoobzio/capitan"
)

// TestHandlerFailure_ExecuteHandlerPanics tests behavior when an execute signal handler panics.
// Note: capitan's behavior on panic depends on configuration.
func TestHandlerFailure_ExecuteHandlerPanics(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	ctx := context.Background()

	execSignal := capitan.NewSignal("exec.panic", "Execute that panics")
	compSignal := capitan.NewSignal("comp.panic", "Compensate")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	var panicRecovered bool
	var mu sync.Mutex

	// Handler that panics
	c.Hook(execSignal, func(_ context.Context, _ *capitan.Event) {
		panic("handler panic!")
	})

	step := ago.NewSagaStep[Order]("step", store, orderKey, execSignal, compSignal).
		WithCapitan(c)

	flow := ago.NewFlow(Order{ID: "panic-order"}, execSignal)
	flow.CorrelationID = "handler-panic-test"

	// Wrap in recover to catch if panic propagates
	func() {
		defer func() {
			if r := recover(); r != nil {
				mu.Lock()
				panicRecovered = true
				mu.Unlock()
			}
		}()
		_, _ = step.Process(ctx, flow)
	}()

	c.Shutdown()

	mu.Lock()
	recovered := panicRecovered
	mu.Unlock()

	// Document behavior - does capitan propagate panics or swallow them?
	if recovered {
		t.Log("BEHAVIOR: Handler panic propagated through capitan and was recovered")
	} else {
		t.Log("BEHAVIOR: Handler panic was swallowed by capitan (sync mode may differ)")
	}

	// Saga state should still exist (step executed before handler ran)
	state, _ := store.GetSaga(ctx, "handler-panic-test")
	if state != nil {
		t.Logf("Saga state exists with %d compensations", len(state.Compensations))
	}
}

// TestHandlerFailure_CompensationHandlerFails tests when a compensation handler fails.
// ago emits compensation signals but doesn't verify handler success.
func TestHandlerFailure_CompensationHandlerFails(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	ctx := context.Background()

	execSignal := capitan.NewSignal("exec", "Execute")
	compSignal := capitan.NewSignal("comp.fail", "Compensation that fails")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	var compAttempts int64
	c.Hook(compSignal, func(_ context.Context, _ *capitan.Event) {
		atomic.AddInt64(&compAttempts, 1)
		// Handler "fails" internally but capitan.Emit is fire-and-forget
		// There's no mechanism to report handler failure back to ago
	})

	step := ago.NewSagaStep[Order]("step", store, orderKey, execSignal, compSignal).
		WithCapitan(c)
	compensate := ago.NewCompensate[Order]("rollback", store, orderKey).WithCapitan(c)

	flow := ago.NewFlow(Order{ID: "comp-fail-order"}, execSignal)
	flow.CorrelationID = "handler-comp-fail-test"

	flow, _ = step.Process(ctx, flow)
	_, err := compensate.Process(ctx, flow)

	c.Shutdown()

	// Compensation "succeeded" from ago's perspective - it emitted the signal
	if err != nil {
		t.Errorf("compensation should not report error (fire-and-forget): %v", err)
	}

	// Handler was called
	if atomic.LoadInt64(&compAttempts) != 1 {
		t.Errorf("expected 1 compensation attempt, got %d", compAttempts)
	}

	// Saga is marked as Failed (compensation complete)
	state, _ := store.GetSaga(ctx, "handler-comp-fail-test")
	if state.Status != ago.SagaStatusFailed {
		t.Errorf("expected Failed status, got %v", state.Status)
	}

	t.Log("DESIGN NOTE: ago has no way to know if compensation handler actually succeeded")
	t.Log("Compensation is fire-and-forget - downstream handlers must be idempotent")
}

// TestHandlerFailure_PartialCompensationHandlerFailure tests multi-step saga where
// some compensation handlers fail.
func TestHandlerFailure_PartialCompensationHandlerFailure(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	ctx := context.Background()

	step1Exec := capitan.NewSignal("step1.exec", "Step 1")
	step1Comp := capitan.NewSignal("step1.comp", "Step 1 rollback")
	step2Exec := capitan.NewSignal("step2.exec", "Step 2")
	step2Comp := capitan.NewSignal("step2.comp", "Step 2 rollback (fails)")
	step3Exec := capitan.NewSignal("step3.exec", "Step 3")
	step3Comp := capitan.NewSignal("step3.comp", "Step 3 rollback")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	var compOrder []string
	var mu sync.Mutex

	c.Hook(step1Comp, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		compOrder = append(compOrder, "step1")
		mu.Unlock()
	})
	c.Hook(step2Comp, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		compOrder = append(compOrder, "step2-failed")
		mu.Unlock()
		// This handler "fails" but there's no way to report it
	})
	c.Hook(step3Comp, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		compOrder = append(compOrder, "step3")
		mu.Unlock()
	})

	s1 := ago.NewSagaStep[Order]("step1", store, orderKey, step1Exec, step1Comp).WithCapitan(c)
	s2 := ago.NewSagaStep[Order]("step2", store, orderKey, step2Exec, step2Comp).WithCapitan(c)
	s3 := ago.NewSagaStep[Order]("step3", store, orderKey, step3Exec, step3Comp).WithCapitan(c)
	compensate := ago.NewCompensate[Order]("rollback", store, orderKey).WithCapitan(c)

	flow := ago.NewFlow(Order{ID: "partial-comp-fail"}, step1Exec)
	flow.CorrelationID = "partial-comp-fail-test"

	flow, _ = s1.Process(ctx, flow)
	flow, _ = s2.Process(ctx, flow)
	flow, _ = s3.Process(ctx, flow)
	_, _ = compensate.Process(ctx, flow)

	c.Shutdown()

	mu.Lock()
	order := compOrder
	mu.Unlock()

	// All compensations were attempted in reverse order, regardless of "failure"
	if len(order) != 3 {
		t.Errorf("expected 3 compensation attempts, got %d: %v", len(order), order)
	}
	if order[0] != "step3" || order[1] != "step2-failed" || order[2] != "step1" {
		t.Errorf("expected [step3, step2-failed, step1], got %v", order)
	}

	t.Log("DESIGN NOTE: All compensation signals are emitted regardless of handler success")
	t.Log("There is no rollback-of-rollback mechanism")
}

// TestHandlerFailure_SlowHandler tests behavior when handler is slower than expected.
func TestHandlerFailure_SlowHandler(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	ctx := context.Background()

	execSignal := capitan.NewSignal("exec.slow", "Slow execute")
	compSignal := capitan.NewSignal("comp.slow", "Compensate")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	var handlerStarted, handlerFinished int64

	c.Hook(execSignal, func(_ context.Context, _ *capitan.Event) {
		atomic.AddInt64(&handlerStarted, 1)
		// Simulate slow handler - but in sync mode this blocks
		// In async mode the signal would be "sent" and handler runs separately
		atomic.AddInt64(&handlerFinished, 1)
	})

	step := ago.NewSagaStep[Order]("step", store, orderKey, execSignal, compSignal).
		WithCapitan(c)

	flow := ago.NewFlow(Order{ID: "slow-order"}, execSignal)
	flow.CorrelationID = "slow-handler-test"

	_, err := step.Process(ctx, flow)
	if err != nil {
		t.Fatalf("step failed: %v", err)
	}

	c.Shutdown()

	// In sync mode, handler completes before Process returns
	if atomic.LoadInt64(&handlerStarted) != 1 {
		t.Error("handler should have started")
	}
	if atomic.LoadInt64(&handlerFinished) != 1 {
		t.Error("handler should have finished (sync mode)")
	}

	t.Log("NOTE: Sync mode means handler completes before Process returns")
	t.Log("In production (async), Process returns immediately after Emit")
}

// TestHandlerFailure_HandlerErrorViaChannel tests a pattern for handlers to report errors.
// This shows how users might implement error reporting since ago doesn't provide it.
func TestHandlerFailure_HandlerErrorViaChannel(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	ctx := context.Background()

	execSignal := capitan.NewSignal("exec", "Execute")
	compSignal := capitan.NewSignal("comp", "Compensate")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	// User-implemented error channel pattern
	errChan := make(chan error, 1)

	c.Hook(execSignal, func(_ context.Context, e *capitan.Event) {
		// Handler can report errors via side channel
		idempotencyKey, _ := ago.IdempotencyKey.From(e)
		if idempotencyKey == "" {
			errChan <- errors.New("missing idempotency key")
			return
		}
		// Simulate work that might fail
		errChan <- nil // Success
	})

	step := ago.NewSagaStep[Order]("step", store, orderKey, execSignal, compSignal).
		WithCapitan(c)

	flow := ago.NewFlow(Order{ID: "error-channel"}, execSignal)
	flow.CorrelationID = "error-channel-test"

	_, err := step.Process(ctx, flow)
	if err != nil {
		t.Fatalf("step failed: %v", err)
	}

	// Check handler result via side channel
	select {
	case handlerErr := <-errChan:
		if handlerErr != nil {
			t.Errorf("handler reported error: %v", handlerErr)
		}
	default:
		t.Error("handler did not report result")
	}

	c.Shutdown()

	t.Log("PATTERN: Users can implement error channels for handler feedback")
	t.Log("This is outside ago's scope but demonstrates integration pattern")
}

// TestHandlerFailure_IdempotencyKeyUsage tests that handlers receive and can use IdempotencyKey.
func TestHandlerFailure_IdempotencyKeyUsage(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	ctx := context.Background()

	execSignal := capitan.NewSignal("exec", "Execute")
	compSignal := capitan.NewSignal("comp", "Compensate")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	var receivedKeys []string
	var mu sync.Mutex

	c.Hook(execSignal, func(_ context.Context, e *capitan.Event) {
		key, ok := ago.IdempotencyKey.From(e)
		mu.Lock()
		if ok {
			receivedKeys = append(receivedKeys, key)
		}
		mu.Unlock()
	})

	step := ago.NewSagaStep[Order]("step", store, orderKey, execSignal, compSignal).
		WithCapitan(c)

	// Execute same step multiple times
	for i := 0; i < 3; i++ {
		flow := ago.NewFlow(Order{ID: "idem-key-test"}, execSignal)
		flow.CorrelationID = "idem-key-usage-test"
		_, _ = step.Process(ctx, flow)
	}

	c.Shutdown()

	mu.Lock()
	keys := receivedKeys
	mu.Unlock()

	// Due to idempotency, handler should only be called once
	if len(keys) != 1 {
		t.Errorf("expected 1 handler call due to idempotency, got %d", len(keys))
	}

	// Verify key format
	if len(keys) > 0 {
		expectedKey := "idem-key-usage-test:step"
		if keys[0] != expectedKey {
			t.Errorf("expected key %q, got %q", expectedKey, keys[0])
		}
	}

	t.Log("IdempotencyKey format: {correlationID}:{stepName}")
	t.Log("Handlers should use this key for external system deduplication")
}
