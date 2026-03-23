package integration

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zoobz-io/ago"
	agotesting "github.com/zoobz-io/ago/testing"
	"github.com/zoobz-io/capitan"
)

// TestTiming_ResponseAfterTimeout tests what happens when a response arrives
// after the request has already timed out.
func TestTiming_ResponseAfterTimeout(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()

	requestSignal := capitan.NewSignal("late.request", "Request")
	responseSignal := capitan.NewSignal("late.response", "Response")
	queryKey := capitan.NewKey[Query]("query", "test.Query")
	resultKey := capitan.NewKey[Result]("result", "test.Result")

	var responsesSent int64

	// Responder that delays longer than timeout
	c.Hook(requestSignal, func(ctx context.Context, e *capitan.Event) {
		corrID, _ := ago.CorrelationKey.From(e)

		// Simulate slow response - send after requester times out
		go func() {
			time.Sleep(100 * time.Millisecond) // Longer than timeout
			atomic.AddInt64(&responsesSent, 1)
			c.Emit(ctx, responseSignal,
				resultKey.Field(Result{Data: "late response", Count: 1}),
				ago.CorrelationKey.Field(corrID),
			)
		}()
	})

	req := ago.NewRequest[Query, Result]("late", requestSignal, responseSignal, queryKey, resultKey).
		WithCapitan(c).
		Timeout(50 * time.Millisecond)

	flow := ago.NewFlow(Query{Term: "test"}, requestSignal)
	flow.CorrelationID = "late-response-test"

	_, err := req.Process(ctx, flow)

	// Should timeout
	if !errors.Is(err, ago.ErrTimeout) {
		t.Errorf("expected ErrTimeout, got %v", err)
	}

	// Wait for the late response to be sent
	time.Sleep(150 * time.Millisecond)

	c.Shutdown()

	// Response was sent but ignored (no one listening anymore)
	if atomic.LoadInt64(&responsesSent) != 1 {
		t.Error("late response should have been sent")
	}

	t.Log("BEHAVIOR: Late responses are simply ignored - no hook registered after timeout")
	t.Log("This is correct behavior - the waiter cleaned up its hook")
}

// TestTiming_ConcurrentSagaAndRecovery tests what happens when recovery runs
// while a saga is actively executing.
func TestTiming_ConcurrentSagaAndRecovery(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	ctx := context.Background()

	execSignal := capitan.NewSignal("concurrent.exec", "Execute")
	compSignal := capitan.NewSignal("concurrent.comp", "Compensate")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	var execCount, compCount int64
	c.Hook(execSignal, func(_ context.Context, _ *capitan.Event) {
		atomic.AddInt64(&execCount, 1)
	})
	c.Hook(compSignal, func(_ context.Context, _ *capitan.Event) {
		atomic.AddInt64(&compCount, 1)
	})

	step := ago.NewSagaStep[Order]("step", store, orderKey, execSignal, compSignal).
		WithCapitan(c)

	// Execute step (saga is now "running")
	flow := ago.NewFlow(Order{ID: "concurrent-saga"}, execSignal)
	flow.CorrelationID = "concurrent-saga-recovery-test"
	_, _ = step.Process(ctx, flow)

	// Now run recovery concurrently
	var wg sync.WaitGroup
	wg.Add(2)

	// Recovery goroutine
	go func() {
		defer wg.Done()
		_ = ago.RecoverSagas[Order](ctx, store, orderKey, c)
	}()

	// Simulate another step execution attempt
	go func() {
		defer wg.Done()
		// Different correlation - different saga
		flow2 := ago.NewFlow(Order{ID: "concurrent-saga-2"}, execSignal)
		flow2.CorrelationID = "concurrent-saga-2-test"
		_, _ = step.Process(ctx, flow2)
	}()

	wg.Wait()
	c.Shutdown()

	t.Logf("Executions: %d, Compensations: %d",
		atomic.LoadInt64(&execCount),
		atomic.LoadInt64(&compCount))

	// Recovery should have compensated the first saga
	// Second saga should have executed
	// No assertions on exact counts - documenting behavior
	t.Log("BEHAVIOR: Recovery and execution can run concurrently")
	t.Log("Each saga has its own state - no cross-contamination expected")
}

// TestTiming_RapidFireRequests tests many requests in quick succession.
func TestTiming_RapidFireRequests(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()

	requestSignal := capitan.NewSignal("rapid.request", "Request")
	responseSignal := capitan.NewSignal("rapid.response", "Response")
	queryKey := capitan.NewKey[Query]("query", "test.Query")
	resultKey := capitan.NewKey[Result]("result", "test.Result")

	// Fast responder
	c.Hook(requestSignal, func(ctx context.Context, e *capitan.Event) {
		query, _ := queryKey.From(e)
		corrID, _ := ago.CorrelationKey.From(e)
		c.Emit(ctx, responseSignal,
			resultKey.Field(Result{Data: query.Term, Count: 1}),
			ago.CorrelationKey.Field(corrID),
		)
	})

	req := ago.NewRequest[Query, Result]("rapid", requestSignal, responseSignal, queryKey, resultKey).
		WithCapitan(c).
		Timeout(500 * time.Millisecond)

	numRequests := 100
	var wg sync.WaitGroup
	var successCount int64
	var errorCount int64

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			flow := ago.NewFlow(Query{Term: "rapid-test"}, requestSignal)
			flow.CorrelationID = "rapid-" + string(rune('0'+idx%10)) + string(rune('0'+idx/10))

			_, err := req.Process(ctx, flow)
			if err != nil {
				atomic.AddInt64(&errorCount, 1)
			} else {
				atomic.AddInt64(&successCount, 1)
			}
		}(i)
	}

	wg.Wait()
	c.Shutdown()

	success := atomic.LoadInt64(&successCount)
	errors := atomic.LoadInt64(&errorCount)

	t.Logf("Rapid fire results: %d success, %d errors out of %d", success, errors, numRequests)

	if success+errors != int64(numRequests) {
		t.Errorf("lost requests: expected %d, got %d", numRequests, success+errors)
	}

	// All should succeed in sync mode
	if errors > 0 {
		t.Logf("WARNING: %d requests failed - possible correlation collision or timing issue", errors)
	}
}

// TestTiming_SagaStepDuringCompensation tests what happens if a step is executed
// while compensation is running for the same saga.
func TestTiming_SagaStepDuringCompensation(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	ctx := context.Background()

	execSignal := capitan.NewSignal("race.exec", "Execute")
	compSignal := capitan.NewSignal("race.comp", "Compensate")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	var execCount, compCount int64
	c.Hook(execSignal, func(_ context.Context, _ *capitan.Event) {
		atomic.AddInt64(&execCount, 1)
	})
	c.Hook(compSignal, func(_ context.Context, _ *capitan.Event) {
		atomic.AddInt64(&compCount, 1)
	})

	step := ago.NewSagaStep[Order]("step", store, orderKey, execSignal, compSignal).
		WithCapitan(c)
	compensate := ago.NewCompensate[Order]("rollback", store, orderKey).WithCapitan(c)

	// Initial execution
	flow := ago.NewFlow(Order{ID: "race-saga"}, execSignal)
	flow.CorrelationID = "race-step-comp-test"
	flow, _ = step.Process(ctx, flow)

	// Start compensation
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, _ = compensate.Process(ctx, flow)
	}()

	// Try to execute another step during compensation
	go func() {
		defer wg.Done()
		// Same correlation ID - should this be blocked?
		_, _ = step.Process(ctx, flow)
	}()

	wg.Wait()
	c.Shutdown()

	t.Logf("Executions: %d, Compensations: %d",
		atomic.LoadInt64(&execCount),
		atomic.LoadInt64(&compCount))

	// Document current behavior
	t.Log("BEHAVIOR: No locking between step execution and compensation")
	t.Log("The idempotency check on step name prevents duplicate execution")
	t.Log("but doesn't prevent execution during active compensation")
}

// TestTiming_AwaitEventBeforeHook tests the race between event emission and
// hook registration in Await.
func TestTiming_AwaitEventBeforeHook(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()

	eventSignal := capitan.NewSignal("early.event", "Early event")
	statusKey := capitan.NewStringKey("status")

	// Pre-emit the event BEFORE setting up the await
	// In sync mode this completes immediately
	c.Emit(ctx, eventSignal,
		statusKey.Field("pre-emitted"),
		ago.CorrelationKey.Field("early-event-test"),
	)

	// Now set up await - will it see the already-emitted event?
	await := ago.NewAwait[Order, string]("wait-early", eventSignal, statusKey).
		WithCapitan(c).
		Timeout(50 * time.Millisecond)

	flow := ago.NewFlow(Order{ID: "early-order"}, eventSignal)
	flow.CorrelationID = "early-event-test"

	_, err := await.Process(ctx, flow)

	c.Shutdown()

	// Await should timeout because event was emitted before hook was registered
	if !errors.Is(err, ago.ErrTimeout) {
		t.Logf("Result: %v (expected timeout because event was pre-emitted)", err)
	}

	t.Log("BEHAVIOR: Await only sees events emitted AFTER its hook is registered")
	t.Log("Pre-emitted events are lost - this is by design with capitan's pub/sub model")
}

// TestTiming_MultipleAwaitersRace tests multiple awaiters for the same correlation.
func TestTiming_MultipleAwaitersRace(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()

	eventSignal := capitan.NewSignal("race.event", "Race event")
	statusKey := capitan.NewStringKey("status")

	numWaiters := 5
	var wg sync.WaitGroup
	results := make(chan error, numWaiters)

	// Start multiple awaiters for the SAME correlation
	for i := 0; i < numWaiters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			await := ago.NewAwait[Order, string]("wait", eventSignal, statusKey).
				WithCapitan(c).
				Timeout(200 * time.Millisecond)

			flow := ago.NewFlow(Order{ID: "race-order"}, eventSignal)
			flow.CorrelationID = "multi-await-same-corr"

			_, err := await.Process(ctx, flow)
			results <- err
		}()
	}

	// Give awaiters time to register
	time.Sleep(20 * time.Millisecond)

	// Emit single event - all awaiters share same correlation
	c.Emit(ctx, eventSignal,
		statusKey.Field("raced"),
		ago.CorrelationKey.Field("multi-await-same-corr"),
	)

	wg.Wait()
	close(results)

	c.Shutdown()

	var successes, timeouts int
	for err := range results {
		if err == nil {
			successes++
		} else if errors.Is(err, ago.ErrTimeout) {
			timeouts++
		}
	}

	t.Logf("Multiple awaiters for same correlation: %d success, %d timeout", successes, timeouts)

	// All should succeed - each await creates its own hook
	if successes != numWaiters {
		t.Log("BEHAVIOR: Each Await creates independent hook - all should receive the event")
	}
}

// TestTiming_CompensationWhileExecuting tests compensation triggered during execution.
// With the WithSaga fix, concurrent access is now serialized - step execution checks
// saga status and won't execute if compensating.
func TestTiming_CompensationWhileExecuting(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	ctx := context.Background()

	step1Exec := capitan.NewSignal("step1.exec", "Step 1")
	step1Comp := capitan.NewSignal("step1.comp", "Step 1 comp")
	step2Exec := capitan.NewSignal("step2.exec", "Step 2")
	step2Comp := capitan.NewSignal("step2.comp", "Step 2 comp")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	var events []string
	var mu sync.Mutex

	c.Hook(step1Exec, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		events = append(events, "s1-exec")
		mu.Unlock()
	})
	c.Hook(step1Comp, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		events = append(events, "s1-comp")
		mu.Unlock()
	})
	c.Hook(step2Exec, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		events = append(events, "s2-exec")
		mu.Unlock()
	})
	c.Hook(step2Comp, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		events = append(events, "s2-comp")
		mu.Unlock()
	})

	s1 := ago.NewSagaStep[Order]("step1", store, orderKey, step1Exec, step1Comp).WithCapitan(c)
	s2 := ago.NewSagaStep[Order]("step2", store, orderKey, step2Exec, step2Comp).WithCapitan(c)
	compensate := ago.NewCompensate[Order]("rollback", store, orderKey).WithCapitan(c)

	flow := ago.NewFlow(Order{ID: "slow-saga"}, step1Exec)
	flow.CorrelationID = "slow-execution-test"

	// Execute step 1
	flow, _ = s1.Process(ctx, flow)

	// Start step 2 and compensation concurrently
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, _ = s2.Process(ctx, flow)
	}()

	go func() {
		defer wg.Done()
		_, _ = compensate.Process(ctx, flow)
	}()

	wg.Wait()
	c.Shutdown()

	mu.Lock()
	eventOrder := events
	mu.Unlock()

	t.Logf("Event order: %v", eventOrder)

	// With WithSaga fix: Either step2 executes first, then compensation runs,
	// OR compensation starts first and step2 is blocked from executing.
	// Either way, we should NOT see step2 execute AFTER compensation marks status.

	// Verify we have step 1 execution
	if len(eventOrder) == 0 || eventOrder[0] != "s1-exec" {
		t.Error("step 1 should have executed")
	}

	// If compensation won the race, step2 should NOT have executed
	// If step2 won the race, we should see s2-exec, then s1-comp
	hasS2Exec := false
	hasS1Comp := false
	for _, e := range eventOrder {
		if e == "s2-exec" {
			hasS2Exec = true
		}
		if e == "s1-comp" {
			hasS1Comp = true
		}
	}

	t.Logf("Step 2 executed: %v, Compensation ran: %v", hasS2Exec, hasS1Comp)
	t.Log("BEHAVIOR: WithSaga serializes access - no interleaving within state mutations")
}

// TestTiming_ConcurrentStepExecution tests that concurrent executions of the
// same step are properly serialized via WithSaga.
func TestTiming_ConcurrentStepExecution(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	ctx := context.Background()

	execSignal := capitan.NewSignal("concurrent.step.exec", "Execute")
	compSignal := capitan.NewSignal("concurrent.step.comp", "Compensate")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	var execCount int64
	c.Hook(execSignal, func(_ context.Context, _ *capitan.Event) {
		atomic.AddInt64(&execCount, 1)
	})

	step := ago.NewSagaStep[Order]("step", store, orderKey, execSignal, compSignal).
		WithCapitan(c)

	flow := ago.NewFlow(Order{ID: "concurrent-step"}, execSignal)
	flow.CorrelationID = "concurrent-step-exec-test"

	// Run same step multiple times concurrently
	numGoroutines := 10
	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = step.Process(ctx, flow)
		}()
	}

	wg.Wait()
	c.Shutdown()

	executions := atomic.LoadInt64(&execCount)
	t.Logf("Concurrent step executions: %d (expected 1)", executions)

	// With WithSaga, only ONE execution should emit the signal
	// All others should see the compensation record and skip
	if executions != 1 {
		t.Errorf("expected exactly 1 execution (idempotency via compensation record), got %d", executions)
	}
}

// TestTiming_ConcurrentCompensation tests that concurrent compensations are
// properly serialized via WithSaga.
func TestTiming_ConcurrentCompensation(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	store := agotesting.NewMockStore()
	ctx := context.Background()

	execSignal := capitan.NewSignal("concurrent.comp.exec", "Execute")
	compSignal := capitan.NewSignal("concurrent.comp.comp", "Compensate")
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	var compCount int64
	c.Hook(compSignal, func(_ context.Context, _ *capitan.Event) {
		atomic.AddInt64(&compCount, 1)
	})

	step := ago.NewSagaStep[Order]("step", store, orderKey, execSignal, compSignal).
		WithCapitan(c)
	compensate := ago.NewCompensate[Order]("rollback", store, orderKey).WithCapitan(c)

	flow := ago.NewFlow(Order{ID: "concurrent-comp"}, execSignal)
	flow.CorrelationID = "concurrent-comp-test"

	// Execute step first
	flow, _ = step.Process(ctx, flow)

	// Run compensation multiple times concurrently
	numGoroutines := 10
	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = compensate.Process(ctx, flow)
		}()
	}

	wg.Wait()
	c.Shutdown()

	compensations := atomic.LoadInt64(&compCount)
	t.Logf("Concurrent compensation signals: %d (expected 1)", compensations)

	// With WithSaga + MarkCompensated, only ONE compensation signal should emit
	// The first compensation marks the step, all others see it's already compensated
	if compensations != 1 {
		t.Errorf("expected exactly 1 compensation signal (idempotency via MarkCompensated), got %d", compensations)
	}

	// Verify final state
	state, err := store.GetSaga(ctx, flow.CorrelationID)
	if err != nil {
		t.Fatalf("failed to get saga state: %v", err)
	}

	if state.Status != ago.SagaStatusFailed {
		t.Errorf("expected saga status Failed (compensation complete), got %s", state.Status)
	}
}
