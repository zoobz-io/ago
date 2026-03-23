package integration

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zoobz-io/ago"
	"github.com/zoobz-io/capitan"
	"github.com/zoobz-io/pipz"
)

// Query represents a test query for request/response tests.
type Query struct {
	Term string
}

// Result represents a test result for request/response tests.
type Result struct {
	Data  string
	Count int
}

// TestRequestResponse_Success tests successful request/response pattern.
func TestRequestResponse_Success(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()

	requestSignal := capitan.NewSignal("search.request", "Search request")
	responseSignal := capitan.NewSignal("search.response", "Search response")
	queryKey := capitan.NewKey[Query]("query", "test.Query")
	resultKey := capitan.NewKey[Result]("result", "test.Result")

	// Set up responder
	c.Hook(requestSignal, func(ctx context.Context, e *capitan.Event) {
		query, _ := queryKey.From(e)
		corrID, _ := ago.CorrelationKey.From(e)

		// Simulate processing
		result := Result{Data: "results for: " + query.Term, Count: 42}

		c.Emit(ctx, responseSignal,
			resultKey.Field(result),
			ago.CorrelationKey.Field(corrID),
		)
	})

	req := ago.NewRequest[Query, Result](pipz.NewIdentity("search", ""), requestSignal, responseSignal, queryKey, resultKey).
		WithCapitan(c).
		Timeout(100 * time.Millisecond)

	flow := ago.NewFlow(Query{Term: "golang"}, requestSignal)
	flow.CorrelationID = "req-success-test"

	result, err := req.Process(ctx, flow)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}

	// Verify response in flow
	resp, ok := ago.From(result, resultKey)
	if !ok {
		t.Fatal("response not found in flow")
	}
	if resp.Count != 42 {
		t.Errorf("expected count 42, got %d", resp.Count)
	}
	if resp.Data != "results for: golang" {
		t.Errorf("unexpected data: %q", resp.Data)
	}
}

// TestRequestResponse_Timeout tests request timeout handling.
func TestRequestResponse_Timeout(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()

	requestSignal := capitan.NewSignal("slow.request", "Slow request")
	responseSignal := capitan.NewSignal("slow.response", "Slow response")
	queryKey := capitan.NewKey[Query]("query", "test.Query")
	resultKey := capitan.NewKey[Result]("result", "test.Result")

	// No responder - will timeout

	req := ago.NewRequest[Query, Result](pipz.NewIdentity("slow", ""), requestSignal, responseSignal, queryKey, resultKey).
		WithCapitan(c).
		Timeout(50 * time.Millisecond)

	flow := ago.NewFlow(Query{Term: "test"}, requestSignal)
	flow.CorrelationID = "req-timeout-test"

	_, err := req.Process(ctx, flow)
	if !errors.Is(err, ago.ErrTimeout) {
		t.Errorf("expected ErrTimeout, got %v", err)
	}
}

// TestRequestResponse_ConcurrentRequests tests multiple concurrent requests.
func TestRequestResponse_ConcurrentRequests(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()

	requestSignal := capitan.NewSignal("concurrent.request", "Request")
	responseSignal := capitan.NewSignal("concurrent.response", "Response")
	queryKey := capitan.NewKey[Query]("query", "test.Query")
	resultKey := capitan.NewKey[Result]("result", "test.Result")

	// Set up responder with small delay
	c.Hook(requestSignal, func(ctx context.Context, e *capitan.Event) {
		query, _ := queryKey.From(e)
		corrID, _ := ago.CorrelationKey.From(e)

		time.Sleep(10 * time.Millisecond) // Simulate work

		result := Result{Data: query.Term, Count: len(query.Term)}
		c.Emit(ctx, responseSignal,
			resultKey.Field(result),
			ago.CorrelationKey.Field(corrID),
		)
	})

	req := ago.NewRequest[Query, Result](pipz.NewIdentity("concurrent", ""), requestSignal, responseSignal, queryKey, resultKey).
		WithCapitan(c).
		Timeout(500 * time.Millisecond)

	numRequests := 10
	var wg sync.WaitGroup
	results := make(chan Result, numRequests)
	errs := make(chan error, numRequests)

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			term := string(rune('a' + idx))
			flow := ago.NewFlow(Query{Term: term}, requestSignal)
			flow.CorrelationID = "concurrent-req-" + term

			result, err := req.Process(ctx, flow)
			if err != nil {
				errs <- err
				return
			}

			resp, ok := ago.From(result, resultKey)
			if !ok {
				errs <- errors.New("response not found")
				return
			}
			results <- resp
		}(i)
	}

	wg.Wait()
	close(results)
	close(errs)

	// Check for errors
	for err := range errs {
		t.Errorf("request error: %v", err)
	}

	// Verify all responses received
	count := 0
	for range results {
		count++
	}
	if count != numRequests {
		t.Errorf("expected %d responses, got %d", numRequests, count)
	}
}

// TestRequestResponse_CorrelationMismatch tests that wrong correlation IDs are ignored.
func TestRequestResponse_CorrelationMismatch(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()

	requestSignal := capitan.NewSignal("mismatch.request", "Request")
	responseSignal := capitan.NewSignal("mismatch.response", "Response")
	queryKey := capitan.NewKey[Query]("query", "test.Query")
	resultKey := capitan.NewKey[Result]("result", "test.Result")

	// Responder that sends wrong correlation ID
	c.Hook(requestSignal, func(ctx context.Context, _ *capitan.Event) {
		c.Emit(ctx, responseSignal,
			resultKey.Field(Result{Data: "wrong", Count: 0}),
			ago.CorrelationKey.Field("wrong-correlation-id"),
		)
	})

	req := ago.NewRequest[Query, Result](pipz.NewIdentity("mismatch", ""), requestSignal, responseSignal, queryKey, resultKey).
		WithCapitan(c).
		Timeout(50 * time.Millisecond)

	flow := ago.NewFlow(Query{Term: "test"}, requestSignal)
	flow.CorrelationID = "correct-correlation-id"

	_, err := req.Process(ctx, flow)
	if !errors.Is(err, ago.ErrTimeout) {
		t.Errorf("expected timeout (wrong correlation ignored), got %v", err)
	}
}

// TestAwait_Success tests successful await pattern.
func TestAwait_Success(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()

	eventSignal := capitan.NewSignal("order.shipped", "Order shipped")
	trackingKey := capitan.NewStringKey("tracking_number")

	// Emit event after short delay
	go func() {
		time.Sleep(20 * time.Millisecond)
		c.Emit(ctx, eventSignal,
			trackingKey.Field("TRACK123"),
			ago.CorrelationKey.Field("await-success-test"),
		)
	}()

	await := ago.NewAwait[Order, string](pipz.NewIdentity("wait-shipment", ""), eventSignal, trackingKey).
		WithCapitan(c).
		Timeout(200 * time.Millisecond)

	flow := ago.NewFlow(Order{ID: "order-123"}, eventSignal)
	flow.CorrelationID = "await-success-test"

	result, err := await.Process(ctx, flow)
	if err != nil {
		t.Fatalf("await failed: %v", err)
	}

	tracking, ok := ago.From(result, trackingKey)
	if !ok {
		t.Fatal("tracking not found in flow")
	}
	if tracking != "TRACK123" {
		t.Errorf("expected TRACK123, got %q", tracking)
	}
}

// TestAwait_Timeout tests await timeout handling.
func TestAwait_Timeout(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()

	eventSignal := capitan.NewSignal("never.happens", "Never happens")
	statusKey := capitan.NewStringKey("status")

	await := ago.NewAwait[Order, string](pipz.NewIdentity("wait-never", ""), eventSignal, statusKey).
		WithCapitan(c).
		Timeout(50 * time.Millisecond)

	flow := ago.NewFlow(Order{ID: "order-123"}, eventSignal)
	flow.CorrelationID = "await-timeout-test"

	_, err := await.Process(ctx, flow)
	if !errors.Is(err, ago.ErrTimeout) {
		t.Errorf("expected ErrTimeout, got %v", err)
	}
}

// TestAwait_MultipleWaiters tests multiple flows waiting for same signal type.
func TestAwait_MultipleWaiters(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()

	eventSignal := capitan.NewSignal("order.completed", "Order completed")
	statusKey := capitan.NewStringKey("status")

	numWaiters := 5
	var wg sync.WaitGroup
	var successCount int64

	// Start waiters
	for i := 0; i < numWaiters; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			await := ago.NewAwait[Order, string](pipz.NewIdentity("wait", ""), eventSignal, statusKey).
				WithCapitan(c).
				Timeout(200 * time.Millisecond)

			corrID := "multi-await-" + string(rune('0'+idx))
			flow := ago.NewFlow(Order{ID: corrID}, eventSignal)
			flow.CorrelationID = corrID

			// Emit corresponding event
			go func() {
				time.Sleep(20 * time.Millisecond)
				c.Emit(ctx, eventSignal,
					statusKey.Field("completed"),
					ago.CorrelationKey.Field(corrID),
				)
			}()

			_, err := await.Process(ctx, flow)
			if err == nil {
				atomic.AddInt64(&successCount, 1)
			}
		}(i)
	}

	wg.Wait()

	if atomic.LoadInt64(&successCount) != int64(numWaiters) {
		t.Errorf("expected %d successes, got %d", numWaiters, successCount)
	}
}

// TestRequestResponse_ChainedRequests tests request followed by another request.
func TestRequestResponse_ChainedRequests(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()

	// First request/response
	req1Signal := capitan.NewSignal("step1.request", "Step 1 request")
	resp1Signal := capitan.NewSignal("step1.response", "Step 1 response")
	query1Key := capitan.NewKey[Query]("query1", "test.Query")
	result1Key := capitan.NewKey[Result]("result1", "test.Result")

	// Second request/response
	req2Signal := capitan.NewSignal("step2.request", "Step 2 request")
	resp2Signal := capitan.NewSignal("step2.response", "Step 2 response")
	query2Key := capitan.NewKey[Query]("query2", "test.Query")
	result2Key := capitan.NewKey[Result]("result2", "test.Result")

	// Set up responders
	c.Hook(req1Signal, func(ctx context.Context, e *capitan.Event) {
		corrID, _ := ago.CorrelationKey.From(e)
		c.Emit(ctx, resp1Signal,
			result1Key.Field(Result{Data: "step1-done", Count: 1}),
			ago.CorrelationKey.Field(corrID),
		)
	})

	c.Hook(req2Signal, func(ctx context.Context, e *capitan.Event) {
		corrID, _ := ago.CorrelationKey.From(e)
		c.Emit(ctx, resp2Signal,
			result2Key.Field(Result{Data: "step2-done", Count: 2}),
			ago.CorrelationKey.Field(corrID),
		)
	})

	req1 := ago.NewRequest[Query, Result](pipz.NewIdentity("step1", ""), req1Signal, resp1Signal, query1Key, result1Key).
		WithCapitan(c).
		Timeout(100 * time.Millisecond)

	req2 := ago.NewRequest[Query, Result](pipz.NewIdentity("step2", ""), req2Signal, resp2Signal, query2Key, result2Key).
		WithCapitan(c).
		Timeout(100 * time.Millisecond)

	flow := ago.NewFlow(Query{Term: "chain-test"}, req1Signal)
	flow.CorrelationID = "chained-req-test"

	// Execute first request
	flow, err := req1.Process(ctx, flow)
	if err != nil {
		t.Fatalf("step1 failed: %v", err)
	}

	// Verify first result
	res1, ok := ago.From(flow, result1Key)
	if !ok || res1.Count != 1 {
		t.Fatal("step1 result not found or incorrect")
	}

	// Execute second request
	flow, err = req2.Process(ctx, flow)
	if err != nil {
		t.Fatalf("step2 failed: %v", err)
	}

	// Verify second result
	res2, ok := ago.From(flow, result2Key)
	if !ok || res2.Count != 2 {
		t.Fatal("step2 result not found or incorrect")
	}

	// Both results should be in flow
	_, ok1 := ago.From(flow, result1Key)
	_, ok2 := ago.From(flow, result2Key)
	if !ok1 || !ok2 {
		t.Error("expected both results in flow")
	}
}
