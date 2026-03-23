package ago

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zoobz-io/capitan"
	"github.com/zoobz-io/pipz"
)

type RequestPayload struct {
	Query string `json:"query"`
}

type ResponsePayload struct {
	Result string `json:"result"`
}

func TestRequest_Success(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()

	requestSignal := capitan.NewSignal("query.request", "Query request")
	responseSignal := capitan.NewSignal("query.response", "Query response")
	requestKey := capitan.NewKey[RequestPayload]("request", "test.Request")
	responseKey := capitan.NewKey[ResponsePayload]("response", "test.Response")

	// Set up responder that replies immediately
	c.Hook(requestSignal, func(ctx context.Context, e *capitan.Event) {
		corrID, _ := CorrelationKey.From(e)
		c.Emit(ctx, responseSignal,
			responseKey.Field(ResponsePayload{Result: "success"}),
			CorrelationKey.Field(corrID),
		)
	})

	// Create request primitive
	req := NewRequest[RequestPayload, ResponsePayload](pipz.NewIdentity("query", ""), requestSignal, responseSignal, requestKey, responseKey).
		WithCapitan(c).
		Timeout(100 * time.Millisecond)

	flow := NewFlow(RequestPayload{Query: "test"}, requestSignal)
	flow.CorrelationID = "req-123"

	result, err := req.Process(ctx, flow)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}

	// Verify response was stored in flow
	resp, ok := From(result, responseKey)
	if !ok {
		t.Fatal("response not found in flow")
	}
	if resp.Result != "success" {
		t.Errorf("expected 'success', got %q", resp.Result)
	}

	c.Shutdown()
}

func TestRequest_Timeout(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()

	requestSignal := capitan.NewSignal("slow.request", "Slow request")
	responseSignal := capitan.NewSignal("slow.response", "Slow response")
	requestKey := capitan.NewKey[RequestPayload]("request", "test.Request")
	responseKey := capitan.NewKey[ResponsePayload]("response", "test.Response")

	// No responder - will timeout

	req := NewRequest[RequestPayload, ResponsePayload](pipz.NewIdentity("slow", ""), requestSignal, responseSignal, requestKey, responseKey).
		WithCapitan(c).
		Timeout(50 * time.Millisecond)

	flow := NewFlow(RequestPayload{Query: "test"}, requestSignal)
	flow.CorrelationID = "timeout-123"

	_, err := req.Process(ctx, flow)
	if !errors.Is(err, ErrTimeout) {
		t.Errorf("expected ErrTimeout, got %v", err)
	}

	c.Shutdown()
}

func TestAwait_Success(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()

	eventSignal := capitan.NewSignal("order.completed", "Order completed")
	statusKey := capitan.NewStringKey("status")

	// Emit the event in a goroutine after a short delay
	go func() {
		time.Sleep(10 * time.Millisecond)
		c.Emit(ctx, eventSignal,
			statusKey.Field("completed"),
			CorrelationKey.Field("await-123"),
		)
	}()

	await := NewAwait[Order, string](pipz.NewIdentity("wait-complete", ""), eventSignal, statusKey).
		WithCapitan(c).
		Timeout(100 * time.Millisecond)

	flow := NewFlow(Order{ID: "1"}, eventSignal)
	flow.CorrelationID = "await-123"

	result, err := await.Process(ctx, flow)
	if err != nil {
		t.Fatalf("await failed: %v", err)
	}

	status, ok := From(result, statusKey)
	if !ok {
		t.Fatal("status not found in flow")
	}
	if status != "completed" {
		t.Errorf("expected 'completed', got %q", status)
	}

	c.Shutdown()
}

func TestAwait_Timeout(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()

	eventSignal := capitan.NewSignal("never.happens", "Never happens")
	statusKey := capitan.NewStringKey("status")

	await := NewAwait[Order, string](pipz.NewIdentity("wait-never", ""), eventSignal, statusKey).
		WithCapitan(c).
		Timeout(50 * time.Millisecond)

	flow := NewFlow(Order{ID: "1"}, eventSignal)
	flow.CorrelationID = "await-timeout"

	_, err := await.Process(ctx, flow)
	if !errors.Is(err, ErrTimeout) {
		t.Errorf("expected ErrTimeout, got %v", err)
	}

	c.Shutdown()
}
