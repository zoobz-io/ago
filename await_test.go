package ago

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zoobzio/capitan"
)

func TestAwait_Basic(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()
	responseSignal := capitan.NewSignal("order.response", "Order response")
	responseKey := capitan.NewKey[string]("response", "Response value")

	// Set up await
	await := NewAwait[Order, string]("await-response", responseSignal, responseKey).
		WithCapitan(c).
		Timeout(100 * time.Millisecond)

	flow := NewFlow(Order{ID: "order-123"}, responseSignal)
	flow.CorrelationID = "await-test-123"

	// Emit response in a goroutine after a short delay
	go func() {
		time.Sleep(10 * time.Millisecond)
		c.Emit(ctx, responseSignal,
			responseKey.Field("success"),
			CorrelationKey.Field("await-test-123"),
		)
	}()

	result, err := await.Process(ctx, flow)

	if err != nil {
		t.Fatalf("Await failed: %v", err)
	}

	// Check the response was captured
	response, ok := From(result, responseKey)
	if !ok {
		t.Error("expected response field to be set")
	}
	if response != "success" {
		t.Errorf("expected response 'success', got %q", response)
	}
}

func TestAwait_TimeoutExpired(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()
	responseSignal := capitan.NewSignal("order.response", "Order response")
	responseKey := capitan.NewKey[string]("response", "Response value")

	await := NewAwait[Order, string]("await-response", responseSignal, responseKey).
		WithCapitan(c).
		Timeout(10 * time.Millisecond)

	flow := NewFlow(Order{ID: "order-123"}, responseSignal)
	flow.CorrelationID = "await-timeout-test"

	// Don't emit anything - should timeout
	_, err := await.Process(ctx, flow)

	if !errors.Is(err, ErrTimeout) {
		t.Errorf("expected ErrTimeout, got %v", err)
	}
}

func TestAwait_WrongCorrelationID(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()
	responseSignal := capitan.NewSignal("order.response", "Order response")
	responseKey := capitan.NewKey[string]("response", "Response value")

	await := NewAwait[Order, string]("await-response", responseSignal, responseKey).
		WithCapitan(c).
		Timeout(50 * time.Millisecond)

	flow := NewFlow(Order{ID: "order-123"}, responseSignal)
	flow.CorrelationID = "await-test-correct"

	// Emit response with wrong correlation ID
	go func() {
		time.Sleep(10 * time.Millisecond)
		c.Emit(ctx, responseSignal,
			responseKey.Field("wrong"),
			CorrelationKey.Field("await-test-wrong"),
		)
	}()

	_, err := await.Process(ctx, flow)

	// Should timeout because correlation ID doesn't match
	if !errors.Is(err, ErrTimeout) {
		t.Errorf("expected ErrTimeout for wrong correlation ID, got %v", err)
	}
}

func TestAwait_Name(t *testing.T) {
	responseSignal := capitan.NewSignal("order.response", "Order response")
	responseKey := capitan.NewKey[string]("response", "Response value")

	await := NewAwait[Order, string]("my-await", responseSignal, responseKey)

	if await.Name() != "my-await" {
		t.Errorf("expected name 'my-await', got %q", await.Name())
	}
}

func TestAwait_Close(t *testing.T) {
	responseSignal := capitan.NewSignal("order.response", "Order response")
	responseKey := capitan.NewKey[string]("response", "Response value")

	await := NewAwait[Order, string]("my-await", responseSignal, responseKey)

	if err := await.Close(); err != nil {
		t.Errorf("expected nil error from Close, got %v", err)
	}
}

func TestAwait_DefaultTimeout(t *testing.T) {
	responseSignal := capitan.NewSignal("order.response", "Order response")
	responseKey := capitan.NewKey[string]("response", "Response value")

	await := NewAwait[Order, string]("my-await", responseSignal, responseKey)

	// Default timeout should be 30 seconds
	if await.timeout != 30*time.Second {
		t.Errorf("expected default timeout 30s, got %v", await.timeout)
	}
}

func TestAwait_Build(t *testing.T) {
	responseSignal := capitan.NewSignal("order.response", "Order response")
	responseKey := capitan.NewKey[string]("response", "Response value")

	await := NewAwait[Order, string]("my-await", responseSignal, responseKey)
	built := await.Build()

	if built == nil {
		t.Error("expected Build to return non-nil Chainable")
	}
}

func TestAwait_ContextCancellation(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	responseSignal := capitan.NewSignal("order.response", "Order response")
	responseKey := capitan.NewKey[string]("response", "Response value")

	await := NewAwait[Order, string]("await-response", responseSignal, responseKey).
		WithCapitan(c).
		Timeout(5 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())

	flow := NewFlow(Order{ID: "order-123"}, responseSignal)
	flow.CorrelationID = "await-cancel-test"

	// Cancel context after short delay
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	_, err := await.Process(ctx, flow)

	// Should return timeout error when context is canceled
	if !errors.Is(err, ErrTimeout) {
		t.Errorf("expected ErrTimeout on context cancellation, got %v", err)
	}
}
