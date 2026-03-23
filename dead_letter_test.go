package ago

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/zoobz-io/capitan"
	"github.com/zoobz-io/herald"
)

// dlqProvider implements herald.Provider for dead letter testing.
type dlqProvider struct {
	mu       sync.Mutex
	messages []dlqMessage
	failNext bool
}

type dlqMessage struct {
	Data     []byte
	Metadata herald.Metadata
}

func (d *dlqProvider) Publish(_ context.Context, data []byte, metadata herald.Metadata) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failNext {
		d.failNext = false
		return errors.New("publish failed")
	}
	d.messages = append(d.messages, dlqMessage{Data: data, Metadata: metadata})
	return nil
}

func (*dlqProvider) Subscribe(_ context.Context) <-chan herald.Result[herald.Message] {
	return nil
}

func (*dlqProvider) Close() error {
	return nil
}

func (d *dlqProvider) getMessages() []dlqMessage {
	d.mu.Lock()
	defer d.mu.Unlock()
	result := make([]dlqMessage, len(d.messages))
	copy(result, d.messages)
	return result
}

func TestDeadLetter_Basic(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	var signalEmitted bool
	var capturedOrder Order
	var capturedCorrID string
	var mu sync.Mutex

	c.Hook(DeadLetterRouted, func(_ context.Context, e *capitan.Event) {
		mu.Lock()
		signalEmitted = true
		order, ok := orderKey.From(e)
		if ok {
			capturedOrder = order
		}
		capturedCorrID, _ = CorrelationKey.From(e)
		mu.Unlock()
	})

	dl := NewDeadLetter[Order]("dead-letter", orderKey).WithCapitan(c)

	flow := NewFlow(Order{ID: "order-123", Total: 99.99}, DeadLetterRouted)
	flow.CorrelationID = "corr-dead-123"

	_, err := dl.Process(ctx, flow)
	if err != nil {
		t.Fatalf("DeadLetter failed: %v", err)
	}

	c.Shutdown()

	mu.Lock()
	defer mu.Unlock()

	if !signalEmitted {
		t.Error("expected DeadLetterRouted signal to be emitted")
	}
	if capturedOrder.ID != "order-123" {
		t.Errorf("expected order ID 'order-123', got %q", capturedOrder.ID)
	}
	if capturedCorrID != "corr-dead-123" {
		t.Errorf("expected correlation_id 'corr-dead-123', got %q", capturedCorrID)
	}
}

func TestDeadLetter_WithErrors(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	var capturedError error
	var mu sync.Mutex

	c.Hook(DeadLetterRouted, func(_ context.Context, e *capitan.Event) {
		mu.Lock()
		capturedError, _ = ErrorKey.From(e)
		mu.Unlock()
	})

	dl := NewDeadLetter[Order]("dead-letter", orderKey).WithCapitan(c)

	flow := NewFlow(Order{ID: "order-123"}, DeadLetterRouted)
	flow.Errors = []error{
		errors.New("error 1"),
		errors.New("error 2"),
	}

	_, err := dl.Process(ctx, flow)
	if err != nil {
		t.Fatalf("DeadLetter failed: %v", err)
	}

	c.Shutdown()

	mu.Lock()
	defer mu.Unlock()

	if capturedError == nil {
		t.Error("expected error field to be set")
	}
}

func TestDeadLetter_WithProvider(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()
	orderKey := capitan.NewKey[Order]("order", "test.Order")
	provider := &dlqProvider{}

	dl := NewDeadLetter[Order]("dead-letter", orderKey).
		WithCapitan(c).
		WithProvider(provider)

	flow := NewFlow(Order{ID: "order-456", Total: 50.0}, DeadLetterRouted)
	flow.CorrelationID = "corr-dlq-456"
	flow.Metadata = map[string]string{"source": "test"}
	flow.Errors = []error{errors.New("processing failed")}

	_, err := dl.Process(ctx, flow)
	if err != nil {
		t.Fatalf("DeadLetter failed: %v", err)
	}

	c.Shutdown()

	messages := provider.getMessages()
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	// Verify data
	var order Order
	if err := json.Unmarshal(messages[0].Data, &order); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if order.ID != "order-456" {
		t.Errorf("expected ID 'order-456', got %q", order.ID)
	}

	// Verify metadata
	if messages[0].Metadata["correlation_id"] != "corr-dlq-456" {
		t.Errorf("expected correlation_id 'corr-dlq-456', got %q", messages[0].Metadata["correlation_id"])
	}
	if messages[0].Metadata["source"] != "test" {
		t.Errorf("expected source 'test', got %q", messages[0].Metadata["source"])
	}
	if messages[0].Metadata["error"] != "processing failed" {
		t.Errorf("expected error message, got %q", messages[0].Metadata["error"])
	}
}

func TestDeadLetter_ProviderError(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()
	orderKey := capitan.NewKey[Order]("order", "test.Order")
	provider := &dlqProvider{failNext: true}

	dl := NewDeadLetter[Order]("dead-letter", orderKey).
		WithCapitan(c).
		WithProvider(provider)

	flow := NewFlow(Order{ID: "order-789"}, DeadLetterRouted)

	_, err := dl.Process(ctx, flow)
	if err == nil {
		t.Error("expected error from provider")
	}
}

func TestDeadLetter_WithCustomSignal(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	ctx := context.Background()
	orderKey := capitan.NewKey[Order]("order", "test.Order")
	customSignal := capitan.NewSignal("custom.dlq", "Custom DLQ")

	var signalEmitted bool
	var mu sync.Mutex

	c.Hook(customSignal, func(_ context.Context, _ *capitan.Event) {
		mu.Lock()
		signalEmitted = true
		mu.Unlock()
	})

	dl := NewDeadLetter[Order]("dead-letter", orderKey).
		WithCapitan(c).
		WithSignal(customSignal)

	flow := NewFlow(Order{ID: "order-custom"}, customSignal)

	_, err := dl.Process(ctx, flow)
	if err != nil {
		t.Fatalf("DeadLetter failed: %v", err)
	}

	c.Shutdown()

	mu.Lock()
	defer mu.Unlock()

	if !signalEmitted {
		t.Error("expected custom signal to be emitted")
	}
}

func TestDeadLetter_Name(t *testing.T) {
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	dl := NewDeadLetter[Order]("my-dead-letter", orderKey)

	if dl.Name() != "my-dead-letter" {
		t.Errorf("expected name 'my-dead-letter', got %q", dl.Name())
	}
}

func TestDeadLetter_Close(t *testing.T) {
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	dl := NewDeadLetter[Order]("my-dead-letter", orderKey)

	if err := dl.Close(); err != nil {
		t.Errorf("expected nil error from Close, got %v", err)
	}
}

func TestDeadLetter_Build(t *testing.T) {
	orderKey := capitan.NewKey[Order]("order", "test.Order")

	dl := NewDeadLetter[Order]("my-dead-letter", orderKey)
	built := dl.Build()

	if built == nil {
		t.Error("expected Build to return non-nil Chainable")
	}
}
