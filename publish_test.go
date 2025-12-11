package ago

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/zoobzio/capitan"
	"github.com/zoobzio/herald"
)

// mockProvider implements herald.Provider for testing.
type mockProvider struct {
	mu       sync.Mutex
	messages []mockMessage
	failNext bool
}

type mockMessage struct {
	Data     []byte
	Metadata herald.Metadata
}

func (m *mockProvider) Publish(_ context.Context, data []byte, metadata herald.Metadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNext {
		m.failNext = false
		return ErrNotFound // reuse error for testing
	}
	m.messages = append(m.messages, mockMessage{Data: data, Metadata: metadata})
	return nil
}

func (*mockProvider) Subscribe(_ context.Context) <-chan herald.Result[herald.Message] {
	return nil
}

func (*mockProvider) Close() error {
	return nil
}

func (m *mockProvider) getMessages() []mockMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]mockMessage, len(m.messages))
	copy(result, m.messages)
	return result
}

func TestPublish(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")
	provider := &mockProvider{}

	publish := Publish[Order]("publish-order", provider)

	flow := NewFlow(Order{ID: "order-123", Total: 99.99}, signal)
	flow.CorrelationID = "corr-123"
	flow.CausationID = "cause-456"

	result, err := publish.Process(ctx, flow)

	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Flow should be unchanged
	if result.Payload.ID != "order-123" {
		t.Errorf("expected ID 'order-123', got %q", result.Payload.ID)
	}

	// Verify message was published
	messages := provider.getMessages()
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	// Verify data
	var order Order
	if err := json.Unmarshal(messages[0].Data, &order); err != nil {
		t.Fatalf("failed to unmarshal data: %v", err)
	}
	if order.ID != "order-123" {
		t.Errorf("expected ID 'order-123', got %q", order.ID)
	}
	if order.Total != 99.99 {
		t.Errorf("expected Total 99.99, got %v", order.Total)
	}

	// Verify metadata
	if messages[0].Metadata["correlation_id"] != "corr-123" {
		t.Errorf("expected correlation_id 'corr-123', got %q", messages[0].Metadata["correlation_id"])
	}
	if messages[0].Metadata["causation_id"] != "cause-456" {
		t.Errorf("expected causation_id 'cause-456', got %q", messages[0].Metadata["causation_id"])
	}
}

func TestPublish_WithFlowMetadata(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")
	provider := &mockProvider{}

	publish := Publish[Order]("publish-order", provider)

	flow := NewFlow(Order{ID: "order-456"}, signal)
	flow.Metadata = map[string]string{
		"custom_key": "custom_value",
		"source":     "test",
	}

	_, err := publish.Process(ctx, flow)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	messages := provider.getMessages()
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	// Verify custom metadata was copied
	if messages[0].Metadata["custom_key"] != "custom_value" {
		t.Errorf("expected custom_key 'custom_value', got %q", messages[0].Metadata["custom_key"])
	}
	if messages[0].Metadata["source"] != "test" {
		t.Errorf("expected source 'test', got %q", messages[0].Metadata["source"])
	}
}

func TestPublish_NoCorrelationID(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")
	provider := &mockProvider{}

	publish := Publish[Order]("publish-order", provider)

	flow := NewFlow(Order{ID: "order-789"}, signal)
	// No CorrelationID or CausationID set

	_, err := publish.Process(ctx, flow)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	messages := provider.getMessages()
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	// Verify correlation_id is not in metadata (or empty)
	if _, exists := messages[0].Metadata["correlation_id"]; exists {
		t.Error("expected correlation_id to not be set")
	}
}

func TestPublish_ProviderError(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")
	provider := &mockProvider{failNext: true}

	publish := Publish[Order]("publish-order", provider)

	flow := NewFlow(Order{ID: "order-fail"}, signal)

	_, err := publish.Process(ctx, flow)
	if err == nil {
		t.Error("expected error from provider")
	}
}

func TestPublish_MultipleMessages(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")
	provider := &mockProvider{}

	publish := Publish[Order]("publish-order", provider)

	for i := 0; i < 5; i++ {
		flow := NewFlow(Order{ID: "order-" + string(rune('0'+i))}, signal)
		_, err := publish.Process(ctx, flow)
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	messages := provider.getMessages()
	if len(messages) != 5 {
		t.Errorf("expected 5 messages, got %d", len(messages))
	}
}
