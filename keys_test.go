package ago

import (
	"context"
	"testing"

	"github.com/zoobzio/capitan"
)

func TestCorrelationKey(t *testing.T) {
	if CorrelationKey.Name() != "correlation_id" {
		t.Errorf("expected 'correlation_id', got %q", CorrelationKey.Name())
	}

	// Test Field creation
	field := CorrelationKey.Field("test-123")
	if field == nil {
		t.Error("expected field to be non-nil")
	}
}

func TestCausationKey(t *testing.T) {
	if CausationKey.Name() != "causation_id" {
		t.Errorf("expected 'causation_id', got %q", CausationKey.Name())
	}

	// Test Field creation
	field := CausationKey.Field("cause-456")
	if field == nil {
		t.Error("expected field to be non-nil")
	}
}

func TestIdempotencyKey(t *testing.T) {
	if IdempotencyKey.Name() != "idempotency_key" {
		t.Errorf("expected 'idempotency_key', got %q", IdempotencyKey.Name())
	}

	// Test Field creation
	field := IdempotencyKey.Field("idem-789")
	if field == nil {
		t.Error("expected field to be non-nil")
	}
}

func TestCorrelationKey_FromEvent(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	signal := capitan.NewSignal("test", "Test")
	var capturedCorrID string

	c.Hook(signal, func(_ context.Context, e *capitan.Event) {
		corrID, ok := CorrelationKey.From(e)
		if ok {
			capturedCorrID = corrID
		}
	})

	c.Emit(context.Background(), signal, CorrelationKey.Field("corr-test-123"))
	c.Shutdown()

	if capturedCorrID != "corr-test-123" {
		t.Errorf("expected 'corr-test-123', got %q", capturedCorrID)
	}
}

func TestCausationKey_FromEvent(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	signal := capitan.NewSignal("test", "Test")
	var capturedCauseID string

	c.Hook(signal, func(_ context.Context, e *capitan.Event) {
		causeID, ok := CausationKey.From(e)
		if ok {
			capturedCauseID = causeID
		}
	})

	c.Emit(context.Background(), signal, CausationKey.Field("cause-test-456"))
	c.Shutdown()

	if capturedCauseID != "cause-test-456" {
		t.Errorf("expected 'cause-test-456', got %q", capturedCauseID)
	}
}

func TestIdempotencyKey_FromEvent(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	signal := capitan.NewSignal("test", "Test")
	var capturedIdemKey string

	c.Hook(signal, func(_ context.Context, e *capitan.Event) {
		idemKey, ok := IdempotencyKey.From(e)
		if ok {
			capturedIdemKey = idemKey
		}
	})

	c.Emit(context.Background(), signal, IdempotencyKey.Field("idem-test-789"))
	c.Shutdown()

	if capturedIdemKey != "idem-test-789" {
		t.Errorf("expected 'idem-test-789', got %q", capturedIdemKey)
	}
}

func TestKey_NotPresent(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	signal := capitan.NewSignal("test", "Test")
	var found bool

	c.Hook(signal, func(_ context.Context, e *capitan.Event) {
		_, found = CorrelationKey.From(e)
	})

	// Emit without correlation key
	c.Emit(context.Background(), signal)
	c.Shutdown()

	if found {
		t.Error("expected CorrelationKey to not be found")
	}
}
