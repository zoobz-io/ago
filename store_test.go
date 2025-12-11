package ago

import (
	"bytes"
	"testing"
	"time"
)

func TestSagaState_IsExpired(t *testing.T) {
	// Not expired: no timeout
	state := &SagaState{
		CreatedAt: time.Now().Add(-1 * time.Hour),
		Timeout:   0,
	}
	if state.IsExpired() {
		t.Error("saga with no timeout should not be expired")
	}

	// Not expired: within timeout
	state = &SagaState{
		CreatedAt: time.Now().Add(-1 * time.Minute),
		Timeout:   5 * time.Minute,
	}
	if state.IsExpired() {
		t.Error("saga within timeout should not be expired")
	}

	// Expired: past timeout
	state = &SagaState{
		CreatedAt: time.Now().Add(-10 * time.Minute),
		Timeout:   5 * time.Minute,
	}
	if !state.IsExpired() {
		t.Error("saga past timeout should be expired")
	}
}

func TestSagaState_IsExpired_EdgeCase(t *testing.T) {
	// Exactly at timeout boundary
	state := &SagaState{
		CreatedAt: time.Now().Add(-5 * time.Minute),
		Timeout:   5 * time.Minute,
	}
	// Should be expired (> not >=)
	if !state.IsExpired() {
		t.Error("saga at timeout boundary should be expired")
	}
}

func TestSagaStatus_Values(t *testing.T) {
	// Verify status constants have expected values
	if SagaStatusPending != "pending" {
		t.Errorf("expected 'pending', got %q", SagaStatusPending)
	}
	if SagaStatusRunning != "running" {
		t.Errorf("expected 'running', got %q", SagaStatusRunning)
	}
	if SagaStatusCompensating != "compensating" {
		t.Errorf("expected 'compensating', got %q", SagaStatusCompensating)
	}
	if SagaStatusCompleted != "completed" {
		t.Errorf("expected 'completed', got %q", SagaStatusCompleted)
	}
	if SagaStatusFailed != "failed" {
		t.Errorf("expected 'failed', got %q", SagaStatusFailed)
	}
}

func TestPendingState_Fields(t *testing.T) {
	now := time.Now()
	state := &PendingState{
		CorrelationID: "test-123",
		CreatedAt:     now,
		Timeout:       30 * time.Second,
	}

	if state.CorrelationID != "test-123" {
		t.Errorf("expected 'test-123', got %q", state.CorrelationID)
	}
	if state.CreatedAt != now {
		t.Errorf("expected %v, got %v", now, state.CreatedAt)
	}
	if state.Timeout != 30*time.Second {
		t.Errorf("expected 30s, got %v", state.Timeout)
	}
}

func TestCompensationRecord_Fields(t *testing.T) {
	data := []byte(`{"test": true}`)
	record := CompensationRecord{
		StepName: "reserve-inventory",
		Data:     data,
	}

	if record.StepName != "reserve-inventory" {
		t.Errorf("expected 'reserve-inventory', got %q", record.StepName)
	}
	if !bytes.Equal(record.Data, data) {
		t.Errorf("expected %s, got %s", data, record.Data)
	}
}

func TestErrNotFound(t *testing.T) {
	if ErrNotFound.Error() != "ago: state not found" {
		t.Errorf("expected 'ago: state not found', got %q", ErrNotFound.Error())
	}
}

func TestErrTimeout(t *testing.T) {
	if ErrTimeout.Error() != "ago: request timeout" {
		t.Errorf("expected 'ago: request timeout', got %q", ErrTimeout.Error())
	}
}
