package ago

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zoobz-io/capitan"
)

func TestMemoryStore_Pending(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	signal := capitan.NewSignal("test.signal", "Test")
	state := &PendingState{
		CorrelationID: "corr-123",
		Signal:        signal,
		CreatedAt:     time.Now(),
		Timeout:       30 * time.Second,
	}

	// Set
	if err := store.SetPending(ctx, "corr-123", state); err != nil {
		t.Fatalf("SetPending failed: %v", err)
	}

	// Get
	got, err := store.GetPending(ctx, "corr-123")
	if err != nil {
		t.Fatalf("GetPending failed: %v", err)
	}
	if got.CorrelationID != "corr-123" {
		t.Errorf("expected 'corr-123', got %q", got.CorrelationID)
	}

	// Delete
	if err := store.DeletePending(ctx, "corr-123"); err != nil {
		t.Fatalf("DeletePending failed: %v", err)
	}

	// Get after delete
	_, err = store.GetPending(ctx, "corr-123")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestMemoryStore_Saga(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	signal := capitan.NewSignal("comp.signal", "Compensation")
	state := &SagaState{
		CorrelationID: "saga-123",
		Status:        SagaStatusRunning,
		CurrentStep:   1,
		Compensations: []CompensationRecord{
			{StepName: "step1", Signal: signal, Data: []byte(`{"id":"1"}`)},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Set
	if err := store.SetSaga(ctx, "saga-123", state); err != nil {
		t.Fatalf("SetSaga failed: %v", err)
	}

	// Get
	got, err := store.GetSaga(ctx, "saga-123")
	if err != nil {
		t.Fatalf("GetSaga failed: %v", err)
	}
	if got.Status != SagaStatusRunning {
		t.Errorf("expected Running, got %v", got.Status)
	}
	if len(got.Compensations) != 1 {
		t.Errorf("expected 1 compensation, got %d", len(got.Compensations))
	}

	// Update
	state.Status = SagaStatusCompensating
	state.UpdatedAt = time.Now()
	if err := store.UpdateSaga(ctx, "saga-123", state); err != nil {
		t.Fatalf("UpdateSaga failed: %v", err)
	}

	got, _ = store.GetSaga(ctx, "saga-123")
	if got.Status != SagaStatusCompensating {
		t.Errorf("expected Compensating, got %v", got.Status)
	}

	// List incomplete
	incomplete, err := store.ListIncompleteSagas(ctx)
	if err != nil {
		t.Fatalf("ListIncompleteSagas failed: %v", err)
	}
	if len(incomplete) != 1 {
		t.Errorf("expected 1 incomplete saga, got %d", len(incomplete))
	}

	// Mark complete and verify list
	state.Status = SagaStatusCompleted
	_ = store.UpdateSaga(ctx, "saga-123", state)

	incomplete, _ = store.ListIncompleteSagas(ctx)
	if len(incomplete) != 0 {
		t.Errorf("expected 0 incomplete sagas, got %d", len(incomplete))
	}

	// Delete
	if err := store.DeleteSaga(ctx, "saga-123"); err != nil {
		t.Fatalf("DeleteSaga failed: %v", err)
	}

	_, err = store.GetSaga(ctx, "saga-123")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestMemoryStore_UpdateNonexistent(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	state := &SagaState{
		CorrelationID: "nonexistent",
		Status:        SagaStatusRunning,
	}

	err := store.UpdateSaga(ctx, "nonexistent", state)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}
