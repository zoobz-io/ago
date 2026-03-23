package testing

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zoobz-io/ago"
	"github.com/zoobz-io/capitan"
)

func TestNewMockStore(t *testing.T) {
	store := NewMockStore()
	if store == nil {
		t.Fatal("expected non-nil store")
	}
	if store.MemoryStore == nil {
		t.Error("expected embedded MemoryStore")
	}
}

func TestMockStore_Tracking(t *testing.T) {
	ctx := context.Background()
	store := NewMockStore()

	// Test GetSaga tracking
	_, _ = store.GetSaga(ctx, "test-1")
	if store.GetSagaCalls() != 1 {
		t.Errorf("expected 1 GetSaga call, got %d", store.GetSagaCalls())
	}

	// Test SetSaga tracking
	_ = store.SetSaga(ctx, "test-1", &ago.SagaState{
		CorrelationID: "test-1",
		Status:        ago.SagaStatusRunning,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	})
	if store.SetSagaCalls() != 1 {
		t.Errorf("expected 1 SetSaga call, got %d", store.SetSagaCalls())
	}

	// Test UpdateSaga tracking
	_ = store.UpdateSaga(ctx, "test-1", &ago.SagaState{
		CorrelationID: "test-1",
		Status:        ago.SagaStatusCompleted,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	})
	if store.UpdateSagaCalls() != 1 {
		t.Errorf("expected 1 UpdateSaga call, got %d", store.UpdateSagaCalls())
	}

	// Test MarkCompensated tracking
	_ = store.MarkCompensated(ctx, "test-1", "step1")
	if store.CompensatedCalls() != 1 {
		t.Errorf("expected 1 MarkCompensated call, got %d", store.CompensatedCalls())
	}

	// Test WithSaga tracking
	_ = store.WithSaga(ctx, "test-1", func(s *ago.SagaState) (*ago.SagaState, error) {
		return s, nil
	})
	if store.WithSagaCalls() != 1 {
		t.Errorf("expected 1 WithSaga call, got %d", store.WithSagaCalls())
	}
}

func TestMockStore_FailureInjection(t *testing.T) {
	ctx := context.Background()
	store := NewMockStore()

	// Test FailGetSaga
	store.FailGetSaga(true)
	_, err := store.GetSaga(ctx, "test")
	if !errors.Is(err, ago.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
	store.FailGetSaga(false)

	// Test FailSetSaga
	store.FailSetSaga(true)
	err = store.SetSaga(ctx, "test", &ago.SagaState{})
	if !errors.Is(err, ago.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
	store.FailSetSaga(false)

	// Test FailUpdateSaga
	store.FailUpdateSaga(true)
	err = store.UpdateSaga(ctx, "test", &ago.SagaState{})
	if !errors.Is(err, ago.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
	store.FailUpdateSaga(false)

	// Test FailMarkCompensated
	store.FailMarkCompensated(true)
	err = store.MarkCompensated(ctx, "test", "step")
	if !errors.Is(err, ago.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
	store.FailMarkCompensated(false)

	// Test FailWithSaga
	store.FailWithSaga(true)
	err = store.WithSaga(ctx, "test", func(s *ago.SagaState) (*ago.SagaState, error) {
		return s, nil
	})
	if !errors.Is(err, ago.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestMockStore_Reset(t *testing.T) {
	ctx := context.Background()
	store := NewMockStore()

	// Make some calls
	_, _ = store.GetSaga(ctx, "test")
	_ = store.SetSaga(ctx, "test", &ago.SagaState{})
	store.FailGetSaga(true)

	// Reset
	store.Reset()

	// Verify counts are reset
	if store.GetSagaCalls() != 0 {
		t.Errorf("expected 0 GetSaga calls after reset, got %d", store.GetSagaCalls())
	}
	if store.SetSagaCalls() != 0 {
		t.Errorf("expected 0 SetSaga calls after reset, got %d", store.SetSagaCalls())
	}

	// Verify failures are reset
	_, err := store.GetSaga(ctx, "test")
	if errors.Is(err, ago.ErrNotFound) {
		t.Error("expected failure flag to be reset")
	}
}

func TestNewSignalTracker(t *testing.T) {
	tracker := NewSignalTracker()
	if tracker == nil {
		t.Error("expected non-nil tracker")
	}
	if tracker.Count() != 0 {
		t.Errorf("expected 0 initial signals, got %d", tracker.Count())
	}
}

func TestSignalTracker_Track(t *testing.T) {
	tracker := NewSignalTracker()
	signal := capitan.NewSignal("test.signal", "Test signal")

	tracker.Track(signal, "corr-123")
	tracker.Track(signal, "corr-456")

	if tracker.Count() != 2 {
		t.Errorf("expected 2 signals, got %d", tracker.Count())
	}

	signals := tracker.Signals()
	if len(signals) != 2 {
		t.Fatalf("expected 2 signals, got %d", len(signals))
	}
	if signals[0].Signal.Name() != "test.signal" {
		t.Errorf("expected signal name 'test.signal', got %q", signals[0].Signal.Name())
	}
	if signals[0].CorrelationID != "corr-123" {
		t.Errorf("expected correlation ID 'corr-123', got %q", signals[0].CorrelationID)
	}
}

func TestSignalTracker_SignalNames(t *testing.T) {
	tracker := NewSignalTracker()
	signal1 := capitan.NewSignal("signal.one", "Signal one")
	signal2 := capitan.NewSignal("signal.two", "Signal two")

	tracker.Track(signal1, "")
	tracker.Track(signal2, "")
	tracker.Track(signal1, "")

	names := tracker.SignalNames()
	if len(names) != 3 {
		t.Fatalf("expected 3 names, got %d", len(names))
	}
	if names[0] != "signal.one" {
		t.Errorf("expected 'signal.one', got %q", names[0])
	}
	if names[1] != "signal.two" {
		t.Errorf("expected 'signal.two', got %q", names[1])
	}
	if names[2] != "signal.one" {
		t.Errorf("expected 'signal.one', got %q", names[2])
	}
}

func TestSignalTracker_CountSignal(t *testing.T) {
	tracker := NewSignalTracker()
	signal1 := capitan.NewSignal("signal.one", "Signal one")
	signal2 := capitan.NewSignal("signal.two", "Signal two")

	tracker.Track(signal1, "")
	tracker.Track(signal2, "")
	tracker.Track(signal1, "")
	tracker.Track(signal1, "")

	if tracker.CountSignal("signal.one") != 3 {
		t.Errorf("expected 3 'signal.one', got %d", tracker.CountSignal("signal.one"))
	}
	if tracker.CountSignal("signal.two") != 1 {
		t.Errorf("expected 1 'signal.two', got %d", tracker.CountSignal("signal.two"))
	}
	if tracker.CountSignal("signal.three") != 0 {
		t.Errorf("expected 0 'signal.three', got %d", tracker.CountSignal("signal.three"))
	}
}

func TestSignalTracker_Reset(t *testing.T) {
	tracker := NewSignalTracker()
	signal := capitan.NewSignal("test.signal", "Test signal")

	tracker.Track(signal, "")
	tracker.Track(signal, "")

	tracker.Reset()

	if tracker.Count() != 0 {
		t.Errorf("expected 0 signals after reset, got %d", tracker.Count())
	}
}

func TestHookTracker(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	tracker := NewSignalTracker()
	signal1 := capitan.NewSignal("hook.one", "Hook one")
	signal2 := capitan.NewSignal("hook.two", "Hook two")

	HookTracker(c, tracker, signal1, signal2)

	c.Emit(context.Background(), signal1, ago.CorrelationKey.Field("corr-1"))
	c.Emit(context.Background(), signal2, ago.CorrelationKey.Field("corr-2"))
	c.Emit(context.Background(), signal1, ago.CorrelationKey.Field("corr-3"))

	c.Shutdown()

	if tracker.Count() != 3 {
		t.Errorf("expected 3 tracked signals, got %d", tracker.Count())
	}

	signals := tracker.Signals()
	if signals[0].CorrelationID != "corr-1" {
		t.Errorf("expected 'corr-1', got %q", signals[0].CorrelationID)
	}
}

func TestWaitForSignals(t *testing.T) {
	tracker := NewSignalTracker()
	signal := capitan.NewSignal("test.signal", "Test signal")

	// Add signals in background
	go func() {
		time.Sleep(10 * time.Millisecond)
		tracker.Track(signal, "")
		tracker.Track(signal, "")
	}()

	result := WaitForSignals(tracker, 2, 100*time.Millisecond)
	if !result {
		t.Error("expected WaitForSignals to return true")
	}
}

func TestWaitForSignals_Timeout(t *testing.T) {
	tracker := NewSignalTracker()

	result := WaitForSignals(tracker, 5, 10*time.Millisecond)
	if result {
		t.Error("expected WaitForSignals to return false on timeout")
	}
}

func TestAssertSignalOrder(t *testing.T) {
	tracker := NewSignalTracker()
	signal1 := capitan.NewSignal("first", "First")
	signal2 := capitan.NewSignal("second", "Second")

	tracker.Track(signal1, "")
	tracker.Track(signal2, "")

	// This shouldn't fail
	mockT := &testing.T{}
	AssertSignalOrder(mockT, tracker, "first", "second")
	if mockT.Failed() {
		t.Error("expected AssertSignalOrder to pass")
	}
}

func TestAssertSignalCount(t *testing.T) {
	tracker := NewSignalTracker()
	signal := capitan.NewSignal("test", "Test")

	tracker.Track(signal, "")
	tracker.Track(signal, "")
	tracker.Track(signal, "")

	// This shouldn't fail
	mockT := &testing.T{}
	AssertSignalCount(mockT, tracker, "test", 3)
	if mockT.Failed() {
		t.Error("expected AssertSignalCount to pass")
	}
}

func TestTrackedSignal_Timestamp(t *testing.T) {
	tracker := NewSignalTracker()
	signal := capitan.NewSignal("test", "Test")

	before := time.Now()
	tracker.Track(signal, "")
	after := time.Now()

	signals := tracker.Signals()
	if signals[0].Timestamp.Before(before) || signals[0].Timestamp.After(after) {
		t.Error("expected timestamp to be between before and after")
	}
}
