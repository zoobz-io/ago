// Package testing provides test helpers for ago.
package testing

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zoobz-io/ago"
	"github.com/zoobz-io/capitan"
)

// MockStore wraps MemoryStore with tracking and failure injection.
type MockStore struct {
	*ago.MemoryStore

	mu               sync.RWMutex
	getSagaCalls     int64
	setSagaCalls     int64
	updateSagaCalls  int64
	compensatedCalls int64
	withSagaCalls    int64

	failGetSaga         bool
	failSetSaga         bool
	failUpdateSaga      bool
	failMarkCompensated bool
	failWithSaga        bool
}

// NewMockStore creates a mock store for testing.
func NewMockStore() *MockStore {
	return &MockStore{
		MemoryStore: ago.NewMemoryStore(),
	}
}

// GetSaga wraps with tracking and optional failure.
func (m *MockStore) GetSaga(ctx context.Context, correlationID string) (*ago.SagaState, error) {
	atomic.AddInt64(&m.getSagaCalls, 1)
	m.mu.RLock()
	fail := m.failGetSaga
	m.mu.RUnlock()
	if fail {
		return nil, ago.ErrNotFound
	}
	return m.MemoryStore.GetSaga(ctx, correlationID)
}

// SetSaga wraps with tracking and optional failure.
func (m *MockStore) SetSaga(ctx context.Context, correlationID string, state *ago.SagaState) error {
	atomic.AddInt64(&m.setSagaCalls, 1)
	m.mu.RLock()
	fail := m.failSetSaga
	m.mu.RUnlock()
	if fail {
		return ago.ErrNotFound
	}
	return m.MemoryStore.SetSaga(ctx, correlationID, state)
}

// UpdateSaga wraps with tracking and optional failure.
func (m *MockStore) UpdateSaga(ctx context.Context, correlationID string, state *ago.SagaState) error {
	atomic.AddInt64(&m.updateSagaCalls, 1)
	m.mu.RLock()
	fail := m.failUpdateSaga
	m.mu.RUnlock()
	if fail {
		return ago.ErrNotFound
	}
	return m.MemoryStore.UpdateSaga(ctx, correlationID, state)
}

// MarkCompensated wraps with tracking and optional failure.
func (m *MockStore) MarkCompensated(ctx context.Context, correlationID, stepName string) error {
	atomic.AddInt64(&m.compensatedCalls, 1)
	m.mu.RLock()
	fail := m.failMarkCompensated
	m.mu.RUnlock()
	if fail {
		return ago.ErrNotFound
	}
	return m.MemoryStore.MarkCompensated(ctx, correlationID, stepName)
}

// WithSaga wraps with tracking and optional failure.
func (m *MockStore) WithSaga(ctx context.Context, correlationID string, fn func(*ago.SagaState) (*ago.SagaState, error)) error {
	atomic.AddInt64(&m.withSagaCalls, 1)
	m.mu.RLock()
	fail := m.failWithSaga
	m.mu.RUnlock()
	if fail {
		return ago.ErrNotFound
	}
	return m.MemoryStore.WithSaga(ctx, correlationID, fn)
}

// FailGetSaga causes GetSaga to return an error.
func (m *MockStore) FailGetSaga(fail bool) {
	m.mu.Lock()
	m.failGetSaga = fail
	m.mu.Unlock()
}

// FailSetSaga causes SetSaga to return an error.
func (m *MockStore) FailSetSaga(fail bool) {
	m.mu.Lock()
	m.failSetSaga = fail
	m.mu.Unlock()
}

// FailUpdateSaga causes UpdateSaga to return an error.
func (m *MockStore) FailUpdateSaga(fail bool) {
	m.mu.Lock()
	m.failUpdateSaga = fail
	m.mu.Unlock()
}

// FailMarkCompensated causes MarkCompensated to return an error.
func (m *MockStore) FailMarkCompensated(fail bool) {
	m.mu.Lock()
	m.failMarkCompensated = fail
	m.mu.Unlock()
}

// FailWithSaga causes WithSaga to return an error.
func (m *MockStore) FailWithSaga(fail bool) {
	m.mu.Lock()
	m.failWithSaga = fail
	m.mu.Unlock()
}

// GetSagaCalls returns the number of GetSaga calls.
func (m *MockStore) GetSagaCalls() int64 {
	return atomic.LoadInt64(&m.getSagaCalls)
}

// SetSagaCalls returns the number of SetSaga calls.
func (m *MockStore) SetSagaCalls() int64 {
	return atomic.LoadInt64(&m.setSagaCalls)
}

// UpdateSagaCalls returns the number of UpdateSaga calls.
func (m *MockStore) UpdateSagaCalls() int64 {
	return atomic.LoadInt64(&m.updateSagaCalls)
}

// CompensatedCalls returns the number of MarkCompensated calls.
func (m *MockStore) CompensatedCalls() int64 {
	return atomic.LoadInt64(&m.compensatedCalls)
}

// WithSagaCalls returns the number of WithSaga calls.
func (m *MockStore) WithSagaCalls() int64 {
	return atomic.LoadInt64(&m.withSagaCalls)
}

// Reset clears all tracking state.
func (m *MockStore) Reset() {
	atomic.StoreInt64(&m.getSagaCalls, 0)
	atomic.StoreInt64(&m.setSagaCalls, 0)
	atomic.StoreInt64(&m.updateSagaCalls, 0)
	atomic.StoreInt64(&m.compensatedCalls, 0)
	atomic.StoreInt64(&m.withSagaCalls, 0)
	m.mu.Lock()
	m.failGetSaga = false
	m.failSetSaga = false
	m.failUpdateSaga = false
	m.failMarkCompensated = false
	m.failWithSaga = false
	m.mu.Unlock()
}

// SignalTracker tracks emitted signals for verification.
type SignalTracker struct {
	mu      sync.RWMutex
	signals []TrackedSignal
}

// TrackedSignal records a signal emission.
type TrackedSignal struct {
	Signal        capitan.Signal
	CorrelationID string
	Timestamp     time.Time
	Fields        map[string]interface{}
}

// NewSignalTracker creates a new signal tracker.
func NewSignalTracker() *SignalTracker {
	return &SignalTracker{
		signals: make([]TrackedSignal, 0),
	}
}

// Track records a signal emission.
func (s *SignalTracker) Track(signal capitan.Signal, correlationID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.signals = append(s.signals, TrackedSignal{
		Signal:        signal,
		CorrelationID: correlationID,
		Timestamp:     time.Now(),
	})
}

// Signals returns all tracked signals.
func (s *SignalTracker) Signals() []TrackedSignal {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]TrackedSignal, len(s.signals))
	copy(result, s.signals)
	return result
}

// SignalNames returns the names of all tracked signals in order.
func (s *SignalTracker) SignalNames() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, len(s.signals))
	for i, sig := range s.signals {
		names[i] = sig.Signal.Name()
	}
	return names
}

// Count returns the number of tracked signals.
func (s *SignalTracker) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.signals)
}

// CountSignal returns how many times a specific signal was emitted.
func (s *SignalTracker) CountSignal(name string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, sig := range s.signals {
		if sig.Signal.Name() == name {
			count++
		}
	}
	return count
}

// Reset clears all tracked signals.
func (s *SignalTracker) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.signals = s.signals[:0]
}

// HookTracker attaches to a capitan instance and tracks signals.
func HookTracker(c *capitan.Capitan, tracker *SignalTracker, signals ...capitan.Signal) {
	for _, signal := range signals {
		sig := signal // capture
		c.Hook(sig, func(_ context.Context, e *capitan.Event) {
			corrID, _ := ago.CorrelationKey.From(e)
			tracker.Track(sig, corrID)
		})
	}
}

// WaitForSignals waits until the tracker has at least n signals or timeout.
func WaitForSignals(tracker *SignalTracker, n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if tracker.Count() >= n {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return tracker.Count() >= n
}

// AssertSignalOrder verifies signals were emitted in the expected order.
func AssertSignalOrder(t *testing.T, tracker *SignalTracker, expected ...string) {
	t.Helper()
	names := tracker.SignalNames()
	if len(names) != len(expected) {
		t.Errorf("expected %d signals, got %d: %v", len(expected), len(names), names)
		return
	}
	for i, name := range expected {
		if names[i] != name {
			t.Errorf("signal %d: expected %q, got %q", i, name, names[i])
		}
	}
}

// AssertSignalCount verifies a specific signal was emitted n times.
func AssertSignalCount(t *testing.T, tracker *SignalTracker, signalName string, expected int) {
	t.Helper()
	count := tracker.CountSignal(signalName)
	if count != expected {
		t.Errorf("expected signal %q emitted %d times, got %d", signalName, expected, count)
	}
}
