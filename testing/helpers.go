// Package testing provides test utilities for ago consumers.
package testing

import (
	"context"
	"sync"
	"testing"

	"github.com/zoobz-io/ago"
	"github.com/zoobz-io/capitan"
)

// SignalTracker records capitan signals for assertion in tests.
type SignalTracker struct {
	mu      sync.Mutex
	signals []TrackedSignal
}

// TrackedSignal records a single signal emission.
type TrackedSignal struct {
	Signal capitan.Signal
	Event  *capitan.Event
}

// NewSignalTracker creates a tracker and hooks the given signals
// on the provided capitan instance.
func NewSignalTracker(c *capitan.Capitan, signals ...capitan.Signal) *SignalTracker {
	st := &SignalTracker{}
	for _, sig := range signals {
		s := sig // capture
		c.Hook(s, func(_ context.Context, e *capitan.Event) {
			st.mu.Lock()
			defer st.mu.Unlock()
			st.signals = append(st.signals, TrackedSignal{Signal: s, Event: e.Clone()})
		})
	}
	return st
}

// Signals returns a copy of all tracked signals.
func (st *SignalTracker) Signals() []TrackedSignal {
	st.mu.Lock()
	defer st.mu.Unlock()
	result := make([]TrackedSignal, len(st.signals))
	copy(result, st.signals)
	return result
}

// Count returns the number of tracked signals.
func (st *SignalTracker) Count() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.signals)
}

// CountSignal returns how many times a specific signal was emitted.
func (st *SignalTracker) CountSignal(signal capitan.Signal) int {
	st.mu.Lock()
	defer st.mu.Unlock()
	count := 0
	for _, s := range st.signals {
		if s.Signal.Name() == signal.Name() {
			count++
		}
	}
	return count
}

// AssertSignalCount checks that the tracker has exactly n signals.
func AssertSignalCount(t *testing.T, st *SignalTracker, expected int) {
	t.Helper()
	if got := st.Count(); got != expected {
		t.Errorf("expected %d signals, got %d", expected, got)
	}
}

// AssertSignalOrder checks that signals were emitted in the expected order.
func AssertSignalOrder(t *testing.T, st *SignalTracker, expected ...capitan.Signal) {
	t.Helper()
	signals := st.Signals()
	if len(signals) < len(expected) {
		t.Fatalf("expected at least %d signals, got %d", len(expected), len(signals))
	}
	for i, exp := range expected {
		if signals[i].Signal.Name() != exp.Name() {
			t.Errorf("signal %d: expected %q, got %q", i, exp.Name(), signals[i].Signal.Name())
		}
	}
}

// InvokeJSON is a test helper that invokes a tool with a JSON string.
func InvokeJSON(t *testing.T, r *ago.Registry, toolName, jsonInput string) (*ago.Result, error) {
	t.Helper()
	return r.Invoke(context.Background(), toolName, []byte(jsonInput), ago.NoIdentity{})
}

// NewTestRegistry creates a registry with sync-mode capitan for deterministic testing.
// Returns both the registry and the capitan instance.
func NewTestRegistry() (*ago.Registry, *capitan.Capitan) {
	c := capitan.New(capitan.WithSyncMode())
	r := ago.NewRegistry().WithCapitan(c)
	return r, c
}
