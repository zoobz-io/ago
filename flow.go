// Package ago provides event-driven pattern primitives for pipz.
//
// ago ("I do" in Latin) bridges capitan events with pipz pipelines,
// enabling distributed sagas, request/response patterns, and stateful
// coordination across processes.
//
// Flow[T] wraps a typed payload with correlation context and accumulated
// state. All primitives implement pipz.Chainable[*Flow[T]], composable
// via pipz topology or flume schema configuration.
package ago

import (
	"sync"
	"time"

	"github.com/zoobz-io/capitan"
)

// Flow wraps a typed payload with correlation context and accumulated state.
// T is the business payload type.
type Flow[T any] struct {
	// Payload is the typed business data.
	Payload T

	// Origin metadata captured at creation.
	Signal    capitan.Signal
	Timestamp time.Time
	Severity  capitan.Severity

	// Correlation for saga and request/response patterns.
	CorrelationID string
	CausationID   string

	// Broker metadata for herald integration.
	Metadata map[string]string

	// Errors accumulated during processing.
	Errors []error

	// Type-safe accumulated state via capitan.Field pattern.
	fields map[string]capitan.Field

	mu sync.RWMutex
}

// NewFlow creates a Flow with the given payload and signal.
func NewFlow[T any](payload T, signal capitan.Signal) *Flow[T] {
	return &Flow[T]{
		Payload:   payload,
		Signal:    signal,
		Timestamp: time.Now(),
		Severity:  capitan.SeverityInfo,
		Metadata:  make(map[string]string),
		fields:    make(map[string]capitan.Field),
	}
}

// NewFromEvent creates a Flow from a capitan Event using a typed key.
// Returns nil if the key is not present in the event.
func NewFromEvent[T any](e *capitan.Event, key capitan.GenericKey[T]) *Flow[T] {
	payload, ok := key.From(e)
	if !ok {
		return nil
	}
	return &Flow[T]{
		Payload:   payload,
		Signal:    e.Signal(),
		Timestamp: e.Timestamp(),
		Severity:  e.Severity(),
		Metadata:  make(map[string]string),
		fields:    make(map[string]capitan.Field),
	}
}

// Set adds or updates a typed field in the flow's accumulated state.
func (f *Flow[T]) Set(field capitan.Field) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fields[field.Key().Name()] = field
}

// Get retrieves a field by key, returning nil if not present.
func (f *Flow[T]) Get(key capitan.Key) capitan.Field {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.fields[key.Name()]
}

// From extracts a typed value from the flow's accumulated state.
// Returns the value and true if present, or zero value and false otherwise.
func From[T, V any](f *Flow[T], key capitan.GenericKey[V]) (V, bool) {
	var zero V
	field := f.Get(key)
	if field == nil {
		return zero, false
	}
	if gf, ok := field.(capitan.GenericField[V]); ok {
		return gf.Get(), true
	}
	return zero, false
}

// Fields returns all accumulated fields as a slice.
func (f *Flow[T]) Fields() []capitan.Field {
	f.mu.RLock()
	defer f.mu.RUnlock()
	result := make([]capitan.Field, 0, len(f.fields))
	for _, field := range f.fields {
		result = append(result, field)
	}
	return result
}

// AddError appends an error to the flow's error accumulator.
func (f *Flow[T]) AddError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Errors = append(f.Errors, err)
}

// HasErrors returns true if any errors have been accumulated.
func (f *Flow[T]) HasErrors() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.Errors) > 0
}

// Clone creates a deep copy of the Flow for parallel processing.
// Implements pipz.Cloner[*Flow[T]].
func (f *Flow[T]) Clone() *Flow[T] {
	f.mu.RLock()
	defer f.mu.RUnlock()

	clone := &Flow[T]{
		Payload:       f.Payload,
		Signal:        f.Signal,
		Timestamp:     f.Timestamp,
		Severity:      f.Severity,
		CorrelationID: f.CorrelationID,
		CausationID:   f.CausationID,
		Metadata:      make(map[string]string, len(f.Metadata)),
		Errors:        make([]error, len(f.Errors)),
		fields:        make(map[string]capitan.Field, len(f.fields)),
	}

	for k, v := range f.Metadata {
		clone.Metadata[k] = v
	}
	copy(clone.Errors, f.Errors)
	for k, v := range f.fields {
		clone.fields[k] = v
	}

	return clone
}
