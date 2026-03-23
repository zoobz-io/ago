package ago

import (
	"context"

	"github.com/google/uuid"
	"github.com/zoobz-io/pipz"
)

// Correlate ensures CorrelationID exists on the flow, generating one if missing.
func Correlate[T any](name pipz.Name) pipz.Chainable[*Flow[T]] {
	return pipz.Transform(name, func(_ context.Context, f *Flow[T]) *Flow[T] {
		if f.CorrelationID == "" {
			f.CorrelationID = uuid.New().String()
		}
		return f
	})
}

// CorrelateFrom sets both CorrelationID and CausationID from a parent.
// If the flow has no correlation, a new one is generated.
func CorrelateFrom[T any](name pipz.Name, parentCorrelation string) pipz.Chainable[*Flow[T]] {
	return pipz.Transform(name, func(_ context.Context, f *Flow[T]) *Flow[T] {
		if f.CorrelationID == "" {
			f.CorrelationID = uuid.New().String()
		}
		if parentCorrelation != "" {
			f.CausationID = parentCorrelation
		}
		return f
	})
}
