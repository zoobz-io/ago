package ago

import (
	"context"

	"github.com/zoobz-io/capitan"
	"github.com/zoobz-io/pipz"
)

// Emit emits a capitan signal with fields derived from the flow.
type Emit[T any] struct {
	name    pipz.Name
	capitan *capitan.Capitan
	signal  capitan.Signal
	key     capitan.GenericKey[T]
}

// NewEmit creates an emit primitive.
func NewEmit[T any](name pipz.Name, signal capitan.Signal, key capitan.GenericKey[T]) *Emit[T] {
	return &Emit[T]{
		name:   name,
		signal: signal,
		key:    key,
	}
}

// WithCapitan sets a custom capitan instance. Defaults to global.
func (e *Emit[T]) WithCapitan(c *capitan.Capitan) *Emit[T] {
	e.capitan = c
	return e
}

// Build creates the chainable processor.
func (e *Emit[T]) Build() pipz.Chainable[*Flow[T]] {
	return pipz.Effect(e.name, func(ctx context.Context, f *Flow[T]) error {
		fields := []capitan.Field{e.key.Field(f.Payload)}
		fields = append(fields, f.Fields()...)
		if f.CorrelationID != "" {
			fields = append(fields, CorrelationKey.Field(f.CorrelationID))
		}
		if f.CausationID != "" {
			fields = append(fields, CausationKey.Field(f.CausationID))
		}

		if e.capitan != nil {
			e.capitan.Emit(ctx, e.signal, fields...)
		} else {
			capitan.Emit(ctx, e.signal, fields...)
		}
		return nil
	})
}

// Name returns the processor name.
func (e *Emit[T]) Name() pipz.Name {
	return e.name
}

// Process implements Chainable.
func (e *Emit[T]) Process(ctx context.Context, f *Flow[T]) (*Flow[T], error) {
	return e.Build().Process(ctx, f)
}

// Close implements Chainable.
func (*Emit[T]) Close() error {
	return nil
}
