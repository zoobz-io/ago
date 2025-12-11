package ago

import (
	"context"
	"time"

	"github.com/zoobzio/capitan"
	"github.com/zoobzio/pipz"
)

// Await waits for a correlated event on a signal.
type Await[T, V any] struct {
	name    pipz.Name
	capitan *capitan.Capitan
	signal  capitan.Signal
	key     capitan.GenericKey[V]
	timeout time.Duration
}

// NewAwait creates an await primitive.
func NewAwait[T, V any](name pipz.Name, signal capitan.Signal, key capitan.GenericKey[V]) *Await[T, V] {
	return &Await[T, V]{
		name:    name,
		signal:  signal,
		key:     key,
		timeout: 30 * time.Second,
	}
}

// WithCapitan sets a custom capitan instance. Defaults to global.
func (a *Await[T, V]) WithCapitan(c *capitan.Capitan) *Await[T, V] {
	a.capitan = c
	return a
}

// Timeout sets the maximum wait time.
func (a *Await[T, V]) Timeout(d time.Duration) *Await[T, V] {
	a.timeout = d
	return a
}

// Build creates the chainable processor.
func (a *Await[T, V]) Build() pipz.Chainable[*Flow[T]] {
	return pipz.Apply(a.name, func(ctx context.Context, f *Flow[T]) (*Flow[T], error) {
		// Create result channel
		resultCh := make(chan V, 1)
		hookFn := func(_ context.Context, e *capitan.Event) {
			corrID, ok := CorrelationKey.From(e)
			if !ok || corrID != f.CorrelationID {
				return
			}
			value, ok := a.key.From(e)
			if ok {
				select {
				case resultCh <- value:
				default:
				}
			}
		}

		// Hook signal
		var listener *capitan.Listener
		if a.capitan != nil {
			listener = a.capitan.Hook(a.signal, hookFn)
		} else {
			listener = capitan.Hook(a.signal, hookFn)
		}
		defer listener.Close()

		// Wait for event or timeout
		timeoutCtx, cancel := context.WithTimeout(ctx, a.timeout)
		defer cancel()

		select {
		case value := <-resultCh:
			f.Set(a.key.Field(value))
			return f, nil
		case <-timeoutCtx.Done():
			return f, ErrTimeout
		}
	})
}

// Name returns the processor name.
func (a *Await[T, V]) Name() pipz.Name {
	return a.name
}

// Process implements Chainable.
func (a *Await[T, V]) Process(ctx context.Context, f *Flow[T]) (*Flow[T], error) {
	return a.Build().Process(ctx, f)
}

// Close implements Chainable.
func (*Await[T, V]) Close() error {
	return nil
}
