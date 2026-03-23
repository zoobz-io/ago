package ago

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/zoobz-io/capitan"
	"github.com/zoobz-io/herald"
	"github.com/zoobz-io/pipz"
)

// DeadLetter routes failed messages to a dead letter queue.
type DeadLetter[T any] struct {
	identity pipz.Identity
	capitan  *capitan.Capitan
	signal   capitan.Signal
	key      capitan.GenericKey[T]
	provider herald.Provider
}

// NewDeadLetter creates a dead letter primitive.
func NewDeadLetter[T any](identity pipz.Identity, key capitan.GenericKey[T]) *DeadLetter[T] {
	return &DeadLetter[T]{
		identity: identity,
		signal:   DeadLetterRouted,
		key:      key,
	}
}

// WithCapitan sets a custom capitan instance.
func (d *DeadLetter[T]) WithCapitan(c *capitan.Capitan) *DeadLetter[T] {
	d.capitan = c
	return d
}

// WithSignal sets a custom signal for dead letter events.
func (d *DeadLetter[T]) WithSignal(signal capitan.Signal) *DeadLetter[T] {
	d.signal = signal
	return d
}

// WithProvider sets a broker provider for external DLQ.
func (d *DeadLetter[T]) WithProvider(provider herald.Provider) *DeadLetter[T] {
	d.provider = provider
	return d
}

// Build creates the chainable processor.
func (d *DeadLetter[T]) Build() pipz.Chainable[*Flow[T]] {
	return pipz.Apply(d.identity, func(ctx context.Context, f *Flow[T]) (*Flow[T], error) {
		// Emit dead letter signal
		emitFn := capitan.Emit
		if d.capitan != nil {
			emitFn = d.capitan.Emit
		}

		fields := []capitan.Field{d.key.Field(f.Payload)}
		if f.CorrelationID != "" {
			fields = append(fields, CorrelationKey.Field(f.CorrelationID))
		}
		if len(f.Errors) > 0 {
			fields = append(fields, ErrorKey.Field(errors.Join(f.Errors...)))
		}
		emitFn(ctx, d.signal, fields...)

		// Publish to broker if configured
		if d.provider != nil {
			data, err := json.Marshal(f.Payload)
			if err != nil {
				return f, err
			}

			metadata := make(herald.Metadata, len(f.Metadata)+2)
			for k, v := range f.Metadata {
				metadata[k] = v
			}
			if f.CorrelationID != "" {
				metadata["correlation_id"] = f.CorrelationID
			}
			if len(f.Errors) > 0 {
				metadata["error"] = errors.Join(f.Errors...).Error()
			}

			if err := d.provider.Publish(ctx, data, metadata); err != nil {
				return f, err
			}
		}

		return f, nil
	})
}

// Identity returns the processor identity.
func (d *DeadLetter[T]) Identity() pipz.Identity {
	return d.identity
}

// Schema returns the processor schema.
func (d *DeadLetter[T]) Schema() pipz.Node {
	return pipz.Node{Identity: d.identity, Type: "dead-letter"}
}

// Process implements Chainable.
func (d *DeadLetter[T]) Process(ctx context.Context, f *Flow[T]) (*Flow[T], error) {
	return d.Build().Process(ctx, f)
}

// Close implements Chainable.
func (*DeadLetter[T]) Close() error {
	return nil
}
