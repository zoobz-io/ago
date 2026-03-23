package ago

import (
	"context"
	"encoding/json"
	"time"

	"github.com/zoobz-io/capitan"
	"github.com/zoobz-io/pipz"
)

// Compensate runs the compensation stack in reverse for a saga.
type Compensate[T any] struct {
	name    pipz.Name
	store   Store
	key     capitan.GenericKey[T]
	capitan *capitan.Capitan
}

// NewCompensate creates a compensation primitive.
func NewCompensate[T any](name pipz.Name, store Store, key capitan.GenericKey[T]) *Compensate[T] {
	return &Compensate[T]{
		name:  name,
		store: store,
		key:   key,
	}
}

// WithCapitan sets a custom capitan instance. Defaults to global.
func (c *Compensate[T]) WithCapitan(cpt *capitan.Capitan) *Compensate[T] {
	c.capitan = cpt
	return c
}

// Build creates the chainable processor.
//
// Design note: Compensate uses multiple WithSaga calls rather than a single atomic operation:
//  1. Initial call: transition status to "compensating" and capture compensation records
//  2. Per-step: emit signal, then MarkCompensated (outside WithSaga - uses its own idempotency)
//  3. Final call: transition status to "failed" (compensation complete)
//
// This design allows signal emission and external idempotency tracking between state transitions.
// The initial WithSaga ensures only one caller proceeds; others see "compensating" and return early.
func (c *Compensate[T]) Build() pipz.Chainable[*Flow[T]] {
	return pipz.Apply(c.name, func(ctx context.Context, f *Flow[T]) (*Flow[T], error) {
		emitFn := capitan.Emit
		if c.capitan != nil {
			emitFn = c.capitan.Emit
		}

		// Capture compensation records and mark as compensating atomically
		var compensations []CompensationRecord
		shouldCompensate := false

		err := c.store.WithSaga(ctx, f.CorrelationID, func(state *SagaState) (*SagaState, error) {
			if state == nil {
				return nil, ErrNotFound
			}

			// Already compensating - another caller is handling this
			if state.Status == SagaStatusCompensating {
				return nil, nil // Let the first caller handle it
			}

			// Already completed compensation
			if state.Status == SagaStatusFailed {
				return nil, nil // Nothing to do
			}

			// Mark as compensating - we're the first and only handler
			state.Status = SagaStatusCompensating
			state.UpdatedAt = time.Now()
			compensations = make([]CompensationRecord, len(state.Compensations))
			copy(compensations, state.Compensations)
			shouldCompensate = true
			return state, nil
		})
		if err != nil {
			return f, err
		}

		if !shouldCompensate {
			return f, nil
		}

		// Emit compensating signal
		emitFn(ctx, SagaCompensating, CorrelationKey.Field(f.CorrelationID))

		// Execute compensations in reverse order
		for i := len(compensations) - 1; i >= 0; i-- {
			record := compensations[i]

			// Check idempotency
			compensated, checkErr := c.store.IsCompensated(ctx, f.CorrelationID, record.StepName)
			if checkErr != nil {
				return f, checkErr
			}
			if compensated {
				continue // Already compensated, skip
			}

			var payload T
			if unmarshalErr := json.Unmarshal(record.Data, &payload); unmarshalErr != nil {
				// Mark saga as failed atomically - best effort, primary error takes precedence
				//nolint:errcheck // Best effort cleanup; unmarshalErr is the primary error
				c.store.WithSaga(ctx, f.CorrelationID, func(state *SagaState) (*SagaState, error) {
					if state == nil {
						return nil, nil
					}
					state.Status = SagaStatusFailed
					state.Error = unmarshalErr.Error()
					state.UpdatedAt = time.Now()
					return state, nil
				})
				emitFn(ctx, SagaFailed,
					CorrelationKey.Field(f.CorrelationID),
					ErrorKey.Field(unmarshalErr),
				)
				return f, unmarshalErr
			}

			// Generate idempotency key for downstream handlers
			idempotencyKey := f.CorrelationID + ":compensate:" + record.StepName

			// Emit compensation signal
			emitFn(ctx, record.Signal,
				c.key.Field(payload),
				CorrelationKey.Field(f.CorrelationID),
				StepNameKey.Field(record.StepName),
				IdempotencyKey.Field(idempotencyKey),
			)

			// Mark step as compensated
			if markErr := c.store.MarkCompensated(ctx, f.CorrelationID, record.StepName); markErr != nil {
				return f, markErr
			}
		}

		// Mark as failed (compensation successful) atomically
		err = c.store.WithSaga(ctx, f.CorrelationID, func(state *SagaState) (*SagaState, error) {
			if state == nil {
				return nil, nil
			}
			state.Status = SagaStatusFailed
			state.UpdatedAt = time.Now()
			return state, nil
		})
		if err != nil {
			return f, err
		}

		emitFn(ctx, SagaCompleted, CorrelationKey.Field(f.CorrelationID))

		return f, nil
	})
}

// Name returns the processor name.
func (c *Compensate[T]) Name() pipz.Name {
	return c.name
}

// Process implements Chainable.
func (c *Compensate[T]) Process(ctx context.Context, f *Flow[T]) (*Flow[T], error) {
	return c.Build().Process(ctx, f)
}

// Close implements Chainable.
func (*Compensate[T]) Close() error {
	return nil
}
