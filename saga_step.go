package ago

import (
	"context"
	"encoding/json"
	"time"

	"github.com/zoobz-io/capitan"
	"github.com/zoobz-io/pipz"
)

// SagaStep executes a saga step with compensation registration.
type SagaStep[T any] struct {
	name       pipz.Name
	store      Store
	capitan    *capitan.Capitan
	execute    capitan.Signal
	compensate capitan.Signal
	key        capitan.GenericKey[T]
	timeout    time.Duration
}

// NewSagaStep creates a saga step. Store is required for saga state
// persistence and idempotency tracking.
func NewSagaStep[T any](
	name pipz.Name,
	store Store,
	key capitan.GenericKey[T],
	execute capitan.Signal,
	compensate capitan.Signal,
) *SagaStep[T] {
	return &SagaStep[T]{
		name:       name,
		store:      store,
		key:        key,
		execute:    execute,
		compensate: compensate,
	}
}

// WithCapitan sets a custom capitan instance. Defaults to global.
func (s *SagaStep[T]) WithCapitan(c *capitan.Capitan) *SagaStep[T] {
	s.capitan = c
	return s
}

// WithTimeout sets the saga timeout. If the saga runs longer than this duration,
// RecoverSagas will trigger compensation. Zero means no timeout.
// Note: This only affects saga creation - if the saga already exists, timeout is unchanged.
func (s *SagaStep[T]) WithTimeout(d time.Duration) *SagaStep[T] {
	s.timeout = d
	return s
}

// Build creates the chainable processor.
func (s *SagaStep[T]) Build() pipz.Chainable[*Flow[T]] {
	return pipz.Apply(s.name, func(ctx context.Context, f *Flow[T]) (*Flow[T], error) {
		stepName := string(s.name)

		// Serialize payload for compensation (done outside WithSaga to avoid
		// holding lock during serialization)
		data, err := json.Marshal(f.Payload)
		if err != nil {
			return f, err
		}

		// Track whether we should emit (set inside WithSaga callback)
		shouldEmit := false

		// Use WithSaga for exclusive access to saga state
		err = s.store.WithSaga(ctx, f.CorrelationID, func(state *SagaState) (*SagaState, error) {
			// Create new saga if doesn't exist
			if state == nil {
				state = &SagaState{
					CorrelationID: f.CorrelationID,
					Status:        SagaStatusRunning,
					CurrentStep:   0,
					Compensations: []CompensationRecord{},
					CreatedAt:     time.Now(),
					UpdatedAt:     time.Now(),
					Timeout:       s.timeout,
				}
			}

			// Check if saga is compensating - don't execute new steps
			if state.Status == SagaStatusCompensating || state.Status == SagaStatusFailed {
				return nil, nil // No changes, saga is rolling back
			}

			// Check idempotency via compensation record - if compensation is registered,
			// the signal was already emitted.
			for _, comp := range state.Compensations {
				if comp.StepName == stepName {
					// Already executed, skip
					return nil, nil // No changes needed
				}
			}

			// Push compensation record - this is our idempotency marker
			state.Compensations = append(state.Compensations, CompensationRecord{
				StepName: stepName,
				Signal:   s.compensate,
				Data:     data,
			})
			state.CurrentStep++
			state.UpdatedAt = time.Now()

			shouldEmit = true
			return state, nil
		})
		if err != nil {
			return f, err
		}

		// Only emit if we successfully registered the step
		if !shouldEmit {
			return f, nil
		}

		// Generate deterministic idempotency key for downstream handlers
		idempotencyKey := f.CorrelationID + ":" + stepName

		// Emit execute signal with idempotency key
		fields := []capitan.Field{s.key.Field(f.Payload)}
		fields = append(fields, f.Fields()...)
		if f.CorrelationID != "" {
			fields = append(fields, CorrelationKey.Field(f.CorrelationID))
		}
		fields = append(fields, IdempotencyKey.Field(idempotencyKey))

		if s.capitan != nil {
			s.capitan.Emit(ctx, s.execute, fields...)
		} else {
			capitan.Emit(ctx, s.execute, fields...)
		}

		// Emit step completed signal
		if s.capitan != nil {
			s.capitan.Emit(ctx, SagaStepCompleted,
				CorrelationKey.Field(f.CorrelationID),
				StepNameKey.Field(stepName),
			)
		} else {
			capitan.Emit(ctx, SagaStepCompleted,
				CorrelationKey.Field(f.CorrelationID),
				StepNameKey.Field(stepName),
			)
		}

		return f, nil
	})
}

// Name returns the processor name.
func (s *SagaStep[T]) Name() pipz.Name {
	return s.name
}

// Process implements Chainable by delegating to Build().
func (s *SagaStep[T]) Process(ctx context.Context, f *Flow[T]) (*Flow[T], error) {
	return s.Build().Process(ctx, f)
}

// Close implements Chainable.
func (*SagaStep[T]) Close() error {
	return nil
}
