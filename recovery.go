package ago

import (
	"context"
	"encoding/json"
	"time"

	"github.com/zoobz-io/capitan"
)

// errSagaTimeoutExceeded is the error message for expired sagas.
const errSagaTimeoutExceeded = "saga timeout exceeded"

// RecoverSagas finds incomplete sagas and runs their compensations.
// This includes:
// - Sagas left in "running" or "compensating" state (from crashes)
// - Sagas that have exceeded their timeout
//
// Call this at startup to recover from crashes or restarts.
func RecoverSagas[T any](ctx context.Context, store Store, key capitan.GenericKey[T], c *capitan.Capitan) error {
	sagas, err := store.ListIncompleteSagas(ctx)
	if err != nil {
		return err
	}

	emitFn := capitan.Emit
	if c != nil {
		emitFn = c.Emit
	}

	for _, saga := range sagas {
		// Set error message for expired sagas
		if saga.IsExpired() && saga.Error == "" {
			saga.Error = errSagaTimeoutExceeded
		}

		if recoverErr := recoverSaga(ctx, store, saga, key, emitFn); recoverErr != nil {
			// Log but continue with other sagas
			emitFn(ctx, SagaFailed,
				CorrelationKey.Field(saga.CorrelationID),
				ErrorKey.Field(recoverErr),
			)
		}
	}

	return nil
}

func recoverSaga[T any](
	ctx context.Context,
	store Store,
	saga *SagaState,
	key capitan.GenericKey[T],
	emitFn func(context.Context, capitan.Signal, ...capitan.Field),
) error {
	// Mark as compensating
	saga.Status = SagaStatusCompensating
	saga.UpdatedAt = time.Now()
	if err := store.UpdateSaga(ctx, saga.CorrelationID, saga); err != nil {
		return err
	}

	emitFn(ctx, SagaCompensating, CorrelationKey.Field(saga.CorrelationID))

	// Execute compensations in reverse order
	for i := len(saga.Compensations) - 1; i >= 0; i-- {
		comp := saga.Compensations[i]

		var payload T
		if unmarshalErr := json.Unmarshal(comp.Data, &payload); unmarshalErr != nil {
			saga.Status = SagaStatusFailed
			saga.Error = unmarshalErr.Error()
			saga.UpdatedAt = time.Now()
			//nolint:errcheck // Best effort cleanup; unmarshalErr is the primary error
			store.UpdateSaga(ctx, saga.CorrelationID, saga)
			return unmarshalErr
		}

		// Emit compensation signal
		emitFn(ctx, comp.Signal,
			key.Field(payload),
			CorrelationKey.Field(saga.CorrelationID),
			StepNameKey.Field(comp.StepName),
		)
	}

	// Mark as failed (saga failed but compensation completed)
	saga.Status = SagaStatusFailed
	saga.UpdatedAt = time.Now()
	if err := store.UpdateSaga(ctx, saga.CorrelationID, saga); err != nil {
		return err
	}

	emitFn(ctx, SagaCompleted, CorrelationKey.Field(saga.CorrelationID))

	return nil
}
