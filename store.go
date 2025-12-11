package ago

import (
	"context"
	"time"

	"github.com/zoobzio/capitan"
)

// Store provides persistence for coordination and saga state.
// Implementations enable distributed coordination and restart recovery.
type Store interface {
	// Pending request/await state.
	SetPending(ctx context.Context, correlationID string, state *PendingState) error
	GetPending(ctx context.Context, correlationID string) (*PendingState, error)
	DeletePending(ctx context.Context, correlationID string) error

	// Saga state.
	SetSaga(ctx context.Context, correlationID string, state *SagaState) error
	GetSaga(ctx context.Context, correlationID string) (*SagaState, error)
	UpdateSaga(ctx context.Context, correlationID string, state *SagaState) error
	DeleteSaga(ctx context.Context, correlationID string) error
	ListIncompleteSagas(ctx context.Context) ([]*SagaState, error)

	// WithSaga executes a callback with exclusive access to a saga's state.
	// If the saga doesn't exist, callback receives nil and can return a new state to create it.
	// If callback returns a non-nil state, it is saved. If callback returns an error,
	// no changes are persisted.
	// Implementations must ensure the callback has exclusive access (mutex, transaction, etc.).
	//
	// NOTE: Signal emission typically happens AFTER WithSaga returns, outside the lock.
	// This means a crash between state commit and signal emission could leave state
	// updated but signal not emitted. This is acceptable because:
	// - Idempotency keys allow safe retry
	// - At-least-once delivery is the expected semantic
	// - Holding locks during signal emission would risk deadlocks
	WithSaga(ctx context.Context, correlationID string, fn func(*SagaState) (*SagaState, error)) error

	// Idempotency for compensation actions.
	MarkCompensated(ctx context.Context, correlationID, stepName string) error
	IsCompensated(ctx context.Context, correlationID, stepName string) (bool, error)
}

// PendingState represents a request or await waiting for a response.
type PendingState struct {
	CorrelationID string
	Signal        capitan.Signal
	CreatedAt     time.Time
	Timeout       time.Duration
}

// SagaStatus represents the lifecycle state of a saga.
type SagaStatus string

const (
	// SagaStatusPending indicates the saga has been created but not started.
	SagaStatusPending SagaStatus = "pending"
	// SagaStatusRunning indicates the saga is actively executing steps.
	SagaStatusRunning SagaStatus = "running"
	// SagaStatusCompensating indicates the saga is rolling back via compensation.
	SagaStatusCompensating SagaStatus = "compensating"
	// SagaStatusCompleted indicates the saga finished successfully.
	SagaStatusCompleted SagaStatus = "completed"
	// SagaStatusFailed indicates the saga failed and compensation is complete.
	SagaStatusFailed SagaStatus = "failed"
)

// SagaState tracks a saga's execution and compensation stack.
type SagaState struct {
	CorrelationID string
	Status        SagaStatus
	CurrentStep   int
	Compensations []CompensationRecord
	CreatedAt     time.Time
	UpdatedAt     time.Time
	Error         string
	// Timeout specifies how long the saga may run before being considered expired.
	// Zero means no timeout. RecoverSagas will compensate expired sagas.
	Timeout time.Duration
}

// IsExpired returns true if the saga has a timeout and has exceeded it.
func (s *SagaState) IsExpired() bool {
	if s.Timeout <= 0 {
		return false
	}
	return time.Since(s.CreatedAt) > s.Timeout
}

// CompensationRecord stores data needed to execute a compensation action.
type CompensationRecord struct {
	StepName string
	Signal   capitan.Signal
	Data     []byte
}
