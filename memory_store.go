package ago

import (
	"context"
	"errors"
	"sync"
)

// ErrNotFound indicates the requested state was not found.
var ErrNotFound = errors.New("ago: state not found")

// MemoryStore is an in-memory Store implementation for testing and single-instance use.
type MemoryStore struct {
	pending     map[string]*PendingState
	sagas       map[string]*SagaState
	compensated map[string]struct{} // key: "correlationID:stepName"

	mu sync.RWMutex

	// Per-saga locks for WithSaga exclusive access
	sagaLocks map[string]*sync.Mutex
	locksMu   sync.Mutex
}

// NewMemoryStore creates a new in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		pending:     make(map[string]*PendingState),
		sagas:       make(map[string]*SagaState),
		compensated: make(map[string]struct{}),
		sagaLocks:   make(map[string]*sync.Mutex),
	}
}

// getSagaLock returns the mutex for a specific saga, creating it if needed.
func (m *MemoryStore) getSagaLock(correlationID string) *sync.Mutex {
	m.locksMu.Lock()
	defer m.locksMu.Unlock()
	if m.sagaLocks[correlationID] == nil {
		m.sagaLocks[correlationID] = &sync.Mutex{}
	}
	return m.sagaLocks[correlationID]
}

// SetPending stores a pending state.
func (m *MemoryStore) SetPending(_ context.Context, correlationID string, state *PendingState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pending[correlationID] = state
	return nil
}

// GetPending retrieves a pending state.
func (m *MemoryStore) GetPending(_ context.Context, correlationID string) (*PendingState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	state, ok := m.pending[correlationID]
	if !ok {
		return nil, ErrNotFound
	}
	return state, nil
}

// DeletePending removes a pending state.
func (m *MemoryStore) DeletePending(_ context.Context, correlationID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pending, correlationID)
	return nil
}

// SetSaga stores a new saga state.
// Stores a deep copy to prevent external mutations from affecting stored state.
func (m *MemoryStore) SetSaga(_ context.Context, correlationID string, state *SagaState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sagas[correlationID] = copySagaState(state)
	return nil
}

// GetSaga retrieves a saga state.
// Returns a deep copy to prevent external mutations from affecting stored state.
func (m *MemoryStore) GetSaga(_ context.Context, correlationID string) (*SagaState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	state, ok := m.sagas[correlationID]
	if !ok {
		return nil, ErrNotFound
	}
	return copySagaState(state), nil
}

// UpdateSaga updates an existing saga state.
// Stores a deep copy to prevent external mutations from affecting stored state.
func (m *MemoryStore) UpdateSaga(_ context.Context, correlationID string, state *SagaState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sagas[correlationID]; !ok {
		return ErrNotFound
	}
	m.sagas[correlationID] = copySagaState(state)
	return nil
}

// DeleteSaga removes a saga state.
func (m *MemoryStore) DeleteSaga(_ context.Context, correlationID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sagas, correlationID)
	return nil
}

// ListIncompleteSagas returns all sagas that are not completed or failed.
// Returns deep copies to prevent external mutations from affecting stored state.
func (m *MemoryStore) ListIncompleteSagas(_ context.Context) ([]*SagaState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*SagaState
	for _, state := range m.sagas {
		if state.Status != SagaStatusCompleted && state.Status != SagaStatusFailed {
			result = append(result, copySagaState(state))
		}
	}
	return result, nil
}

// WithSaga executes a callback with exclusive access to a saga's state.
// The saga lock is held for the duration of the callback.
func (m *MemoryStore) WithSaga(_ context.Context, correlationID string, fn func(*SagaState) (*SagaState, error)) error {
	// Acquire per-saga lock for exclusive access
	lock := m.getSagaLock(correlationID)
	lock.Lock()
	defer lock.Unlock()

	// Get current state (nil if doesn't exist)
	m.mu.RLock()
	existing := m.sagas[correlationID]
	var state *SagaState
	if existing != nil {
		state = copySagaState(existing)
	}
	m.mu.RUnlock()

	// Execute callback
	newState, err := fn(state)
	if err != nil {
		return err
	}

	// Save result if callback returned a state
	if newState != nil {
		m.mu.Lock()
		m.sagas[correlationID] = copySagaState(newState)
		m.mu.Unlock()
	}

	return nil
}

// copySagaState creates a deep copy of a SagaState.
func copySagaState(s *SagaState) *SagaState {
	if s == nil {
		return nil
	}
	cp := &SagaState{
		CorrelationID: s.CorrelationID,
		Status:        s.Status,
		CurrentStep:   s.CurrentStep,
		CreatedAt:     s.CreatedAt,
		UpdatedAt:     s.UpdatedAt,
		Error:         s.Error,
		Timeout:       s.Timeout,
	}
	if len(s.Compensations) > 0 {
		cp.Compensations = make([]CompensationRecord, len(s.Compensations))
		for i, c := range s.Compensations {
			cp.Compensations[i] = CompensationRecord{
				StepName: c.StepName,
				Signal:   c.Signal,
			}
			if len(c.Data) > 0 {
				cp.Compensations[i].Data = make([]byte, len(c.Data))
				copy(cp.Compensations[i].Data, c.Data)
			}
		}
	}
	return cp
}

// MarkCompensated records that a step has been compensated for idempotency.
func (m *MemoryStore) MarkCompensated(_ context.Context, correlationID, stepName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := correlationID + ":" + stepName
	m.compensated[key] = struct{}{}
	return nil
}

// IsCompensated checks if a step has already been compensated.
func (m *MemoryStore) IsCompensated(_ context.Context, correlationID, stepName string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := correlationID + ":" + stepName
	_, ok := m.compensated[key]
	return ok, nil
}
