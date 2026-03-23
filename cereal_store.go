package ago

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/zoobz-io/capitan"
)

// errNoRows is the error string returned by database/sql for no rows.
const errNoRows = "sql: no rows in result set"

// CerealStore implements Store using PostgreSQL via cereal patterns.
// Requires tables: ago_pending_states, ago_saga_states.
type CerealStore struct {
	db *sqlx.DB
}

// NewCerealStore creates a Store backed by PostgreSQL.
func NewCerealStore(db *sqlx.DB) *CerealStore {
	return &CerealStore{db: db}
}

// Migrate creates the required tables if they don't exist.
func (s *CerealStore) Migrate(ctx context.Context) error {
	schema := `
		CREATE TABLE IF NOT EXISTS ago_pending_states (
			correlation_id TEXT PRIMARY KEY,
			signal_name TEXT NOT NULL,
			signal_description TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL,
			timeout_ms BIGINT NOT NULL
		);

		CREATE TABLE IF NOT EXISTS ago_saga_states (
			correlation_id TEXT PRIMARY KEY,
			status TEXT NOT NULL,
			current_step INTEGER NOT NULL,
			compensations JSONB NOT NULL DEFAULT '[]',
			created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL,
			error TEXT,
			timeout_ms BIGINT NOT NULL DEFAULT 0
		);

		CREATE TABLE IF NOT EXISTS ago_compensated_steps (
			correlation_id TEXT NOT NULL,
			step_name TEXT NOT NULL,
			compensated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (correlation_id, step_name)
		);

		CREATE INDEX IF NOT EXISTS idx_ago_saga_states_status ON ago_saga_states(status);
	`
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

// SetPending stores a pending state.
func (s *CerealStore) SetPending(ctx context.Context, correlationID string, state *PendingState) error {
	query := `
		INSERT INTO ago_pending_states (correlation_id, signal_name, signal_description, created_at, timeout_ms)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (correlation_id) DO UPDATE SET
			signal_name = EXCLUDED.signal_name,
			signal_description = EXCLUDED.signal_description,
			created_at = EXCLUDED.created_at,
			timeout_ms = EXCLUDED.timeout_ms
	`
	_, err := s.db.ExecContext(ctx, query,
		correlationID,
		state.Signal.Name(),
		state.Signal.Description(),
		state.CreatedAt,
		state.Timeout.Milliseconds(),
	)
	return err
}

// GetPending retrieves a pending state.
func (s *CerealStore) GetPending(ctx context.Context, correlationID string) (*PendingState, error) {
	query := `
		SELECT correlation_id, signal_name, signal_description, created_at, timeout_ms
		FROM ago_pending_states
		WHERE correlation_id = $1
	`
	var row struct {
		CorrelationID     string    `db:"correlation_id"`
		SignalName        string    `db:"signal_name"`
		SignalDescription string    `db:"signal_description"`
		CreatedAt         time.Time `db:"created_at"`
		TimeoutMs         int64     `db:"timeout_ms"`
	}
	err := s.db.GetContext(ctx, &row, query, correlationID)
	if err != nil {
		if err.Error() == errNoRows {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return &PendingState{
		CorrelationID: row.CorrelationID,
		Signal:        capitan.NewSignal(row.SignalName, row.SignalDescription),
		CreatedAt:     row.CreatedAt,
		Timeout:       time.Duration(row.TimeoutMs) * time.Millisecond,
	}, nil
}

// DeletePending removes a pending state.
func (s *CerealStore) DeletePending(ctx context.Context, correlationID string) error {
	query := `DELETE FROM ago_pending_states WHERE correlation_id = $1`
	_, err := s.db.ExecContext(ctx, query, correlationID)
	return err
}

// SetSaga stores a new saga state.
func (s *CerealStore) SetSaga(ctx context.Context, correlationID string, state *SagaState) error {
	compensations, err := json.Marshal(serializeCompensations(state.Compensations))
	if err != nil {
		return fmt.Errorf("marshal compensations: %w", err)
	}

	query := `
		INSERT INTO ago_saga_states (correlation_id, status, current_step, compensations, created_at, updated_at, error, timeout_ms)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err = s.db.ExecContext(ctx, query,
		correlationID,
		string(state.Status),
		state.CurrentStep,
		compensations,
		state.CreatedAt,
		state.UpdatedAt,
		state.Error,
		state.Timeout.Milliseconds(),
	)
	return err
}

// GetSaga retrieves a saga state.
func (s *CerealStore) GetSaga(ctx context.Context, correlationID string) (*SagaState, error) {
	query := `
		SELECT correlation_id, status, current_step, compensations, created_at, updated_at, error, timeout_ms
		FROM ago_saga_states
		WHERE correlation_id = $1
	`
	var row struct {
		CorrelationID string          `db:"correlation_id"`
		Status        string          `db:"status"`
		CurrentStep   int             `db:"current_step"`
		Compensations json.RawMessage `db:"compensations"`
		CreatedAt     time.Time       `db:"created_at"`
		UpdatedAt     time.Time       `db:"updated_at"`
		Error         *string         `db:"error"`
		TimeoutMs     int64           `db:"timeout_ms"`
	}
	err := s.db.GetContext(ctx, &row, query, correlationID)
	if err != nil {
		if err.Error() == errNoRows {
			return nil, ErrNotFound
		}
		return nil, err
	}

	var comps []compensationJSON
	if err := json.Unmarshal(row.Compensations, &comps); err != nil {
		return nil, fmt.Errorf("unmarshal compensations: %w", err)
	}

	errStr := ""
	if row.Error != nil {
		errStr = *row.Error
	}

	return &SagaState{
		CorrelationID: row.CorrelationID,
		Status:        SagaStatus(row.Status),
		CurrentStep:   row.CurrentStep,
		Compensations: deserializeCompensations(comps),
		CreatedAt:     row.CreatedAt,
		UpdatedAt:     row.UpdatedAt,
		Error:         errStr,
		Timeout:       time.Duration(row.TimeoutMs) * time.Millisecond,
	}, nil
}

// UpdateSaga updates an existing saga state.
func (s *CerealStore) UpdateSaga(ctx context.Context, correlationID string, state *SagaState) error {
	compensations, err := json.Marshal(serializeCompensations(state.Compensations))
	if err != nil {
		return fmt.Errorf("marshal compensations: %w", err)
	}

	query := `
		UPDATE ago_saga_states
		SET status = $2, current_step = $3, compensations = $4, updated_at = $5, error = $6, timeout_ms = $7
		WHERE correlation_id = $1
	`
	result, err := s.db.ExecContext(ctx, query,
		correlationID,
		string(state.Status),
		state.CurrentStep,
		compensations,
		state.UpdatedAt,
		state.Error,
		state.Timeout.Milliseconds(),
	)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

// DeleteSaga removes a saga state.
func (s *CerealStore) DeleteSaga(ctx context.Context, correlationID string) error {
	query := `DELETE FROM ago_saga_states WHERE correlation_id = $1`
	_, err := s.db.ExecContext(ctx, query, correlationID)
	return err
}

// ListIncompleteSagas returns all sagas that are not completed or failed.
func (s *CerealStore) ListIncompleteSagas(ctx context.Context) ([]*SagaState, error) {
	query := `
		SELECT correlation_id, status, current_step, compensations, created_at, updated_at, error, timeout_ms
		FROM ago_saga_states
		WHERE status NOT IN ('completed', 'failed')
		ORDER BY created_at ASC
	`
	var rows []struct {
		CorrelationID string          `db:"correlation_id"`
		Status        string          `db:"status"`
		CurrentStep   int             `db:"current_step"`
		Compensations json.RawMessage `db:"compensations"`
		CreatedAt     time.Time       `db:"created_at"`
		UpdatedAt     time.Time       `db:"updated_at"`
		Error         *string         `db:"error"`
		TimeoutMs     int64           `db:"timeout_ms"`
	}
	err := s.db.SelectContext(ctx, &rows, query)
	if err != nil {
		return nil, err
	}

	result := make([]*SagaState, len(rows))
	for i := range rows {
		var comps []compensationJSON
		if err := json.Unmarshal(rows[i].Compensations, &comps); err != nil {
			return nil, fmt.Errorf("unmarshal compensations: %w", err)
		}

		errStr := ""
		if rows[i].Error != nil {
			errStr = *rows[i].Error
		}

		result[i] = &SagaState{
			CorrelationID: rows[i].CorrelationID,
			Status:        SagaStatus(rows[i].Status),
			CurrentStep:   rows[i].CurrentStep,
			Compensations: deserializeCompensations(comps),
			CreatedAt:     rows[i].CreatedAt,
			UpdatedAt:     rows[i].UpdatedAt,
			Error:         errStr,
			Timeout:       time.Duration(rows[i].TimeoutMs) * time.Millisecond,
		}
	}
	return result, nil
}

// compensationJSON is the JSON representation of CompensationRecord.
type compensationJSON struct {
	StepName          string `json:"step_name"`
	SignalName        string `json:"signal_name"`
	SignalDescription string `json:"signal_description"`
	Data              []byte `json:"data"`
}

func serializeCompensations(comps []CompensationRecord) []compensationJSON {
	result := make([]compensationJSON, len(comps))
	for i, c := range comps {
		result[i] = compensationJSON{
			StepName:          c.StepName,
			SignalName:        c.Signal.Name(),
			SignalDescription: c.Signal.Description(),
			Data:              c.Data,
		}
	}
	return result
}

func deserializeCompensations(comps []compensationJSON) []CompensationRecord {
	result := make([]CompensationRecord, len(comps))
	for i, c := range comps {
		result[i] = CompensationRecord{
			StepName: c.StepName,
			Signal:   capitan.NewSignal(c.SignalName, c.SignalDescription),
			Data:     c.Data,
		}
	}
	return result
}

// MarkCompensated records that a step has been compensated for idempotency.
func (s *CerealStore) MarkCompensated(ctx context.Context, correlationID, stepName string) error {
	query := `
		INSERT INTO ago_compensated_steps (correlation_id, step_name)
		VALUES ($1, $2)
		ON CONFLICT (correlation_id, step_name) DO NOTHING
	`
	_, err := s.db.ExecContext(ctx, query, correlationID, stepName)
	return err
}

// IsCompensated checks if a step has already been compensated.
func (s *CerealStore) IsCompensated(ctx context.Context, correlationID, stepName string) (bool, error) {
	query := `
		SELECT EXISTS(
			SELECT 1 FROM ago_compensated_steps
			WHERE correlation_id = $1 AND step_name = $2
		)
	`
	var exists bool
	err := s.db.GetContext(ctx, &exists, query, correlationID, stepName)
	return exists, err
}

// WithSaga executes a callback with exclusive access to a saga's state.
// Uses a transaction with SELECT FOR UPDATE to ensure exclusive access.
func (s *CerealStore) WithSaga(ctx context.Context, correlationID string, fn func(*SagaState) (*SagaState, error)) error {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	//nolint:errcheck // Rollback after Commit returns sql.ErrTxDone which is expected
	defer tx.Rollback()

	// Try to get existing state with row lock
	query := `
		SELECT correlation_id, status, current_step, compensations, created_at, updated_at, error, timeout_ms
		FROM ago_saga_states
		WHERE correlation_id = $1
		FOR UPDATE
	`
	var row struct {
		CorrelationID string          `db:"correlation_id"`
		Status        string          `db:"status"`
		CurrentStep   int             `db:"current_step"`
		Compensations json.RawMessage `db:"compensations"`
		CreatedAt     time.Time       `db:"created_at"`
		UpdatedAt     time.Time       `db:"updated_at"`
		Error         *string         `db:"error"`
		TimeoutMs     int64           `db:"timeout_ms"`
	}

	var state *SagaState
	err = tx.GetContext(ctx, &row, query, correlationID)
	if err != nil && err.Error() != errNoRows {
		return fmt.Errorf("select for update: %w", err)
	}
	if err == nil {
		var comps []compensationJSON
		if unmarshalErr := json.Unmarshal(row.Compensations, &comps); unmarshalErr != nil {
			return fmt.Errorf("unmarshal compensations: %w", unmarshalErr)
		}
		errStr := ""
		if row.Error != nil {
			errStr = *row.Error
		}
		state = &SagaState{
			CorrelationID: row.CorrelationID,
			Status:        SagaStatus(row.Status),
			CurrentStep:   row.CurrentStep,
			Compensations: deserializeCompensations(comps),
			CreatedAt:     row.CreatedAt,
			UpdatedAt:     row.UpdatedAt,
			Error:         errStr,
			Timeout:       time.Duration(row.TimeoutMs) * time.Millisecond,
		}
	}

	// Execute callback
	newState, err := fn(state)
	if err != nil {
		return err
	}

	// Save result if callback returned a state
	if newState != nil {
		compensations, err := json.Marshal(serializeCompensations(newState.Compensations))
		if err != nil {
			return fmt.Errorf("marshal compensations: %w", err)
		}

		if state == nil {
			// Insert new saga
			insertQuery := `
				INSERT INTO ago_saga_states (correlation_id, status, current_step, compensations, created_at, updated_at, error, timeout_ms)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			`
			_, err = tx.ExecContext(ctx, insertQuery,
				correlationID,
				string(newState.Status),
				newState.CurrentStep,
				compensations,
				newState.CreatedAt,
				newState.UpdatedAt,
				newState.Error,
				newState.Timeout.Milliseconds(),
			)
		} else {
			// Update existing saga
			updateQuery := `
				UPDATE ago_saga_states
				SET status = $2, current_step = $3, compensations = $4, updated_at = $5, error = $6, timeout_ms = $7
				WHERE correlation_id = $1
			`
			_, err = tx.ExecContext(ctx, updateQuery,
				correlationID,
				string(newState.Status),
				newState.CurrentStep,
				compensations,
				newState.UpdatedAt,
				newState.Error,
				newState.Timeout.Milliseconds(),
			)
		}
		if err != nil {
			return fmt.Errorf("save saga: %w", err)
		}
	}

	return tx.Commit()
}
