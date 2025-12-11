package ago

import (
	"bytes"
	"testing"
	"time"

	"github.com/zoobzio/capitan"
)

// Note: CerealStore tests require a real PostgreSQL database.
// These tests verify the serialization/deserialization logic
// that can be tested without a database connection.

func TestSerializeCompensations(t *testing.T) {
	signal := capitan.NewSignal("test.compensate", "Test compensate")
	comps := []CompensationRecord{
		{
			StepName: "step1",
			Signal:   signal,
			Data:     []byte(`{"id": "1"}`),
		},
		{
			StepName: "step2",
			Signal:   signal,
			Data:     []byte(`{"id": "2"}`),
		},
	}

	serialized := serializeCompensations(comps)

	if len(serialized) != 2 {
		t.Fatalf("expected 2 serialized records, got %d", len(serialized))
	}

	if serialized[0].StepName != "step1" {
		t.Errorf("expected StepName 'step1', got %q", serialized[0].StepName)
	}
	if serialized[0].SignalName != "test.compensate" {
		t.Errorf("expected SignalName 'test.compensate', got %q", serialized[0].SignalName)
	}
	if string(serialized[0].Data) != `{"id": "1"}` {
		t.Errorf("expected Data '{\"id\": \"1\"}', got %q", string(serialized[0].Data))
	}

	if serialized[1].StepName != "step2" {
		t.Errorf("expected StepName 'step2', got %q", serialized[1].StepName)
	}
}

func TestDeserializeCompensations(t *testing.T) {
	jsonComps := []compensationJSON{
		{
			StepName:          "step1",
			SignalName:        "test.comp1",
			SignalDescription: "Test comp 1",
			Data:              []byte(`{"id": "1"}`),
		},
		{
			StepName:          "step2",
			SignalName:        "test.comp2",
			SignalDescription: "Test comp 2",
			Data:              []byte(`{"id": "2"}`),
		},
	}

	deserialized := deserializeCompensations(jsonComps)

	if len(deserialized) != 2 {
		t.Fatalf("expected 2 deserialized records, got %d", len(deserialized))
	}

	if deserialized[0].StepName != "step1" {
		t.Errorf("expected StepName 'step1', got %q", deserialized[0].StepName)
	}
	if deserialized[0].Signal.Name() != "test.comp1" {
		t.Errorf("expected Signal.Name 'test.comp1', got %q", deserialized[0].Signal.Name())
	}
	if deserialized[0].Signal.Description() != "Test comp 1" {
		t.Errorf("expected Signal.Description 'Test comp 1', got %q", deserialized[0].Signal.Description())
	}
	if string(deserialized[0].Data) != `{"id": "1"}` {
		t.Errorf("expected Data '{\"id\": \"1\"}', got %q", string(deserialized[0].Data))
	}

	if deserialized[1].StepName != "step2" {
		t.Errorf("expected StepName 'step2', got %q", deserialized[1].StepName)
	}
}

func TestSerializeDeserializeRoundTrip(t *testing.T) {
	signal1 := capitan.NewSignal("test.comp1", "Test compensate 1")
	signal2 := capitan.NewSignal("test.comp2", "Test compensate 2")

	original := []CompensationRecord{
		{
			StepName: "reserve-inventory",
			Signal:   signal1,
			Data:     []byte(`{"item": "widget", "qty": 5}`),
		},
		{
			StepName: "charge-payment",
			Signal:   signal2,
			Data:     []byte(`{"amount": 99.99}`),
		},
	}

	serialized := serializeCompensations(original)
	deserialized := deserializeCompensations(serialized)

	if len(deserialized) != len(original) {
		t.Fatalf("expected %d records, got %d", len(original), len(deserialized))
	}

	for i := range original {
		if deserialized[i].StepName != original[i].StepName {
			t.Errorf("record %d: expected StepName %q, got %q", i, original[i].StepName, deserialized[i].StepName)
		}
		if deserialized[i].Signal.Name() != original[i].Signal.Name() {
			t.Errorf("record %d: expected Signal.Name %q, got %q", i, original[i].Signal.Name(), deserialized[i].Signal.Name())
		}
		if deserialized[i].Signal.Description() != original[i].Signal.Description() {
			t.Errorf("record %d: expected Signal.Description %q, got %q", i, original[i].Signal.Description(), deserialized[i].Signal.Description())
		}
		if !bytes.Equal(deserialized[i].Data, original[i].Data) {
			t.Errorf("record %d: expected Data %q, got %q", i, string(original[i].Data), string(deserialized[i].Data))
		}
	}
}

func TestCompensationJSON_Structure(t *testing.T) {
	cj := compensationJSON{
		StepName:          "test-step",
		SignalName:        "test.signal",
		SignalDescription: "Test signal description",
		Data:              []byte(`{"key": "value"}`),
	}

	if cj.StepName != "test-step" {
		t.Errorf("expected StepName 'test-step', got %q", cj.StepName)
	}
	if cj.SignalName != "test.signal" {
		t.Errorf("expected SignalName 'test.signal', got %q", cj.SignalName)
	}
	if cj.SignalDescription != "Test signal description" {
		t.Errorf("expected SignalDescription 'Test signal description', got %q", cj.SignalDescription)
	}
}

func TestSerializeCompensations_Empty(t *testing.T) {
	var empty []CompensationRecord
	serialized := serializeCompensations(empty)

	if len(serialized) != 0 {
		t.Errorf("expected 0 serialized records, got %d", len(serialized))
	}
}

func TestDeserializeCompensations_Empty(t *testing.T) {
	var empty []compensationJSON
	deserialized := deserializeCompensations(empty)

	if len(deserialized) != 0 {
		t.Errorf("expected 0 deserialized records, got %d", len(deserialized))
	}
}

func TestErrNoRows_Constant(t *testing.T) {
	// Verify the constant matches what database/sql returns
	if errNoRows != "sql: no rows in result set" {
		t.Errorf("expected 'sql: no rows in result set', got %q", errNoRows)
	}
}

func TestCerealStore_NewCerealStore(t *testing.T) {
	// Test that NewCerealStore can be called with nil (for structure verification)
	store := NewCerealStore(nil)
	if store == nil {
		t.Fatal("expected non-nil store")
	}
	if store.db != nil {
		t.Error("expected nil db")
	}
}

func TestTimeoutMillisecondConversion(t *testing.T) {
	// Verify timeout conversion logic used in CerealStore
	timeout := 5 * time.Minute
	ms := timeout.Milliseconds()

	if ms != 300000 {
		t.Errorf("expected 300000ms, got %d", ms)
	}

	// Convert back
	recovered := time.Duration(ms) * time.Millisecond
	if recovered != timeout {
		t.Errorf("expected %v, got %v", timeout, recovered)
	}
}
