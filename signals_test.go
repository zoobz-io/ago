package ago

import (
	"testing"
)

func TestFlowSignals(t *testing.T) {
	// Verify flow lifecycle signals have expected names
	if FlowCreated.Name() != "ago.flow.created" {
		t.Errorf("expected 'ago.flow.created', got %q", FlowCreated.Name())
	}
	if FlowCompleted.Name() != "ago.flow.completed" {
		t.Errorf("expected 'ago.flow.completed', got %q", FlowCompleted.Name())
	}
	if FlowFailed.Name() != "ago.flow.failed" {
		t.Errorf("expected 'ago.flow.failed', got %q", FlowFailed.Name())
	}
}

func TestSagaSignals(t *testing.T) {
	// Verify saga lifecycle signals have expected names
	if SagaStarted.Name() != "ago.saga.started" {
		t.Errorf("expected 'ago.saga.started', got %q", SagaStarted.Name())
	}
	if SagaStepCompleted.Name() != "ago.saga.step.completed" {
		t.Errorf("expected 'ago.saga.step.completed', got %q", SagaStepCompleted.Name())
	}
	if SagaCompensating.Name() != "ago.saga.compensating" {
		t.Errorf("expected 'ago.saga.compensating', got %q", SagaCompensating.Name())
	}
	if SagaCompleted.Name() != "ago.saga.completed" {
		t.Errorf("expected 'ago.saga.completed', got %q", SagaCompleted.Name())
	}
	if SagaFailed.Name() != "ago.saga.failed" {
		t.Errorf("expected 'ago.saga.failed', got %q", SagaFailed.Name())
	}
}

func TestRequestResponseSignals(t *testing.T) {
	// Verify request/response signals have expected names
	if RequestSent.Name() != "ago.request.sent" {
		t.Errorf("expected 'ago.request.sent', got %q", RequestSent.Name())
	}
	if ResponseReceived.Name() != "ago.response.received" {
		t.Errorf("expected 'ago.response.received', got %q", ResponseReceived.Name())
	}
	if RequestTimeout.Name() != "ago.request.timeout" {
		t.Errorf("expected 'ago.request.timeout', got %q", RequestTimeout.Name())
	}
}

func TestDeadLetterSignals(t *testing.T) {
	// Verify dead letter signals have expected names
	if DeadLetterRouted.Name() != "ago.deadletter.routed" {
		t.Errorf("expected 'ago.deadletter.routed', got %q", DeadLetterRouted.Name())
	}
}

func TestCommonKeys(t *testing.T) {
	// Verify common keys exist and have expected key names
	if StepNameKey.Name() != "step_name" {
		t.Errorf("expected 'step_name', got %q", StepNameKey.Name())
	}
	if SagaStatusKey.Name() != "saga_status" {
		t.Errorf("expected 'saga_status', got %q", SagaStatusKey.Name())
	}
	if ErrorKey.Name() != "error" {
		t.Errorf("expected 'error', got %q", ErrorKey.Name())
	}
}

func TestSignalDescriptions(t *testing.T) {
	// Verify signals have non-empty descriptions
	signals := []struct {
		name string
		desc string
	}{
		{"FlowCreated", FlowCreated.Description()},
		{"FlowCompleted", FlowCompleted.Description()},
		{"FlowFailed", FlowFailed.Description()},
		{"SagaStarted", SagaStarted.Description()},
		{"SagaStepCompleted", SagaStepCompleted.Description()},
		{"SagaCompensating", SagaCompensating.Description()},
		{"SagaCompleted", SagaCompleted.Description()},
		{"SagaFailed", SagaFailed.Description()},
		{"RequestSent", RequestSent.Description()},
		{"ResponseReceived", ResponseReceived.Description()},
		{"RequestTimeout", RequestTimeout.Description()},
		{"DeadLetterRouted", DeadLetterRouted.Description()},
	}

	for _, sig := range signals {
		if sig.desc == "" {
			t.Errorf("%s has empty description", sig.name)
		}
	}
}
