package ago

import "github.com/zoobzio/capitan"

// Flow lifecycle signals.
var (
	FlowCreated   = capitan.NewSignal("ago.flow.created", "Flow created")
	FlowCompleted = capitan.NewSignal("ago.flow.completed", "Flow completed")
	FlowFailed    = capitan.NewSignal("ago.flow.failed", "Flow failed")
)

// Saga lifecycle signals.
var (
	SagaStarted       = capitan.NewSignal("ago.saga.started", "Saga started")
	SagaStepCompleted = capitan.NewSignal("ago.saga.step.completed", "Saga step completed")
	SagaCompensating  = capitan.NewSignal("ago.saga.compensating", "Saga compensating")
	SagaCompleted     = capitan.NewSignal("ago.saga.completed", "Saga completed")
	SagaFailed        = capitan.NewSignal("ago.saga.failed", "Saga failed")
)

// Request/response signals.
var (
	RequestSent      = capitan.NewSignal("ago.request.sent", "Request sent")
	ResponseReceived = capitan.NewSignal("ago.response.received", "Response received")
	RequestTimeout   = capitan.NewSignal("ago.request.timeout", "Request timed out")
)

// Dead letter signals.
var (
	DeadLetterRouted = capitan.NewSignal("ago.deadletter.routed", "Message routed to dead letter")
)

// Common keys for signal payloads.
var (
	StepNameKey   = capitan.NewStringKey("step_name")
	SagaStatusKey = capitan.NewKey[SagaStatus]("saga_status", "ago.SagaStatus")
	ErrorKey      = capitan.NewErrorKey("error")
)
