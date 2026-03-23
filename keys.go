package ago

import "github.com/zoobz-io/capitan"

// Common keys for correlation and causation in distributed flows.
var (
	// CorrelationKey identifies related events across services.
	CorrelationKey = capitan.NewStringKey("correlation_id")

	// CausationKey identifies the direct parent event.
	CausationKey = capitan.NewStringKey("causation_id")

	// IdempotencyKey provides a deterministic key for downstream handlers
	// to ensure exactly-once execution with external systems.
	//
	// IMPORTANT: Signal handlers MUST use this key when calling external systems
	// (databases, APIs, payment processors, etc.) to ensure idempotent operations.
	// ago guarantees the key is unique per step execution, but handlers are
	// responsible for using it appropriately.
	//
	// The key format is "{correlationID}:{stepName}" for execution signals
	// and "{correlationID}:compensate:{stepName}" for compensation signals.
	//
	// Example usage in a handler:
	//
	//	c.Hook(chargePayment, func(ctx context.Context, e *capitan.Event) {
	//		idempotencyKey, _ := ago.IdempotencyKey.From(e)
	//		// Use idempotencyKey with your payment processor
	//		paymentService.Charge(ctx, amount, idempotencyKey)
	//	})
	//
	// This is critical because ago may emit duplicate signals in edge cases
	// (e.g., store failures during idempotency marking). The IdempotencyKey
	// ensures external systems see each operation exactly once.
	IdempotencyKey = capitan.NewStringKey("idempotency_key")
)
