# ago

[![CI Status](https://github.com/zoobz-io/ago/workflows/CI/badge.svg)](https://github.com/zoobz-io/ago/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/zoobz-io/ago/graph/badge.svg?branch=main)](https://codecov.io/gh/zoobz-io/ago)
[![Go Report Card](https://goreportcard.com/badge/github.com/zoobz-io/ago)](https://goreportcard.com/report/github.com/zoobz-io/ago)
[![CodeQL](https://github.com/zoobz-io/ago/workflows/CodeQL/badge.svg)](https://github.com/zoobz-io/ago/security/code-scanning)
[![Go Reference](https://pkg.go.dev/badge/github.com/zoobz-io/ago.svg)](https://pkg.go.dev/github.com/zoobz-io/ago)
[![License](https://img.shields.io/github/license/zoobz-io/ago)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/zoobz-io/ago)](go.mod)
[![Release](https://img.shields.io/github/v/release/zoobz-io/ago)](https://github.com/zoobz-io/ago/releases)

Event-driven orchestration primitives for Go.

ago ("I do" in Latin) bridges [capitan](https://github.com/zoobz-io/capitan) events with [pipz](https://github.com/zoobz-io/pipz) pipelines, enabling distributed sagas, request/response patterns, and stateful coordination across processes.

## Installation

```bash
go get github.com/zoobz-io/ago
```

Requirements: Go 1.24+

## Core Concepts

### Flow

`Flow[T]` wraps a typed payload with correlation context and accumulated state:

```go
// Create a flow from an event
flow := ago.NewFromEvent(event, orderKey)

// Or create directly
flow := ago.NewFlow(order, orderCreated)
flow.CorrelationID = "order-123"
```

All primitives implement `pipz.Chainable[*Flow[T]]`, composable via pipz topologies.

### Correlation

Flows carry correlation and causation IDs for distributed tracing:

```go
pipeline := ago.Sequence("process-order",
    ago.Correlate[Order]("correlate"),           // Generate correlation ID
    ago.SagaStep(...).Build(),
    ago.Emit[Order](...).Build(),
)
```

## Primitives

### Saga Orchestration

Execute distributed transactions with automatic compensation on failure:

```go
// Define signals
var (
    reserveInventory   = capitan.NewSignal("inventory.reserve", "Reserve inventory")
    releaseInventory   = capitan.NewSignal("inventory.release", "Release inventory")
    chargePayment      = capitan.NewSignal("payment.charge", "Charge payment")
    refundPayment      = capitan.NewSignal("payment.refund", "Refund payment")
)

// Build saga pipeline
pipeline := ago.Sequence("order-saga",
    ago.Correlate[Order]("correlate"),

    ago.NewSagaStep[Order](
        "reserve-inventory",
        store,
        orderKey,
        reserveInventory,    // Execute signal
        releaseInventory,    // Compensate signal
    ).WithTimeout(5 * time.Minute).Build(),

    ago.NewSagaStep[Order](
        "charge-payment",
        store,
        orderKey,
        chargePayment,
        refundPayment,
    ).Build(),
)

// On failure, trigger compensation
compensate := ago.NewCompensate[Order]("compensate", store, orderKey).Build()
```

Sagas provide:
- Compensation registered atomically *before* execution
- LIFO rollback order
- Idempotency via compensation records
- Crash recovery via `RecoverSagas`
- Configurable timeouts

### Request/Response

Synchronous request/response over async events:

```go
request := ago.NewRequest[Order, PaymentResult](
    "charge-card",
    chargeRequest,      // Request signal
    chargeResponse,     // Response signal
    orderKey,           // Request payload key
    paymentResultKey,   // Response payload key
).Timeout(30 * time.Second).Build()

// Sends request, waits for correlated response
flow, err := request.Process(ctx, flow)
result, _ := ago.From(flow, paymentResultKey)
```

### Await

Wait for a correlated event:

```go
await := ago.NewAwait[Order, ShippingStatus](
    "await-shipment",
    shippingUpdated,
    shippingStatusKey,
).Timeout(24 * time.Hour).Build()
```

### Emit

Fire-and-forget event emission:

```go
emit := ago.NewEmit[Order](
    "emit-created",
    orderCreated,
    orderKey,
).Build()
```

### Enrichment

Augment flows with external data:

```go
// Fails on error
enrich := ago.Enrich[Order, Customer](
    "fetch-customer",
    customerKey,
    func(ctx context.Context, order Order) (Customer, error) {
        return customerService.Get(ctx, order.CustomerID)
    },
)

// Logs error but continues
enrichOptional := ago.EnrichOptional[Order, Discount](
    "fetch-discount",
    discountKey,
    fetchDiscount,
)
```

### Integration

Publish to message brokers:

```go
publish := ago.Publish[Order]("publish-order", kafkaProvider)
```

Route failures to dead letter queue:

```go
deadLetter := ago.NewDeadLetter[Order]("dlq", orderKey).
    WithProvider(dlqProvider).
    Build()
```

## Flow Control

ago provides typed wrappers around pipz flow control:

```go
pipeline := ago.Sequence("order-pipeline",
    // Retry with backoff
    ago.Backoff("retry-payment", paymentStep, 3, time.Second),

    // Circuit breaker
    ago.CircuitBreaker("inventory-breaker", inventoryStep, 5, time.Minute),

    // Rate limiting
    ago.RateLimiter[Order]("rate-limit", 100, 10),

    // Timeout
    ago.Timeout("shipping-timeout", shippingStep, 30*time.Second),

    // Fallback on failure
    ago.Fallback("payment-fallback", primaryPayment, backupPayment),

    // Parallel execution
    ago.Concurrent("parallel-notify",
        func(original *ago.Flow[Order], results map[pipz.Identity]*ago.Flow[Order], errors map[pipz.Identity]error) *ago.Flow[Order] {
            return original
        },
        emailNotify,
        smsNotify,
        pushNotify,
    ),
)
```

## Storage

ago requires a `Store` for saga state and coordination:

```go
// In-memory (testing)
store := ago.NewMemoryStore()

// PostgreSQL (production)
store := ago.NewCerealStore(db)
store.Migrate(ctx) // Create tables
```

## Recovery

Handle crashed or timed-out sagas:

```go
// Run periodically
recovered, err := ago.RecoverSagas(ctx, store, orderKey, capitan.Default())
for _, state := range recovered {
    log.Printf("Recovered saga %s", state.CorrelationID)
}
```

## Example: Order Processing

```go
package main

import (
    "context"
    "time"

    "github.com/zoobz-io/ago"
    "github.com/zoobz-io/capitan"
    "github.com/zoobz-io/pipz"
)

type Order struct {
    ID         string
    CustomerID string
    Total      float64
}

var (
    // Signals
    orderCreated     = capitan.NewSignal("order.created", "Order created")
    inventoryReserve = capitan.NewSignal("inventory.reserve", "Reserve inventory")
    inventoryRelease = capitan.NewSignal("inventory.release", "Release inventory")
    paymentCharge    = capitan.NewSignal("payment.charge", "Charge payment")
    paymentRefund    = capitan.NewSignal("payment.refund", "Refund payment")

    // Keys
    orderKey = capitan.NewKey[Order]("order", "app.Order")
)

func main() {
    store := ago.NewMemoryStore()

    // Build order processing pipeline
    pipeline := ago.Sequence("process-order",
        ago.Correlate[Order]("correlate"),

        ago.NewSagaStep[Order]("reserve", store, orderKey,
            inventoryReserve, inventoryRelease,
        ).WithTimeout(5 * time.Minute).Build(),

        ago.NewSagaStep[Order]("charge", store, orderKey,
            paymentCharge, paymentRefund,
        ).Build(),

        ago.NewEmit[Order]("emit-created", orderCreated, orderKey).Build(),
    )

    // Process an order
    flow := ago.NewFlow(Order{ID: "ORD-123", Total: 99.99}, orderCreated)
    result, err := pipeline.Process(context.Background(), flow)
    if err != nil {
        // Trigger compensation
        compensate := ago.NewCompensate[Order]("compensate", store, orderKey)
        compensate.Process(context.Background(), flow)
    }

    capitan.Shutdown()
}
```

## Testing

Run tests:
```bash
make test
```

Run integration tests:
```bash
make test-integration
```

Run with coverage:
```bash
make coverage
```

## Contributing

Contributions welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT License - see [LICENSE](LICENSE) file for details.
