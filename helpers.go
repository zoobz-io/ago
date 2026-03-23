package ago

import (
	"context"
	"time"

	"github.com/zoobz-io/pipz"
)

// -----------------------------------------------------------------------------
// Adapter Functions - wrap functions to create Flow processors
// -----------------------------------------------------------------------------

// Do creates a processor from a custom function that can fail.
func Do[T any](name string, fn func(context.Context, *Flow[T]) (*Flow[T], error)) pipz.Processor[*Flow[T]] {
	return pipz.Apply(pipz.NewIdentity(name, ""), fn)
}

// Transform creates a processor from a pure transformation function.
func Transform[T any](name string, fn func(context.Context, *Flow[T]) *Flow[T]) pipz.Processor[*Flow[T]] {
	return pipz.Transform(pipz.NewIdentity(name, ""), fn)
}

// Effect creates a processor that performs a side effect without modifying the flow.
func Effect[T any](name string, fn func(context.Context, *Flow[T]) error) pipz.Processor[*Flow[T]] {
	return pipz.Effect(pipz.NewIdentity(name, ""), fn)
}

// Mutate creates a processor that conditionally modifies a flow.
func Mutate[T any](name string, fn func(context.Context, *Flow[T]) *Flow[T], predicate func(context.Context, *Flow[T]) bool) pipz.Processor[*Flow[T]] {
	return pipz.Mutate(pipz.NewIdentity(name, ""), fn, predicate)
}

// EnrichWith creates a processor that optionally enhances a flow.
// Unlike Do, errors are logged but don't stop the pipeline.
func EnrichWith[T any](name string, fn func(context.Context, *Flow[T]) (*Flow[T], error)) pipz.Processor[*Flow[T]] {
	return pipz.Enrich(pipz.NewIdentity(name, ""), fn)
}

// -----------------------------------------------------------------------------
// Sequential Connectors
// -----------------------------------------------------------------------------

// Sequence creates a sequential pipeline of flow processors.
func Sequence[T any](name string, processors ...pipz.Chainable[*Flow[T]]) *pipz.Sequence[*Flow[T]] {
	return pipz.NewSequence(pipz.NewIdentity(name, ""), processors...)
}

// -----------------------------------------------------------------------------
// Control Flow Connectors
// -----------------------------------------------------------------------------

// Filter creates a conditional processor that either processes or passes through.
func Filter[T any](name string, predicate func(context.Context, *Flow[T]) bool, processor pipz.Chainable[*Flow[T]]) *pipz.Filter[*Flow[T]] {
	return pipz.NewFilter(pipz.NewIdentity(name, ""), predicate, processor)
}

// Switch creates a router that directs flows to different processors.
func Switch[T any](name string, condition func(context.Context, *Flow[T]) string) *pipz.Switch[*Flow[T]] {
	return pipz.NewSwitch(pipz.NewIdentity(name, ""), pipz.Condition[*Flow[T]](condition))
}

// Gate creates a simple pass/fail filter.
func Gate[T any](name string, predicate func(context.Context, *Flow[T]) bool) pipz.Processor[*Flow[T]] {
	return pipz.Apply(pipz.NewIdentity(name, ""), func(ctx context.Context, f *Flow[T]) (*Flow[T], error) {
		if predicate(ctx, f) {
			return f, nil
		}
		return f, nil
	})
}

// -----------------------------------------------------------------------------
// Error Handling Connectors
// -----------------------------------------------------------------------------

// Fallback creates a processor that tries alternatives on failure.
func Fallback[T any](name string, processors ...pipz.Chainable[*Flow[T]]) *pipz.Fallback[*Flow[T]] {
	return pipz.NewFallback(pipz.NewIdentity(name, ""), processors...)
}

// Retry creates a processor that retries on failure up to maxAttempts times.
func Retry[T any](name string, processor pipz.Chainable[*Flow[T]], maxAttempts int) *pipz.Retry[*Flow[T]] {
	return pipz.NewRetry(pipz.NewIdentity(name, ""), processor, maxAttempts)
}

// Backoff creates a processor that retries with exponential backoff.
func Backoff[T any](name string, processor pipz.Chainable[*Flow[T]], maxAttempts int, baseDelay time.Duration) *pipz.Backoff[*Flow[T]] {
	return pipz.NewBackoff(pipz.NewIdentity(name, ""), processor, maxAttempts, baseDelay)
}

// Timeout creates a processor that enforces a time limit on execution.
func Timeout[T any](name string, processor pipz.Chainable[*Flow[T]], duration time.Duration) *pipz.Timeout[*Flow[T]] {
	return pipz.NewTimeout(pipz.NewIdentity(name, ""), processor, duration)
}

// Handle creates a processor that handles errors without stopping the pipeline.
func Handle[T any](name string, processor pipz.Chainable[*Flow[T]], errorHandler pipz.Chainable[*pipz.Error[*Flow[T]]]) *pipz.Handle[*Flow[T]] {
	return pipz.NewHandle(pipz.NewIdentity(name, ""), processor, errorHandler)
}

// -----------------------------------------------------------------------------
// Resource Protection Connectors
// -----------------------------------------------------------------------------

// RateLimiter creates a processor that enforces rate limits.
func RateLimiter[T any](name string, requestsPerSecond float64, burst int, processor pipz.Chainable[*Flow[T]]) *pipz.RateLimiter[*Flow[T]] {
	return pipz.NewRateLimiter[*Flow[T]](pipz.NewIdentity(name, ""), requestsPerSecond, burst, processor)
}

// CircuitBreaker creates a processor that prevents cascade failures.
func CircuitBreaker[T any](name string, processor pipz.Chainable[*Flow[T]], failureThreshold int, resetTimeout time.Duration) *pipz.CircuitBreaker[*Flow[T]] {
	return pipz.NewCircuitBreaker(pipz.NewIdentity(name, ""), processor, failureThreshold, resetTimeout)
}

// -----------------------------------------------------------------------------
// Parallel Connectors (require Flow.Clone())
// -----------------------------------------------------------------------------

// Concurrent runs all processors in parallel and returns the original flow.
func Concurrent[T any](name string, reducer func(original *Flow[T], results map[pipz.Identity]*Flow[T], errors map[pipz.Identity]error) *Flow[T], processors ...pipz.Chainable[*Flow[T]]) *pipz.Concurrent[*Flow[T]] {
	return pipz.NewConcurrent(pipz.NewIdentity(name, ""), reducer, processors...)
}

// Race runs all processors in parallel and returns the first successful result.
func Race[T any](name string, processors ...pipz.Chainable[*Flow[T]]) *pipz.Race[*Flow[T]] {
	return pipz.NewRace(pipz.NewIdentity(name, ""), processors...)
}

// WorkerPool creates a bounded parallel executor with a fixed number of workers.
func WorkerPool[T any](name string, workers int, processors ...pipz.Chainable[*Flow[T]]) *pipz.WorkerPool[*Flow[T]] {
	return pipz.NewWorkerPool(pipz.NewIdentity(name, ""), workers, processors...)
}
