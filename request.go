package ago

import (
	"context"
	"errors"
	"time"

	"github.com/zoobz-io/capitan"
	"github.com/zoobz-io/pipz"
)

// ErrTimeout indicates a request timed out waiting for response.
var ErrTimeout = errors.New("ago: request timeout")

// Request sends a request and waits for a correlated response.
type Request[T, R any] struct {
	identity       pipz.Identity
	capitan        *capitan.Capitan
	requestSignal  capitan.Signal
	responseSignal capitan.Signal
	requestKey     capitan.GenericKey[T]
	responseKey    capitan.GenericKey[R]
	timeout        time.Duration
}

// NewRequest creates a request/response primitive.
func NewRequest[T, R any](
	identity pipz.Identity,
	requestSignal capitan.Signal,
	responseSignal capitan.Signal,
	requestKey capitan.GenericKey[T],
	responseKey capitan.GenericKey[R],
) *Request[T, R] {
	return &Request[T, R]{
		identity:       identity,
		requestSignal:  requestSignal,
		responseSignal: responseSignal,
		requestKey:     requestKey,
		responseKey:    responseKey,
		timeout:        30 * time.Second,
	}
}

// WithCapitan sets a custom capitan instance. Defaults to global.
func (r *Request[T, R]) WithCapitan(c *capitan.Capitan) *Request[T, R] {
	r.capitan = c
	return r
}

// Timeout sets the maximum wait time.
func (r *Request[T, R]) Timeout(d time.Duration) *Request[T, R] {
	r.timeout = d
	return r
}

// Build creates the chainable processor.
func (r *Request[T, R]) Build() pipz.Chainable[*Flow[T]] {
	return pipz.Apply(r.identity, func(ctx context.Context, f *Flow[T]) (*Flow[T], error) {
		// Create response channel
		responseCh := make(chan R, 1)
		hookFn := func(_ context.Context, e *capitan.Event) {
			corrID, ok := CorrelationKey.From(e)
			if !ok || corrID != f.CorrelationID {
				return
			}
			resp, ok := r.responseKey.From(e)
			if ok {
				select {
				case responseCh <- resp:
				default:
				}
			}
		}

		// Hook response signal
		var listener *capitan.Listener
		if r.capitan != nil {
			listener = r.capitan.Hook(r.responseSignal, hookFn)
		} else {
			listener = capitan.Hook(r.responseSignal, hookFn)
		}
		defer listener.Close()

		// Emit request signal
		emitFn := capitan.Emit
		if r.capitan != nil {
			emitFn = r.capitan.Emit
		}

		fields := []capitan.Field{r.requestKey.Field(f.Payload)}
		if f.CorrelationID != "" {
			fields = append(fields, CorrelationKey.Field(f.CorrelationID))
		}
		emitFn(ctx, r.requestSignal, fields...)
		emitFn(ctx, RequestSent, CorrelationKey.Field(f.CorrelationID))

		// Wait for response or timeout
		timeoutCtx, cancel := context.WithTimeout(ctx, r.timeout)
		defer cancel()

		select {
		case resp := <-responseCh:
			emitFn(ctx, ResponseReceived, CorrelationKey.Field(f.CorrelationID))
			f.Set(r.responseKey.Field(resp))
			return f, nil
		case <-timeoutCtx.Done():
			emitFn(ctx, RequestTimeout, CorrelationKey.Field(f.CorrelationID))
			return f, ErrTimeout
		}
	})
}

// Identity returns the processor identity.
func (r *Request[T, R]) Identity() pipz.Identity {
	return r.identity
}

// Schema returns the processor schema.
func (r *Request[T, R]) Schema() pipz.Node {
	return pipz.Node{Identity: r.identity, Type: "request"}
}

// Process implements Chainable.
func (r *Request[T, R]) Process(ctx context.Context, f *Flow[T]) (*Flow[T], error) {
	return r.Build().Process(ctx, f)
}

// Close implements Chainable.
func (*Request[T, R]) Close() error {
	return nil
}
