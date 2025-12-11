package ago

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zoobzio/capitan"
	"github.com/zoobzio/pipz"
)

func TestDo(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")

	processor := Do("test-do", func(_ context.Context, f *Flow[Order]) (*Flow[Order], error) {
		f.Payload.Total = 200.0
		return f, nil
	})

	flow := NewFlow(Order{ID: "order-1", Total: 100.0}, signal)
	result, err := processor.Process(ctx, flow)

	if err != nil {
		t.Fatalf("Do failed: %v", err)
	}
	if result.Payload.Total != 200.0 {
		t.Errorf("expected Total 200.0, got %v", result.Payload.Total)
	}
}

func TestDo_WithError(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")
	expectedErr := errors.New("test error")

	processor := Do("test-do-error", func(_ context.Context, f *Flow[Order]) (*Flow[Order], error) {
		return f, expectedErr
	})

	flow := NewFlow(Order{ID: "order-1"}, signal)
	_, err := processor.Process(ctx, flow)

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestTransform(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")

	processor := Transform("test-transform", func(_ context.Context, f *Flow[Order]) *Flow[Order] {
		f.Payload.Total = 300.0
		return f
	})

	flow := NewFlow(Order{ID: "order-1", Total: 100.0}, signal)
	result, err := processor.Process(ctx, flow)

	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}
	if result.Payload.Total != 300.0 {
		t.Errorf("expected Total 300.0, got %v", result.Payload.Total)
	}
}

func TestEffect(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")

	var effectCalled bool
	processor := Effect("test-effect", func(_ context.Context, _ *Flow[Order]) error {
		effectCalled = true
		return nil
	})

	flow := NewFlow(Order{ID: "order-1"}, signal)
	_, err := processor.Process(ctx, flow)

	if err != nil {
		t.Fatalf("Effect failed: %v", err)
	}
	if !effectCalled {
		t.Error("expected effect to be called")
	}
}

func TestEffect_WithError(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")
	expectedErr := errors.New("effect error")

	processor := Effect("test-effect-error", func(_ context.Context, _ *Flow[Order]) error {
		return expectedErr
	})

	flow := NewFlow(Order{ID: "order-1"}, signal)
	_, err := processor.Process(ctx, flow)

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestMutate(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")

	processor := Mutate("test-mutate",
		func(_ context.Context, f *Flow[Order]) *Flow[Order] {
			f.Payload.Total = 500.0
			return f
		},
		func(_ context.Context, f *Flow[Order]) bool {
			return f.Payload.Total > 100.0
		},
	)

	// Should mutate (predicate true)
	flow := NewFlow(Order{ID: "order-1", Total: 200.0}, signal)
	result, _ := processor.Process(ctx, flow)
	if result.Payload.Total != 500.0 {
		t.Errorf("expected Total 500.0 when predicate true, got %v", result.Payload.Total)
	}

	// Should not mutate (predicate false)
	flow = NewFlow(Order{ID: "order-2", Total: 50.0}, signal)
	result, _ = processor.Process(ctx, flow)
	if result.Payload.Total != 50.0 {
		t.Errorf("expected Total 50.0 when predicate false, got %v", result.Payload.Total)
	}
}

func TestEnrichWith(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")

	processor := EnrichWith("test-enrich", func(_ context.Context, f *Flow[Order]) (*Flow[Order], error) {
		f.Payload.Total = 600.0
		return f, nil
	})

	flow := NewFlow(Order{ID: "order-1", Total: 100.0}, signal)
	result, err := processor.Process(ctx, flow)

	if err != nil {
		t.Fatalf("EnrichWith failed: %v", err)
	}
	if result.Payload.Total != 600.0 {
		t.Errorf("expected Total 600.0, got %v", result.Payload.Total)
	}
}

func TestSequence(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")

	p1 := Transform("step1", func(_ context.Context, f *Flow[Order]) *Flow[Order] {
		f.Payload.Total += 10.0
		return f
	})
	p2 := Transform("step2", func(_ context.Context, f *Flow[Order]) *Flow[Order] {
		f.Payload.Total *= 2.0
		return f
	})

	seq := Sequence("test-sequence", p1, p2)

	flow := NewFlow(Order{ID: "order-1", Total: 100.0}, signal)
	result, err := seq.Process(ctx, flow)

	if err != nil {
		t.Fatalf("Sequence failed: %v", err)
	}
	if result.Payload.Total != 220.0 {
		t.Errorf("expected Total 220.0, got %v", result.Payload.Total)
	}
}

func TestFilter(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")

	var processed bool
	inner := Transform("inner", func(_ context.Context, f *Flow[Order]) *Flow[Order] {
		processed = true
		f.Payload.Total = 999.0
		return f
	})

	filter := Filter("test-filter",
		func(_ context.Context, f *Flow[Order]) bool {
			return f.Payload.Total > 100.0
		},
		inner,
	)

	// Should process (predicate true)
	processed = false
	flow := NewFlow(Order{ID: "order-1", Total: 200.0}, signal)
	result, _ := filter.Process(ctx, flow)
	if !processed {
		t.Error("expected inner processor to be called when predicate true")
	}
	if result.Payload.Total != 999.0 {
		t.Errorf("expected Total 999.0, got %v", result.Payload.Total)
	}

	// Should skip (predicate false)
	processed = false
	flow = NewFlow(Order{ID: "order-2", Total: 50.0}, signal)
	result, _ = filter.Process(ctx, flow)
	if processed {
		t.Error("expected inner processor to be skipped when predicate false")
	}
	if result.Payload.Total != 50.0 {
		t.Errorf("expected Total 50.0 (unchanged), got %v", result.Payload.Total)
	}
}

func TestGate(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")

	gate := Gate("test-gate", func(_ context.Context, f *Flow[Order]) bool {
		return f.Payload.Total > 100.0
	})

	// Should pass
	flow := NewFlow(Order{ID: "order-1", Total: 200.0}, signal)
	result, err := gate.Process(ctx, flow)
	if err != nil {
		t.Fatalf("Gate failed: %v", err)
	}
	if result.Payload.Total != 200.0 {
		t.Errorf("expected Total 200.0, got %v", result.Payload.Total)
	}

	// Should also pass (gate doesn't block, just evaluates)
	flow = NewFlow(Order{ID: "order-2", Total: 50.0}, signal)
	_, err = gate.Process(ctx, flow)
	if err != nil {
		t.Fatalf("Gate failed: %v", err)
	}
}

func TestRetry(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")

	attempts := 0
	inner := Do("inner", func(_ context.Context, f *Flow[Order]) (*Flow[Order], error) {
		attempts++
		if attempts < 3 {
			return f, errors.New("transient error")
		}
		return f, nil
	})

	retry := Retry("test-retry", inner, 5)

	flow := NewFlow(Order{ID: "order-1"}, signal)
	_, err := retry.Process(ctx, flow)

	if err != nil {
		t.Fatalf("Retry should have succeeded: %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestBackoff(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")

	attempts := 0
	inner := Do("inner", func(_ context.Context, f *Flow[Order]) (*Flow[Order], error) {
		attempts++
		if attempts < 2 {
			return f, errors.New("transient error")
		}
		return f, nil
	})

	backoff := Backoff("test-backoff", inner, 3, 1*time.Millisecond)

	flow := NewFlow(Order{ID: "order-1"}, signal)
	_, err := backoff.Process(ctx, flow)

	if err != nil {
		t.Fatalf("Backoff should have succeeded: %v", err)
	}
	if attempts != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts)
	}
}

func TestTimeout(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")

	inner := Do("slow", func(_ context.Context, f *Flow[Order]) (*Flow[Order], error) {
		time.Sleep(100 * time.Millisecond)
		return f, nil
	})

	timeout := Timeout("test-timeout", inner, 10*time.Millisecond)

	flow := NewFlow(Order{ID: "order-1"}, signal)
	_, err := timeout.Process(ctx, flow)

	if err == nil {
		t.Error("expected timeout error")
	}
}

func TestFallback(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")

	p1 := Do("failing", func(_ context.Context, f *Flow[Order]) (*Flow[Order], error) {
		return f, errors.New("first failed")
	})
	p2 := Do("succeeding", func(_ context.Context, f *Flow[Order]) (*Flow[Order], error) {
		f.Payload.Total = 777.0
		return f, nil
	})

	fallback := Fallback("test-fallback", p1, p2)

	flow := NewFlow(Order{ID: "order-1", Total: 100.0}, signal)
	result, err := fallback.Process(ctx, flow)

	if err != nil {
		t.Fatalf("Fallback failed: %v", err)
	}
	if result.Payload.Total != 777.0 {
		t.Errorf("expected Total 777.0 from fallback, got %v", result.Payload.Total)
	}
}

func TestRateLimiter(t *testing.T) {
	signal := capitan.NewSignal("test", "Test")

	limiter := RateLimiter[Order]("test-limiter", 100.0, 10)

	if limiter.Name() != "test-limiter" {
		t.Errorf("expected name 'test-limiter', got %q", limiter.Name())
	}

	// Basic functionality test
	ctx := context.Background()
	flow := NewFlow(Order{ID: "order-1"}, signal)
	_, err := limiter.Process(ctx, flow)

	if err != nil {
		t.Fatalf("RateLimiter failed: %v", err)
	}
}

func TestCircuitBreaker(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")

	inner := Do("inner", func(_ context.Context, f *Flow[Order]) (*Flow[Order], error) {
		return f, nil
	})

	cb := CircuitBreaker("test-cb", inner, 5, 1*time.Second)

	flow := NewFlow(Order{ID: "order-1"}, signal)
	_, err := cb.Process(ctx, flow)

	if err != nil {
		t.Fatalf("CircuitBreaker failed: %v", err)
	}
}

func TestSwitch(t *testing.T) {
	sw := Switch("test-switch", func(_ context.Context, f *Flow[Order]) string {
		if f.Payload.Total > 100 {
			return "high"
		}
		return "low"
	})

	if sw.Name() != "test-switch" {
		t.Errorf("expected name 'test-switch', got %q", sw.Name())
	}
}

func TestConcurrent(t *testing.T) {
	concurrent := Concurrent("test-concurrent",
		func(original *Flow[Order], _ map[pipz.Name]*Flow[Order], _ map[pipz.Name]error) *Flow[Order] {
			return original
		},
	)

	if concurrent.Name() != "test-concurrent" {
		t.Errorf("expected name 'test-concurrent', got %q", concurrent.Name())
	}
}

func TestRace(t *testing.T) {
	race := Race[Order]("test-race")

	if race.Name() != "test-race" {
		t.Errorf("expected name 'test-race', got %q", race.Name())
	}
}

func TestWorkerPool(t *testing.T) {
	pool := WorkerPool[Order]("test-pool", 4)

	if pool.Name() != "test-pool" {
		t.Errorf("expected name 'test-pool', got %q", pool.Name())
	}
}

func TestHandle(t *testing.T) {
	inner := Do("failing", func(_ context.Context, f *Flow[Order]) (*Flow[Order], error) {
		return f, errors.New("handled error")
	})

	// Note: Handle expects error handler of type Chainable[*pipz.Error[*Flow[T]]]
	// This is a simplified test just to verify the function exists
	handle := Handle("test-handle", inner, nil)
	if handle == nil {
		t.Error("expected Handle to return non-nil")
	}
}
