package ago_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/zoobz-io/ago"
	"github.com/zoobz-io/capitan"
)

func newEchoTool() *ago.Tool[EchoInput, EchoOutput] {
	return ago.NewTool[EchoInput, EchoOutput]("echo", func(_ context.Context, inv *ago.Invocation) (EchoOutput, error) {
		input, _ := ago.TypedInput[EchoInput](inv)
		return EchoOutput{Echo: input.Message}, nil
	}).WithDescription("Echo the input message")
}

func TestRegistryRegisterAndInvoke(t *testing.T) {
	r := ago.NewRegistry()
	r.Register(newEchoTool())

	result, err := r.Invoke(context.Background(), "echo", []byte(`{"message":"hello"}`), nil)
	if err != nil {
		t.Fatalf("Invoke failed: %v", err)
	}
	if result.IsError() {
		t.Fatal("expected success result")
	}

	output, ok := result.Output.(EchoOutput)
	if !ok {
		t.Fatalf("expected EchoOutput, got %T", result.Output)
	}
	if output.Echo != "hello" {
		t.Errorf("expected 'hello', got %q", output.Echo)
	}
}

func TestRegistryToolNotFound(t *testing.T) {
	r := ago.NewRegistry()

	_, err := r.Invoke(context.Background(), "missing", nil, nil)
	if err == nil {
		t.Fatal("expected error for missing tool")
	}
	if !errors.Is(err, ago.ErrToolNotFound) {
		t.Errorf("expected ErrToolNotFound, got %v", err)
	}
}

func TestRegistryDuplicateRegistrationPanics(t *testing.T) {
	r := ago.NewRegistry()
	r.Register(newEchoTool())

	defer func() {
		if rec := recover(); rec == nil {
			t.Fatal("expected panic on duplicate registration")
		}
	}()
	r.Register(newEchoTool())
}

func TestRegistryGlobalMiddleware(t *testing.T) {
	var order []string

	mw1 := func(next ago.HandlerFunc) ago.HandlerFunc {
		return func(ctx context.Context, inv *ago.Invocation) (*ago.Result, error) {
			order = append(order, "global-before")
			result, err := next(ctx, inv)
			order = append(order, "global-after")
			return result, err
		}
	}

	r := ago.NewRegistry().WithMiddleware(mw1)
	r.Register(newEchoTool())

	_, err := r.Invoke(context.Background(), "echo", []byte(`{"message":"test"}`), nil)
	if err != nil {
		t.Fatalf("Invoke failed: %v", err)
	}

	if len(order) != 2 || order[0] != "global-before" || order[1] != "global-after" {
		t.Errorf("expected [global-before global-after], got %v", order)
	}
}

func TestRegistryPerToolMiddleware(t *testing.T) {
	var order []string

	globalMW := func(next ago.HandlerFunc) ago.HandlerFunc {
		return func(ctx context.Context, inv *ago.Invocation) (*ago.Result, error) {
			order = append(order, "global")
			return next(ctx, inv)
		}
	}

	toolMW := func(next ago.HandlerFunc) ago.HandlerFunc {
		return func(ctx context.Context, inv *ago.Invocation) (*ago.Result, error) {
			order = append(order, "tool")
			return next(ctx, inv)
		}
	}

	tool := newEchoTool().WithMiddleware(toolMW)

	r := ago.NewRegistry().WithMiddleware(globalMW)
	r.Register(tool)

	_, err := r.Invoke(context.Background(), "echo", []byte(`{"message":"test"}`), nil)
	if err != nil {
		t.Fatalf("Invoke failed: %v", err)
	}

	// Global middleware executes first (outer), then tool middleware.
	if len(order) != 2 || order[0] != "global" || order[1] != "tool" {
		t.Errorf("expected [global tool], got %v", order)
	}
}

func TestRegistryPanicRecovery(t *testing.T) {
	tool := ago.NewTool[ago.NoInput, ago.NoOutput]("panicker", func(_ context.Context, _ *ago.Invocation) (ago.NoOutput, error) {
		panic("something went wrong")
	})

	r := ago.NewRegistry()
	r.Register(tool)

	_, err := r.Invoke(context.Background(), "panicker", nil, nil)
	if err == nil {
		t.Fatal("expected error from panic")
	}
	if !errors.Is(err, ago.ErrPanicked) {
		t.Errorf("expected ErrPanicked, got %v", err)
	}
}

func TestRegistryNilIdentity(t *testing.T) {
	tool := ago.NewTool[ago.NoInput, ago.NoOutput]("check-identity", func(_ context.Context, inv *ago.Invocation) (ago.NoOutput, error) {
		if inv.Identity == nil {
			panic("identity should never be nil")
		}
		return ago.NoOutput{}, nil
	})

	r := ago.NewRegistry()
	r.Register(tool)

	// Passing nil identity — registry should substitute NoIdentity.
	_, err := r.Invoke(context.Background(), "check-identity", nil, nil)
	if err != nil {
		t.Fatalf("Invoke failed: %v", err)
	}
}

func TestRegistryToolsAndTool(t *testing.T) {
	r := ago.NewRegistry()
	r.Register(newEchoTool())

	tools := r.Tools()
	if len(tools) != 1 {
		t.Errorf("expected 1 tool, got %d", len(tools))
	}

	tool := r.Tool("echo")
	if tool == nil {
		t.Fatal("expected to find echo tool")
	}
	if tool.Spec().Name != "echo" {
		t.Errorf("expected echo, got %q", tool.Spec().Name)
	}

	missing := r.Tool("missing")
	if missing != nil {
		t.Error("expected nil for missing tool")
	}
}

func TestRegistryLen(t *testing.T) {
	r := ago.NewRegistry()
	if r.Len() != 0 {
		t.Errorf("expected 0, got %d", r.Len())
	}

	r.Register(newEchoTool())
	if r.Len() != 1 {
		t.Errorf("expected 1, got %d", r.Len())
	}
}

func TestRegistrySignals(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	var signals []string
	var mu sync.Mutex

	track := func(signal capitan.Signal) {
		c.Hook(signal, func(_ context.Context, _ *capitan.Event) {
			mu.Lock()
			defer mu.Unlock()
			signals = append(signals, signal.Name())
		})
	}

	track(ago.ToolRegistered)
	track(ago.ToolExecutionStarted)
	track(ago.ToolExecutionCompleted)
	track(ago.ToolExecutionFailed)

	r := ago.NewRegistry().WithCapitan(c)
	r.Register(newEchoTool())

	_, _ = r.Invoke(context.Background(), "echo", []byte(`{"message":"test"}`), nil)

	mu.Lock()
	defer mu.Unlock()

	expected := []string{
		"ago.tool.registered",
		"ago.tool.execution.started",
		"ago.tool.execution.completed",
	}

	if len(signals) != len(expected) {
		t.Fatalf("expected %d signals, got %d: %v", len(expected), len(signals), signals)
	}
	for i, s := range expected {
		if signals[i] != s {
			t.Errorf("signal %d: expected %q, got %q", i, s, signals[i])
		}
	}
}

func TestRegistrySignalsOnFailure(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	var gotFailed bool
	c.Hook(ago.ToolExecutionFailed, func(_ context.Context, _ *capitan.Event) {
		gotFailed = true
	})

	tool := ago.NewTool[ago.NoInput, ago.NoOutput]("fail", func(_ context.Context, _ *ago.Invocation) (ago.NoOutput, error) {
		return ago.NoOutput{}, errors.New("handler error")
	})

	r := ago.NewRegistry().WithCapitan(c)
	r.Register(tool)

	_, _ = r.Invoke(context.Background(), "fail", nil, nil)

	if !gotFailed {
		t.Error("expected ToolExecutionFailed signal")
	}
}
