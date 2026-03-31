package ago_test

import (
	"context"
	"errors"
	"testing"

	"github.com/zoobz-io/ago"
)

type EchoInput struct {
	Message string `json:"message"`
}

type EchoOutput struct {
	Echo string `json:"echo"`
}

func TestNewTool(t *testing.T) {
	tool := ago.NewTool[EchoInput, EchoOutput]("echo", func(_ context.Context, inv *ago.Invocation) (EchoOutput, error) {
		input, _ := ago.TypedInput[EchoInput](inv)
		return EchoOutput{Echo: input.Message}, nil
	})

	spec := tool.Spec()
	if spec.Name != "echo" {
		t.Errorf("expected name 'echo', got %q", spec.Name)
	}

	meta := tool.InputMeta()
	if meta.TypeName != "EchoInput" {
		t.Errorf("expected input type EchoInput, got %q", meta.TypeName)
	}

	outMeta := tool.OutputMeta()
	if outMeta.TypeName != "EchoOutput" {
		t.Errorf("expected output type EchoOutput, got %q", outMeta.TypeName)
	}
}

func TestToolWithDescription(t *testing.T) {
	tool := ago.NewTool[ago.NoInput, ago.NoOutput]("noop", func(_ context.Context, _ *ago.Invocation) (ago.NoOutput, error) {
		return ago.NoOutput{}, nil
	}).WithDescription("A no-op tool")

	if tool.Spec().Description != "A no-op tool" {
		t.Errorf("expected description, got %q", tool.Spec().Description)
	}
}

func TestToolWithMiddleware(t *testing.T) {
	called := false
	mw := func(next ago.HandlerFunc) ago.HandlerFunc {
		return func(ctx context.Context, inv *ago.Invocation) (*ago.Result, error) {
			called = true
			return next(ctx, inv)
		}
	}

	tool := ago.NewTool[ago.NoInput, ago.NoOutput]("mw-test", func(_ context.Context, _ *ago.Invocation) (ago.NoOutput, error) {
		return ago.NoOutput{}, nil
	}).WithMiddleware(mw)

	if len(tool.Middleware()) != 1 {
		t.Errorf("expected 1 middleware, got %d", len(tool.Middleware()))
	}
	_ = called
}

func TestToolWithErrors(t *testing.T) {
	customErr := ago.NewError[ago.NoDetails]("CUSTOM", "custom error")

	tool := ago.NewTool[ago.NoInput, ago.NoOutput]("err-test", func(_ context.Context, _ *ago.Invocation) (ago.NoOutput, error) {
		return ago.NoOutput{}, nil
	}).WithErrors(customErr)

	if len(tool.ErrorDefs()) != 1 {
		t.Errorf("expected 1 error def, got %d", len(tool.ErrorDefs()))
	}
}

func TestToolHandleSuccess(t *testing.T) {
	tool := ago.NewTool[EchoInput, EchoOutput]("echo", func(_ context.Context, inv *ago.Invocation) (EchoOutput, error) {
		input, _ := ago.TypedInput[EchoInput](inv)
		return EchoOutput{Echo: input.Message}, nil
	})

	inv := &ago.Invocation{
		Context:  context.Background(),
		RawInput: []byte(`{"message":"hello"}`),
		Metadata: make(map[string]any),
	}

	result, err := tool.Handle(context.Background(), inv)
	if err != nil {
		t.Fatalf("Handle failed: %v", err)
	}
	if result.IsError() {
		t.Fatal("expected success result")
	}

	output, ok := result.Output.(EchoOutput)
	if !ok {
		t.Fatalf("expected EchoOutput, got %T", result.Output)
	}
	if output.Echo != "hello" {
		t.Errorf("expected echo 'hello', got %q", output.Echo)
	}
}

func TestToolHandleInvalidJSON(t *testing.T) {
	tool := ago.NewTool[EchoInput, EchoOutput]("echo", func(_ context.Context, _ *ago.Invocation) (EchoOutput, error) {
		return EchoOutput{}, nil
	})

	inv := &ago.Invocation{
		Context:  context.Background(),
		RawInput: []byte(`{invalid`),
		Metadata: make(map[string]any),
	}

	_, err := tool.Handle(context.Background(), inv)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !errors.Is(err, ago.ErrValidation) {
		t.Errorf("expected ErrValidation, got %v", err)
	}
}

func TestToolHandleToolError(t *testing.T) {
	customErr := ago.NewError[ago.NoDetails]("NOT_FOUND", "resource not found")

	tool := ago.NewTool[ago.NoInput, ago.NoOutput]("fail", func(_ context.Context, _ *ago.Invocation) (ago.NoOutput, error) {
		return ago.NoOutput{}, customErr
	})

	inv := &ago.Invocation{
		Context:  context.Background(),
		Metadata: make(map[string]any),
	}

	result, err := tool.Handle(context.Background(), inv)
	if err != nil {
		t.Fatalf("expected tool error in result, not dispatch error: %v", err)
	}
	if !result.IsError() {
		t.Fatal("expected error result")
	}
	if result.Error.Code() != "NOT_FOUND" {
		t.Errorf("expected NOT_FOUND, got %q", result.Error.Code())
	}
}

func TestToolHandleNoInput(t *testing.T) {
	tool := ago.NewTool[ago.NoInput, EchoOutput]("no-input", func(_ context.Context, _ *ago.Invocation) (EchoOutput, error) {
		return EchoOutput{Echo: "no input needed"}, nil
	})

	inv := &ago.Invocation{
		Context:  context.Background(),
		Metadata: make(map[string]any),
	}

	result, err := tool.Handle(context.Background(), inv)
	if err != nil {
		t.Fatalf("Handle failed: %v", err)
	}
	if result.IsError() {
		t.Fatal("expected success")
	}
}

type ValidatedInput struct {
	Name string `json:"name"`
}

func (v *ValidatedInput) Validate() error {
	if v.Name == "" {
		return errors.New("name: required")
	}
	return nil
}

func TestToolHandleValidation(t *testing.T) {
	tool := ago.NewTool[ValidatedInput, ago.NoOutput]("validated", func(_ context.Context, _ *ago.Invocation) (ago.NoOutput, error) {
		return ago.NoOutput{}, nil
	})

	// Valid input.
	inv := &ago.Invocation{
		Context:  context.Background(),
		RawInput: []byte(`{"name":"test"}`),
		Metadata: make(map[string]any),
	}
	_, err := tool.Handle(context.Background(), inv)
	if err != nil {
		t.Fatalf("valid input should succeed: %v", err)
	}

	// Invalid input.
	inv2 := &ago.Invocation{
		Context:  context.Background(),
		RawInput: []byte(`{"name":""}`),
		Metadata: make(map[string]any),
	}
	_, err = tool.Handle(context.Background(), inv2)
	if err == nil {
		t.Fatal("invalid input should fail")
	}
	if !errors.Is(err, ago.ErrValidation) {
		t.Errorf("expected ErrValidation, got %v", err)
	}
}
