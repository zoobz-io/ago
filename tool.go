package ago

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/zoobz-io/sentinel"
)

// ToolSpec holds the declarative specification for a tool.
type ToolSpec struct {
	Name        string // Tool name (unique identifier for dispatch)
	Description string // Human-readable description for the LLM
}

// ToolDefinition is the non-generic interface that all tools satisfy.
// Used by the Registry for type-erased storage and dispatch.
type ToolDefinition interface {
	// Spec returns the tool's declarative specification.
	Spec() ToolSpec

	// InputMeta returns sentinel metadata for the input type.
	InputMeta() sentinel.Metadata

	// OutputMeta returns sentinel metadata for the output type.
	OutputMeta() sentinel.Metadata

	// Handle is the type-erased dispatch function.
	// Deserializes raw JSON input, calls the typed handler, wraps the output.
	Handle(ctx context.Context, inv *Invocation) (*Result, error)

	// Middleware returns per-tool middleware.
	Middleware() []Middleware

	// ErrorDefs returns declared tool errors.
	ErrorDefs() []ErrorDefinition
}

// Tool is a typed tool handler with generic input and output types.
// Parallel to rocco.Handler[In, Out]. Scans In and Out via sentinel
// at creation time for schema generation.
type Tool[In, Out any] struct {
	fn         func(*ToolRequest[In]) (Out, error)
	spec       ToolSpec
	inputMeta  sentinel.Metadata
	outputMeta sentinel.Metadata
	middleware []Middleware
	errorDefs  []ErrorDefinition
}

// NewTool creates a typed tool. Scans In and Out via sentinel at creation.
// The handler function receives a ToolRequest[In] with the deserialized,
// validated input in req.Body — parallel to rocco's Request[In].
func NewTool[In, Out any](name string, fn func(*ToolRequest[In]) (Out, error)) *Tool[In, Out] {
	inMeta := sentinel.Scan[In]()
	outMeta := sentinel.Scan[Out]()

	return &Tool[In, Out]{
		fn: fn,
		spec: ToolSpec{
			Name: name,
		},
		inputMeta:  inMeta,
		outputMeta: outMeta,
	}
}

// Handle implements ToolDefinition. Deserializes input JSON into the typed
// In struct, builds a ToolRequest[In], calls the handler, wraps the output.
func (t *Tool[In, Out]) Handle(ctx context.Context, inv *Invocation) (*Result, error) {
	var input In

	// Deserialize raw JSON into typed input if we have raw bytes
	// and the input type is not NoInput.
	if len(inv.RawInput) > 0 && t.inputMeta.TypeName != "NoInput" {
		if err := json.Unmarshal(inv.RawInput, &input); err != nil {
			return nil, ErrValidation.
				WithMessage(fmt.Sprintf("invalid input JSON: %v", err)).
				WithCause(err)
		}

		// Validate if the input implements Validatable.
		if v, ok := any(&input).(Validatable); ok {
			if err := v.Validate(); err != nil {
				return nil, validationErrorFromValidate(err)
			}
		}
	}

	// Build the typed request — Body is already deserialized and validated.
	req := &ToolRequest[In]{
		Context:  ctx,
		Body:     input,
		Identity: inv.Identity,
		ID:       inv.ID,
		ToolName: inv.ToolName,
		Metadata: inv.Metadata,
	}

	// Call the typed handler.
	output, err := t.fn(req)
	if err != nil {
		// If it's a tool error (ErrorDefinition), return as Result for the LLM.
		var e ErrorDefinition
		if errors.As(err, &e) {
			return NewErrorResult(e), nil
		}
		return nil, err
	}

	return NewResult(output), nil
}

// Spec implements ToolDefinition.
func (t *Tool[In, Out]) Spec() ToolSpec { return t.spec }

// InputMeta implements ToolDefinition.
func (t *Tool[In, Out]) InputMeta() sentinel.Metadata { return t.inputMeta }

// OutputMeta implements ToolDefinition.
func (t *Tool[In, Out]) OutputMeta() sentinel.Metadata { return t.outputMeta }

// Middleware implements ToolDefinition.
func (t *Tool[In, Out]) Middleware() []Middleware { return t.middleware }

// ErrorDefs implements ToolDefinition.
func (t *Tool[In, Out]) ErrorDefs() []ErrorDefinition { return t.errorDefs }

// WithDescription sets the tool description for schema generation.
func (t *Tool[In, Out]) WithDescription(desc string) *Tool[In, Out] {
	t.spec.Description = desc
	return t
}

// WithMiddleware adds per-tool middleware.
func (t *Tool[In, Out]) WithMiddleware(mw ...Middleware) *Tool[In, Out] {
	t.middleware = append(t.middleware, mw...)
	return t
}

// WithErrors declares the tool errors this handler may return.
func (t *Tool[In, Out]) WithErrors(errs ...ErrorDefinition) *Tool[In, Out] {
	t.errorDefs = append(t.errorDefs, errs...)
	return t
}


// Validatable is implemented by input types that can self-validate.
type Validatable interface {
	Validate() error
}

// validationErrorFromValidate converts a Validate() error into a structured
// validation error. If the error message contains field-level details, it
// extracts them.
func validationErrorFromValidate(err error) *Error[ValidationDetails] {
	msg := err.Error()
	fields := []ValidationFieldError{{
		Field:   "",
		Message: msg,
	}}

	// Try to split "field: message" patterns.
	if parts := strings.SplitN(msg, ": ", 2); len(parts) == 2 {
		fields = []ValidationFieldError{{
			Field:   parts[0],
			Message: parts[1],
		}}
	}

	return ErrValidation.WithDetails(ValidationDetails{Fields: fields}).
		WithMessage(msg).
		WithCause(err)
}
