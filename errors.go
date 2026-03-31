package ago

import (
	"errors"
	"fmt"
)

// ErrorDefinition is the interface for all ago error types.
// Used for type-erased error storage in tool declarations.
type ErrorDefinition interface {
	error
	Code() string
	Message() string
	DetailsAny() any
}

// Error is a typed error with generic details, parallel to rocco.Error[D].
// Immutable after construction — builder methods return new instances.
type Error[D any] struct {
	code    string
	message string
	details D
	cause   error
}

// NewError creates a typed error definition.
func NewError[D any](code, message string) *Error[D] {
	return &Error[D]{
		code:    code,
		message: message,
	}
}

// Error returns the error message string.
func (e *Error[D]) Error() string { return e.message }

// Code returns the error code.
func (e *Error[D]) Code() string { return e.code }

// Message returns the error message.
func (e *Error[D]) Message() string { return e.message }

// Details returns the typed error details.
func (e *Error[D]) Details() D { return e.details }

// DetailsAny returns the error details as any.
func (e *Error[D]) DetailsAny() any { return e.details }

// Unwrap returns the underlying cause.
func (e *Error[D]) Unwrap() error { return e.cause }

// Is matches errors by code.
func (e *Error[D]) Is(target error) bool {
	var ed ErrorDefinition
	if errors.As(target, &ed) {
		return e.code == ed.Code()
	}
	return false
}

// WithMessage returns a new error with the given message.
func (e *Error[D]) WithMessage(msg string) *Error[D] {
	clone := *e
	clone.message = msg
	return &clone
}

// WithDetails returns a new error with the given details.
func (e *Error[D]) WithDetails(d D) *Error[D] {
	clone := *e
	clone.details = d
	return &clone
}

// WithCause returns a new error with the given underlying cause.
func (e *Error[D]) WithCause(cause error) *Error[D] {
	clone := *e
	clone.cause = cause
	return &clone
}

// NoDetails is used for errors without structured details.
type NoDetails struct{}

// ValidationDetails carries field-level validation errors.
type ValidationDetails struct {
	Fields []ValidationFieldError `json:"fields"`
}

// ValidationFieldError describes a single field validation failure.
type ValidationFieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// PanicDetails carries recovered panic information.
type PanicDetails struct {
	Value string `json:"value"`
	Stack string `json:"stack"`
}

// Pre-defined dispatch errors.
var (
	// ErrToolNotFound is returned when the requested tool is not registered.
	ErrToolNotFound = NewError[NoDetails]("TOOL_NOT_FOUND", "tool not found")

	// ErrValidation is returned when input validation fails.
	ErrValidation = NewError[ValidationDetails]("VALIDATION_FAILED", "input validation failed")

	// ErrPanicked is returned when a tool handler panics.
	ErrPanicked = NewError[PanicDetails]("TOOL_PANICKED", "tool panicked during execution")
)

// NewValidationError creates a validation error with field details.
func NewValidationError(fields []ValidationFieldError) *Error[ValidationDetails] {
	return ErrValidation.WithDetails(ValidationDetails{Fields: fields}).
		WithMessage(fmt.Sprintf("validation failed: %d field(s)", len(fields)))
}
