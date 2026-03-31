package ago_test

import (
	"errors"
	"testing"

	"github.com/zoobz-io/ago"
)

func TestNewError(t *testing.T) {
	err := ago.NewError[ago.NoDetails]("TEST_ERROR", "test error message")

	if err.Code() != "TEST_ERROR" {
		t.Errorf("expected code TEST_ERROR, got %q", err.Code())
	}
	if err.Message() != "test error message" {
		t.Errorf("expected message, got %q", err.Message())
	}
	if err.Error() != "test error message" {
		t.Errorf("expected Error() to return message, got %q", err.Error())
	}
}

func TestErrorWithMessage(t *testing.T) {
	original := ago.NewError[ago.NoDetails]("CODE", "original")
	modified := original.WithMessage("modified")

	if original.Message() != "original" {
		t.Error("WithMessage mutated the original")
	}
	if modified.Message() != "modified" {
		t.Errorf("expected modified message, got %q", modified.Message())
	}
}

func TestErrorWithDetails(t *testing.T) {
	type Details struct {
		Field string
	}
	err := ago.NewError[Details]("CODE", "msg")
	detailed := err.WithDetails(Details{Field: "value"})

	if detailed.Details().Field != "value" {
		t.Errorf("expected field value, got %q", detailed.Details().Field)
	}
	if detailed.DetailsAny().(Details).Field != "value" {
		t.Error("DetailsAny should return the same details")
	}
}

func TestErrorWithCause(t *testing.T) {
	cause := errors.New("underlying")
	err := ago.NewError[ago.NoDetails]("CODE", "msg").WithCause(cause)

	if !errors.Is(err, cause) {
		t.Error("Unwrap should expose the cause")
	}
}

func TestErrorIs(t *testing.T) {
	err1 := ago.NewError[ago.NoDetails]("SAME_CODE", "msg1")
	err2 := ago.NewError[ago.NoDetails]("SAME_CODE", "msg2")
	err3 := ago.NewError[ago.NoDetails]("DIFFERENT", "msg3")

	if !errors.Is(err1, err2) {
		t.Error("errors with same code should match via Is")
	}
	if errors.Is(err1, err3) {
		t.Error("errors with different codes should not match")
	}
}

func TestPreDefinedErrors(t *testing.T) {
	if ago.ErrToolNotFound.Code() != "TOOL_NOT_FOUND" {
		t.Error("ErrToolNotFound has wrong code")
	}
	if ago.ErrValidation.Code() != "VALIDATION_FAILED" {
		t.Error("ErrValidation has wrong code")
	}
	if ago.ErrPanicked.Code() != "TOOL_PANICKED" {
		t.Error("ErrPanicked has wrong code")
	}
}

func TestNewValidationError(t *testing.T) {
	fields := []ago.ValidationFieldError{
		{Field: "name", Message: "required"},
		{Field: "email", Message: "invalid format"},
	}
	err := ago.NewValidationError(fields)

	if !errors.Is(err, ago.ErrValidation) {
		t.Error("NewValidationError should match ErrValidation")
	}

	details := err.Details()
	if len(details.Fields) != 2 {
		t.Errorf("expected 2 field errors, got %d", len(details.Fields))
	}
}
