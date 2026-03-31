package ago_test

import (
	"encoding/json"
	"testing"

	"github.com/zoobz-io/ago"
)

func TestNewResult(t *testing.T) {
	result := ago.NewResult("hello")

	if result.IsError() {
		t.Error("NewResult should not be an error")
	}
	if result.Output != "hello" {
		t.Errorf("expected output 'hello', got %v", result.Output)
	}
}

func TestNewErrorResult(t *testing.T) {
	toolErr := ago.NewError[ago.NoDetails]("FAIL", "something failed")
	result := ago.NewErrorResult(toolErr)

	if !result.IsError() {
		t.Error("NewErrorResult should be an error")
	}
	if result.Error.Code() != "FAIL" {
		t.Errorf("expected error code FAIL, got %q", result.Error.Code())
	}
}

func TestResultMarshalJSONSuccess(t *testing.T) {
	type Output struct {
		Value string `json:"value"`
	}
	result := ago.NewResult(Output{Value: "ok"})

	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("MarshalJSON failed: %v", err)
	}

	var parsed map[string]string
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed["value"] != "ok" {
		t.Errorf("expected value 'ok', got %q", parsed["value"])
	}
}

func TestResultMarshalJSONError(t *testing.T) {
	toolErr := ago.NewError[ago.NoDetails]("FAIL", "something failed")
	result := ago.NewErrorResult(toolErr)

	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("MarshalJSON failed: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed["is_error"] != true {
		t.Error("expected is_error to be true")
	}
	if parsed["code"] != "FAIL" {
		t.Errorf("expected code FAIL, got %v", parsed["code"])
	}
}
