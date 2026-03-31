package ago

import "encoding/json"

// Result represents the outcome of a tool invocation.
// Either Output is set (success) or Error is set (tool-level error).
// Tool-level errors are distinct from dispatch errors — they are
// serialized as tool_result content and fed back to the LLM.
type Result struct {
	// Output is the successful result data (nil on error).
	Output any

	// Error is a tool-level error result (nil on success).
	// The LLM receives this and can adapt its approach.
	Error ErrorDefinition

	// Metadata carries additional response metadata.
	Metadata map[string]any
}

// NewResult creates a successful result.
func NewResult(output any) *Result {
	return &Result{Output: output}
}

// NewErrorResult creates an error result from a tool error.
func NewErrorResult(err ErrorDefinition) *Result {
	return &Result{Error: err}
}

// IsError returns true if this result represents a tool error.
func (r *Result) IsError() bool {
	return r.Error != nil
}

// errorResponse is the JSON structure for tool error results.
type errorResponse struct {
	IsError bool   `json:"is_error"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

// MarshalJSON serializes the result for LLM consumption.
// Success results marshal the output directly.
// Error results marshal as a structured error object.
func (r *Result) MarshalJSON() ([]byte, error) {
	if r.IsError() {
		return json.Marshal(errorResponse{
			IsError: true,
			Code:    r.Error.Code(),
			Message: r.Error.Message(),
			Details: r.Error.DetailsAny(),
		})
	}
	return json.Marshal(r.Output)
}
