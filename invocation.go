package ago

import "context"

// Invocation represents a single tool call from an LLM.
// This is what tool handlers and middleware receive.
type Invocation struct {
	// Context carries deadlines, cancellation, and request-scoped values.
	Context context.Context

	// ID is the unique execution identifier for this invocation.
	ID string

	// ToolName is the name of the tool being called.
	ToolName string

	// Input is the deserialized, typed input. Set by Tool[In, Out].Handle
	// after JSON deserialization.
	Input any

	// RawInput is the raw JSON bytes before deserialization.
	// Available for middleware that needs to inspect or log the raw payload.
	RawInput []byte

	// Identity is the caller identity (NoIdentity if unauthenticated).
	Identity Identity

	// Metadata carries arbitrary key-value pairs set by middleware or the caller.
	Metadata map[string]any
}

// TypedInput extracts the typed input from an invocation.
// Returns the value and true if the type assertion succeeds.
func TypedInput[In any](inv *Invocation) (In, bool) {
	v, ok := inv.Input.(In)
	return v, ok
}

// SetMeta sets a metadata key-value pair on the invocation.
func (inv *Invocation) SetMeta(key string, value any) {
	if inv.Metadata == nil {
		inv.Metadata = make(map[string]any)
	}
	inv.Metadata[key] = value
}

// GetMeta retrieves a metadata value by key.
func (inv *Invocation) GetMeta(key string) (any, bool) {
	if inv.Metadata == nil {
		return nil, false
	}
	v, ok := inv.Metadata[key]
	return v, ok
}
