package ago

import "context"

// Invocation represents a single tool call from an LLM.
// This is the type-erased dispatch context used by middleware and the registry.
// Handlers receive a typed ToolRequest[In] instead.
type Invocation struct {
	// Context carries deadlines, cancellation, and request-scoped values.
	Context context.Context

	// ID is the unique execution identifier for this invocation.
	ID string

	// ToolName is the name of the tool being called.
	ToolName string

	// RawInput is the raw JSON bytes before deserialization.
	// Available for middleware that needs to inspect or log the raw payload.
	RawInput []byte

	// Identity is the caller identity (NoIdentity if unauthenticated).
	Identity Identity

	// Metadata carries arbitrary key-value pairs set by middleware or the caller.
	Metadata map[string]any
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

// ToolRequest is the typed request context passed to tool handlers.
// Parallel to rocco.Request[In] — Body is already deserialized and validated.
type ToolRequest[In any] struct {
	// Context carries deadlines, cancellation, and request-scoped values.
	Context context.Context

	// Body is the deserialized, validated input.
	Body In

	// Identity is the caller identity.
	Identity Identity

	// ID is the unique execution identifier.
	ID string

	// ToolName is the name of the tool being called.
	ToolName string

	// Metadata carries arbitrary key-value pairs set by middleware.
	Metadata map[string]any
}
