package ago

import "github.com/zoobz-io/capitan"

// Field keys for tool lifecycle signals.
var (
	// ToolNameKey identifies the tool by name.
	ToolNameKey = capitan.NewStringKey("tool_name")

	// ExecutionIDKey is the unique identifier for a single tool invocation.
	ExecutionIDKey = capitan.NewStringKey("execution_id")

	// DurationKey is the execution time in milliseconds.
	DurationKey = capitan.NewInt64Key("duration_ms")

	// ErrorKey carries the error message string.
	ErrorKey = capitan.NewStringKey("error")
)
