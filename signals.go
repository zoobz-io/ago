package ago

import "github.com/zoobz-io/capitan"

// Tool lifecycle signals.
var (
	// ToolRegistered is emitted when a tool is added to a registry.
	ToolRegistered = capitan.NewSignal("ago.tool.registered", "Tool registered with registry")

	// ToolExecutionStarted is emitted when a tool invocation is dispatched.
	ToolExecutionStarted = capitan.NewSignal("ago.tool.execution.started", "Tool execution dispatched")

	// ToolExecutionCompleted is emitted when a tool invocation succeeds.
	ToolExecutionCompleted = capitan.NewSignal("ago.tool.execution.completed", "Tool execution completed successfully")

	// ToolExecutionFailed is emitted when a tool invocation fails.
	ToolExecutionFailed = capitan.NewSignal("ago.tool.execution.failed", "Tool execution failed with error")

	// ToolUndeclaredError is emitted when a handler returns an ErrorDefinition
	// not registered via WithErrors. This is a programming error — the developer
	// should declare all tool errors. The error is still returned to the LLM.
	ToolUndeclaredError = capitan.NewSignal("ago.tool.error.undeclared", "Tool returned undeclared error")
)
