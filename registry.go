package ago

import (
	"context"
	"fmt"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/zoobz-io/capitan"
)

// Registry manages tool registration and dispatch.
// The ago equivalent of rocco.Engine — register tools, dispatch by name,
// apply middleware, recover from panics.
type Registry struct {
	mu               sync.RWMutex
	tools            map[string]ToolDefinition
	globalMiddleware []Middleware
	capitan          *capitan.Capitan
}

// NewRegistry creates a new tool registry.
func NewRegistry() *Registry {
	return &Registry{
		tools: make(map[string]ToolDefinition),
	}
}

// WithMiddleware adds global middleware applied to all tool invocations.
func (r *Registry) WithMiddleware(mw ...Middleware) *Registry {
	r.globalMiddleware = append(r.globalMiddleware, mw...)
	return r
}

// WithCapitan sets a specific capitan instance for signal emission.
// If not set, signals are emitted via the default capitan.
func (r *Registry) WithCapitan(c *capitan.Capitan) *Registry {
	r.capitan = c
	return r
}

// Register adds a tool to the registry. Panics if a tool with the
// same name is already registered.
func (r *Registry) Register(tool ToolDefinition) *Registry {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := tool.Spec().Name
	if _, exists := r.tools[name]; exists {
		panic(fmt.Sprintf("ago: tool %q already registered", name))
	}
	r.tools[name] = tool

	r.emitDebug(context.Background(), ToolRegistered,
		ToolNameKey.Field(name),
	)

	return r
}

// Invoke dispatches a tool call by name.
//
// The dispatch path:
//  1. Look up the tool (ErrToolNotFound if missing)
//  2. Generate a unique execution ID
//  3. Build the middleware chain (global outer -> per-tool outer -> handler)
//  4. Execute with panic recovery
//  5. Emit lifecycle signals (started, completed/failed)
func (r *Registry) Invoke(ctx context.Context, toolName string, rawInput []byte, identity Identity) (*Result, error) {
	r.mu.RLock()
	tool, exists := r.tools[toolName]
	r.mu.RUnlock()

	if !exists {
		r.emitWarn(ctx, ToolExecutionFailed,
			ToolNameKey.Field(toolName),
			ErrorKey.Field("tool not found"),
		)
		return nil, ErrToolNotFound.WithMessage(fmt.Sprintf("tool %q not found", toolName))
	}

	if identity == nil {
		identity = NoIdentity{}
	}

	executionID := uuid.New().String()

	inv := &Invocation{
		Context:  ctx,
		ID:       executionID,
		ToolName: toolName,
		RawInput: rawInput,
		Identity: identity,
		Metadata: make(map[string]any),
	}

	// Build the handler chain.
	handler := tool.Handle

	// Apply per-tool middleware (inner to outer).
	toolMW := tool.Middleware()
	for i := len(toolMW) - 1; i >= 0; i-- {
		handler = toolMW[i](handler)
	}

	// Apply global middleware (inner to outer).
	for i := len(r.globalMiddleware) - 1; i >= 0; i-- {
		handler = r.globalMiddleware[i](handler)
	}

	// Emit started.
	r.emitInfo(ctx, ToolExecutionStarted,
		ToolNameKey.Field(toolName),
		ExecutionIDKey.Field(executionID),
	)

	start := time.Now()

	// Execute with panic recovery.
	result, err := r.safeExecute(ctx, handler, inv, toolName)

	duration := time.Since(start).Milliseconds()

	// Check for undeclared tool errors.
	if err == nil && result != nil && result.IsError() {
		if !isErrorDeclared(tool, result.Error) {
			r.emitWarn(ctx, ToolUndeclaredError,
				ToolNameKey.Field(toolName),
				ExecutionIDKey.Field(executionID),
				ErrorKey.Field(fmt.Sprintf("undeclared error %s (add to WithErrors)", result.Error.Code())),
			)
		}
	}

	// Emit completion.
	if err != nil {
		r.emitError(ctx, ToolExecutionFailed,
			ToolNameKey.Field(toolName),
			ExecutionIDKey.Field(executionID),
			DurationKey.Field(duration),
			ErrorKey.Field(err.Error()),
		)
	} else {
		r.emitInfo(ctx, ToolExecutionCompleted,
			ToolNameKey.Field(toolName),
			ExecutionIDKey.Field(executionID),
			DurationKey.Field(duration),
		)
	}

	return result, err
}

// safeExecute wraps handler execution with panic recovery.
func (*Registry) safeExecute(ctx context.Context, handler HandlerFunc, inv *Invocation, toolName string) (result *Result, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			stack := string(debug.Stack())
			err = ErrPanicked.
				WithMessage(fmt.Sprintf("tool %q panicked: %v", toolName, rec)).
				WithDetails(PanicDetails{
					Value: fmt.Sprintf("%v", rec),
					Stack: stack,
				})
		}
	}()
	return handler(ctx, inv)
}

// Tools returns all registered tool definitions, sorted by name.
func (r *Registry) Tools() []ToolDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()
	tools := make([]ToolDefinition, 0, len(r.tools))
	for _, t := range r.tools {
		tools = append(tools, t)
	}
	slices.SortFunc(tools, func(a, b ToolDefinition) int {
		return strings.Compare(a.Spec().Name, b.Spec().Name)
	})
	return tools
}

// Tool returns a specific tool by name, or nil if not found.
func (r *Registry) Tool(name string) ToolDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.tools[name]
}

// Len returns the number of registered tools.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.tools)
}

// isErrorDeclared checks if a tool error was declared via WithErrors.
func isErrorDeclared(tool ToolDefinition, err ErrorDefinition) bool {
	for _, d := range tool.ErrorDefs() {
		if d.Code() == err.Code() {
			return true
		}
	}
	return false
}

// emitDebug dispatches a debug-level capitan signal.
func (r *Registry) emitDebug(ctx context.Context, signal capitan.Signal, fields ...capitan.Field) {
	if r.capitan != nil {
		r.capitan.Debug(ctx, signal, fields...)
	} else {
		capitan.Debug(ctx, signal, fields...)
	}
}

// emitInfo dispatches an info-level capitan signal.
func (r *Registry) emitInfo(ctx context.Context, signal capitan.Signal, fields ...capitan.Field) {
	if r.capitan != nil {
		r.capitan.Info(ctx, signal, fields...)
	} else {
		capitan.Info(ctx, signal, fields...)
	}
}

// emitWarn dispatches a warn-level capitan signal.
func (r *Registry) emitWarn(ctx context.Context, signal capitan.Signal, fields ...capitan.Field) {
	if r.capitan != nil {
		r.capitan.Warn(ctx, signal, fields...)
	} else {
		capitan.Warn(ctx, signal, fields...)
	}
}

// emitError dispatches an error-level capitan signal.
func (r *Registry) emitError(ctx context.Context, signal capitan.Signal, fields ...capitan.Field) {
	if r.capitan != nil {
		r.capitan.Error(ctx, signal, fields...)
	} else {
		capitan.Error(ctx, signal, fields...)
	}
}
