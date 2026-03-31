// Package ago provides a typed LLM tool execution framework.
//
// ago ("I do" in Latin) is the tool execution layer for LLM workflows.
// Where an LLM's decisions become validated, scoped, auditable actions
// in the host system. It plays the same role rocco plays for HTTP, but
// for LLM tool dispatch.
//
// An LLM calls a tool by name with JSON arguments. ago validates the
// input, dispatches to a registered handler, enforces boundaries via
// middleware, and returns a typed result.
//
// Core types:
//   - Tool[In, Out] is a typed tool handler with generic input and output
//   - Registry manages tool registration and dispatch
//   - Invocation carries the tool call context to handlers
//   - Result wraps successful output or tool-level errors
//
// Tool handlers follow the same pattern as rocco endpoint handlers:
// receive context + typed input, call domain contracts, return typed output.
package ago

import "context"

// HandlerFunc is the core execution signature for tool dispatch.
// Middleware wraps this. The registry resolves tool names to this.
type HandlerFunc func(ctx context.Context, inv *Invocation) (*Result, error)

// Middleware wraps a HandlerFunc, producing a new HandlerFunc.
// Global middleware applies to all tools. Per-tool middleware applies to one.
type Middleware func(HandlerFunc) HandlerFunc

// NoInput represents tools that take no input parameters.
type NoInput struct{}

// NoOutput represents tools that produce no meaningful output.
type NoOutput struct{}
