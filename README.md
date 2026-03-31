# ago

[![CI Status](https://github.com/zoobz-io/ago/workflows/CI/badge.svg)](https://github.com/zoobz-io/ago/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/zoobz-io/ago/graph/badge.svg?branch=main)](https://codecov.io/gh/zoobz-io/ago)
[![Go Report Card](https://goreportcard.com/badge/github.com/zoobz-io/ago)](https://goreportcard.com/report/github.com/zoobz-io/ago)
[![CodeQL](https://github.com/zoobz-io/ago/workflows/CodeQL/badge.svg)](https://github.com/zoobz-io/ago/security/code-scanning)
[![Go Reference](https://pkg.go.dev/badge/github.com/zoobz-io/ago.svg)](https://pkg.go.dev/github.com/zoobz-io/ago)
[![License](https://img.shields.io/github/license/zoobz-io/ago)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/zoobz-io/ago)](go.mod)
[![Release](https://img.shields.io/github/v/release/zoobz-io/ago)](https://github.com/zoobz-io/ago/releases)

Typed LLM tool execution framework for Go. The same role [rocco](https://github.com/zoobz-io/rocco) plays for HTTP, ago plays for LLM tool dispatch.

ago ("I do" in Latin) is where an LLM's decisions become validated, scoped, auditable actions in the host system. It sits alongside [cogito](https://github.com/zoobz-io/cogito) ("I think") and [zyn](https://github.com/zoobz-io/zyn) (typed LLM interactions) in the zoobzio AI stack.

## Features

- **Typed tool handlers** with generic input/output via `Tool[In, Out]`
- **Automatic JSON Schema** generation from Go types via [sentinel](https://github.com/zoobz-io/sentinel)
- **Input validation** with JSON deserialization and `Validatable` interface
- **Middleware chain** for cross-cutting concerns (auth, logging, rate limiting)
- **Typed errors** distinguishing tool errors (LLM can retry) from dispatch errors (system failure)
- **Observability** via [capitan](https://github.com/zoobz-io/capitan) lifecycle signals
- **Panic recovery** on every tool invocation

## Install

```bash
go get github.com/zoobz-io/ago
```

## Quick Start

```go
// Define input and output types.
type SearchInput struct {
    Query string `json:"query" desc:"The search query"`
    Limit int    `json:"limit" desc:"Max results to return"`
}

type SearchOutput struct {
    Results []string `json:"results"`
    Total   int      `json:"total"`
}

// Create a typed tool.
search := ago.NewTool[SearchInput, SearchOutput]("search",
    func(ctx context.Context, inv *ago.Invocation) (SearchOutput, error) {
        input, _ := ago.TypedInput[SearchInput](inv)
        results := doSearch(ctx, input.Query, input.Limit)
        return SearchOutput{Results: results, Total: len(results)}, nil
    },
).WithDescription("Search for items by query")

// Register and invoke.
registry := ago.NewRegistry()
registry.Register(search)

result, err := registry.Invoke(ctx, "search", []byte(`{"query":"go","limit":10}`), identity)
```

## Schema Generation

Generate tool schemas for LLM APIs directly from registered tools:

```go
schemas := ago.GenerateSchemas(registry)
// schemas is []ToolSchema with name, description, and JSON Schema for each tool
```

## Middleware

```go
// Global middleware applies to all tools.
registry.WithMiddleware(func(next ago.HandlerFunc) ago.HandlerFunc {
    return func(ctx context.Context, inv *ago.Invocation) (*ago.Result, error) {
        log.Printf("tool=%s id=%s", inv.ToolName, inv.ID)
        return next(ctx, inv)
    }
})

// Per-tool middleware.
tool.WithMiddleware(rateLimitMiddleware, auditMiddleware)
```

## Error Handling

Tool errors are returned to the LLM as structured results. Dispatch errors are system failures.

```go
// Define a tool error the LLM can act on.
var ErrNotFound = ago.NewError[NotFoundDetails]("NOT_FOUND", "resource not found")

// Return it from a handler — it becomes a tool_result, not a crash.
func handler(ctx context.Context, inv *ago.Invocation) (Output, error) {
    return Output{}, ErrNotFound.WithDetails(NotFoundDetails{Resource: "user"})
}
```

## Observability

All lifecycle events are emitted as [capitan](https://github.com/zoobz-io/capitan) signals:

| Signal | When |
|--------|------|
| `ToolRegistered` | Tool added to registry |
| `ToolExecutionStarted` | Invocation dispatched |
| `ToolExecutionCompleted` | Invocation succeeded |
| `ToolExecutionFailed` | Invocation failed |

## Dependencies

| Package | Role |
|---------|------|
| [capitan](https://github.com/zoobz-io/capitan) | Lifecycle signal emission |
| [sentinel](https://github.com/zoobz-io/sentinel) | Type metadata for schema generation |

## License

[MIT](LICENSE)
