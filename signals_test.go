package ago_test

import (
	"testing"

	"github.com/zoobz-io/ago"
)

func TestSignalNames(t *testing.T) {
	signals := map[string]struct {
		name string
	}{
		"ToolRegistered":         {name: "ago.tool.registered"},
		"ToolExecutionStarted":   {name: "ago.tool.execution.started"},
		"ToolExecutionCompleted": {name: "ago.tool.execution.completed"},
		"ToolExecutionFailed":    {name: "ago.tool.execution.failed"},
	}

	actual := map[string]string{
		"ToolRegistered":         ago.ToolRegistered.Name(),
		"ToolExecutionStarted":   ago.ToolExecutionStarted.Name(),
		"ToolExecutionCompleted": ago.ToolExecutionCompleted.Name(),
		"ToolExecutionFailed":    ago.ToolExecutionFailed.Name(),
	}

	for key, expected := range signals {
		if actual[key] != expected.name {
			t.Errorf("%s: expected signal name %q, got %q", key, expected.name, actual[key])
		}
	}
}

func TestKeyNames(t *testing.T) {
	if ago.ToolNameKey.Name() != "tool_name" {
		t.Errorf("expected tool_name, got %q", ago.ToolNameKey.Name())
	}
	if ago.ExecutionIDKey.Name() != "execution_id" {
		t.Errorf("expected execution_id, got %q", ago.ExecutionIDKey.Name())
	}
	if ago.DurationKey.Name() != "duration_ms" {
		t.Errorf("expected duration_ms, got %q", ago.DurationKey.Name())
	}
	if ago.ErrorKey.Name() != "error" {
		t.Errorf("expected error, got %q", ago.ErrorKey.Name())
	}
}
