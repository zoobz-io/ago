package ago_test

import (
	"encoding/json"
	"testing"

	"github.com/zoobz-io/ago"
)

type SearchInput struct {
	Query   string   `json:"query" desc:"The search query string"`
	Limit   int      `json:"limit" desc:"Maximum results to return"`
	Tags    []string `json:"tags,omitempty" desc:"Filter by tags"`
	Verbose bool     `json:"verbose,omitempty" desc:"Include verbose output"`
}

type SearchOutput struct {
	Results []string `json:"results"`
	Total   int      `json:"total"`
}

func TestGenerateSchema(t *testing.T) {
	tool := ago.NewTool[SearchInput, SearchOutput]("search", func(_ *ago.ToolRequest[SearchInput]) (SearchOutput, error) {
		return SearchOutput{}, nil
	}).WithDescription("Search for items")

	schema := ago.GenerateSchema(tool)

	if schema.Name != "search" {
		t.Errorf("expected name 'search', got %q", schema.Name)
	}
	if schema.Description != "Search for items" {
		t.Errorf("expected description, got %q", schema.Description)
	}
	if schema.InputSchema == nil {
		t.Fatal("expected input schema")
	}
	if schema.InputSchema.Type != "object" {
		t.Errorf("expected type 'object', got %q", schema.InputSchema.Type)
	}

	// Check properties exist.
	props := schema.InputSchema.Properties
	if props == nil {
		t.Fatal("expected properties")
	}

	queryProp, ok := props["query"]
	if !ok {
		t.Fatal("expected 'query' property")
	}
	if queryProp.Type != "string" {
		t.Errorf("expected query type 'string', got %q", queryProp.Type)
	}
	if queryProp.Description != "The search query string" {
		t.Errorf("expected query description, got %q", queryProp.Description)
	}

	limitProp, ok := props["limit"]
	if !ok {
		t.Fatal("expected 'limit' property")
	}
	if limitProp.Type != "integer" {
		t.Errorf("expected limit type 'integer', got %q", limitProp.Type)
	}

	tagsProp, ok := props["tags"]
	if !ok {
		t.Fatal("expected 'tags' property")
	}
	if tagsProp.Type != "array" {
		t.Errorf("expected tags type 'array', got %q", tagsProp.Type)
	}

	verboseProp, ok := props["verbose"]
	if !ok {
		t.Fatal("expected 'verbose' property")
	}
	if verboseProp.Type != "boolean" {
		t.Errorf("expected verbose type 'boolean', got %q", verboseProp.Type)
	}
}

func TestGenerateSchemaRequired(t *testing.T) {
	tool := ago.NewTool[SearchInput, SearchOutput]("search", func(_ *ago.ToolRequest[SearchInput]) (SearchOutput, error) {
		return SearchOutput{}, nil
	})

	schema := ago.GenerateSchema(tool)

	// query and limit don't have omitempty — they should be required.
	// tags and verbose have omitempty — they should not be required.
	required := schema.InputSchema.Required
	requiredMap := make(map[string]bool)
	for _, r := range required {
		requiredMap[r] = true
	}

	if !requiredMap["query"] {
		t.Error("expected 'query' to be required")
	}
	if !requiredMap["limit"] {
		t.Error("expected 'limit' to be required")
	}
	if requiredMap["tags"] {
		t.Error("'tags' should not be required (has omitempty)")
	}
	if requiredMap["verbose"] {
		t.Error("'verbose' should not be required (has omitempty)")
	}
}

func TestGenerateSchemaNoInput(t *testing.T) {
	tool := ago.NewTool[ago.NoInput, ago.NoOutput]("noop", func(_ *ago.ToolRequest[ago.NoInput]) (ago.NoOutput, error) {
		return ago.NoOutput{}, nil
	})

	schema := ago.GenerateSchema(tool)
	if schema.InputSchema.Type != "object" {
		t.Errorf("expected object type for NoInput, got %q", schema.InputSchema.Type)
	}
	if len(schema.InputSchema.Properties) != 0 {
		t.Errorf("expected no properties for NoInput, got %d", len(schema.InputSchema.Properties))
	}
}

func TestGenerateSchemas(t *testing.T) {
	r := ago.NewRegistry()
	r.Register(ago.NewTool[ago.NoInput, ago.NoOutput]("tool1", func(_ *ago.ToolRequest[ago.NoInput]) (ago.NoOutput, error) {
		return ago.NoOutput{}, nil
	}))
	r.Register(ago.NewTool[EchoInput, EchoOutput]("tool2", func(_ *ago.ToolRequest[EchoInput]) (EchoOutput, error) {
		return EchoOutput{}, nil
	}))

	schemas := ago.GenerateSchemas(r)
	if len(schemas) != 2 {
		t.Errorf("expected 2 schemas, got %d", len(schemas))
	}
}

func TestSchemaJSON(t *testing.T) {
	tool := ago.NewTool[EchoInput, EchoOutput]("echo", func(_ *ago.ToolRequest[EchoInput]) (EchoOutput, error) {
		return EchoOutput{}, nil
	}).WithDescription("Echo tool")

	schema := ago.GenerateSchema(tool)

	data, err := json.Marshal(schema)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	// Should be valid JSON.
	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if parsed["name"] != "echo" {
		t.Errorf("expected name 'echo', got %v", parsed["name"])
	}
}

func TestSchemaAdditionalPropertiesFalse(t *testing.T) {
	tool := ago.NewTool[EchoInput, EchoOutput]("echo", func(_ *ago.ToolRequest[EchoInput]) (EchoOutput, error) {
		return EchoOutput{}, nil
	})

	schema := ago.GenerateSchema(tool)

	if schema.InputSchema.AdditionalProperties == nil {
		t.Fatal("expected additionalProperties to be set")
	}
	if *schema.InputSchema.AdditionalProperties {
		t.Error("expected additionalProperties to be false")
	}
}

// Nested struct types for testing recursive schema generation.
type ItemDetail struct {
	Category string `json:"category" desc:"Category name"`
	Priority int    `json:"priority" desc:"Priority level"`
}

type CreateItemInput struct {
	Name    string     `json:"name" desc:"Item name"`
	Details ItemDetail `json:"details" desc:"Item details"`
}

func TestGenerateSchemaNestedStruct(t *testing.T) {
	tool := ago.NewTool[CreateItemInput, ago.NoOutput]("create_item", func(_ *ago.ToolRequest[CreateItemInput]) (ago.NoOutput, error) {
		return ago.NoOutput{}, nil
	}).WithDescription("Create an item")

	schema := ago.GenerateSchema(tool)

	props := schema.InputSchema.Properties
	if props == nil {
		t.Fatal("expected properties")
	}

	detailsProp, ok := props["details"]
	if !ok {
		t.Fatal("expected 'details' property")
	}
	if detailsProp.Type != "object" {
		t.Errorf("expected details type 'object', got %q", detailsProp.Type)
	}

	// The nested struct should have its own properties resolved.
	if detailsProp.Properties == nil {
		t.Fatal("expected nested properties on 'details' — sentinel.Scan should cache related types")
	}

	catProp, ok := detailsProp.Properties["category"]
	if !ok {
		t.Fatal("expected 'category' property in details")
	}
	if catProp.Type != "string" {
		t.Errorf("expected category type 'string', got %q", catProp.Type)
	}
	if catProp.Description != "Category name" {
		t.Errorf("expected category description, got %q", catProp.Description)
	}

	priProp, ok := detailsProp.Properties["priority"]
	if !ok {
		t.Fatal("expected 'priority' property in details")
	}
	if priProp.Type != "integer" {
		t.Errorf("expected priority type 'integer', got %q", priProp.Type)
	}
}
