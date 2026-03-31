package ago

import (
	"strings"

	"github.com/zoobz-io/sentinel"
)

// ToolSchema represents a tool's schema for LLM consumption.
// This is the format that gets passed to LLM APIs for tool definitions.
type ToolSchema struct {
	Name        string      `json:"name"`
	Description string      `json:"description,omitempty"`
	InputSchema *JSONSchema `json:"input_schema"`
}

// JSONSchema is a subset of JSON Schema sufficient for LLM tool definitions.
type JSONSchema struct {
	Type                 string                 `json:"type"`
	Properties           map[string]*JSONSchema `json:"properties,omitempty"`
	Required             []string               `json:"required,omitempty"`
	Description          string                 `json:"description,omitempty"`
	Items                *JSONSchema            `json:"items,omitempty"`
	Enum                 []string               `json:"enum,omitempty"`
	AdditionalProperties *bool                  `json:"additionalProperties,omitempty"`
}

// GenerateSchema produces a ToolSchema from a ToolDefinition.
func GenerateSchema(tool ToolDefinition) ToolSchema {
	spec := tool.Spec()
	return ToolSchema{
		Name:        spec.Name,
		Description: spec.Description,
		InputSchema: metadataToSchema(tool.InputMeta()),
	}
}

// GenerateSchemas produces ToolSchemas for all tools in a registry.
func GenerateSchemas(r *Registry) []ToolSchema {
	tools := r.Tools()
	schemas := make([]ToolSchema, len(tools))
	for i, t := range tools {
		schemas[i] = GenerateSchema(t)
	}
	return schemas
}

// metadataToSchema converts sentinel.Metadata to a JSONSchema.
func metadataToSchema(meta sentinel.Metadata) *JSONSchema {
	// NoInput and NoOutput produce empty object schemas.
	if meta.TypeName == "NoInput" || meta.TypeName == "NoOutput" {
		return &JSONSchema{Type: jsonTypeObject}
	}

	// Build a relationship index: field name -> target FQDN.
	relIndex := make(map[string]string, len(meta.Relationships))
	for _, rel := range meta.Relationships {
		relIndex[rel.Field] = rel.To
	}

	schema := &JSONSchema{
		Type:       jsonTypeObject,
		Properties: make(map[string]*JSONSchema),
	}

	for _, field := range meta.Fields {
		name := jsonFieldName(field)
		if name == "-" {
			continue
		}

		prop := fieldToSchema(field, relIndex)

		// Use desc tag for schema description.
		if desc, ok := field.Tags["desc"]; ok && desc != "" {
			prop.Description = desc
		}

		schema.Properties[name] = prop

		// Fields without omitempty are required.
		if !hasOmitempty(field) {
			schema.Required = append(schema.Required, name)
		}
	}

	// Disallow additional properties on root objects.
	f := false
	schema.AdditionalProperties = &f

	return schema
}

// fieldToSchema maps a sentinel.FieldMetadata to a JSONSchema.
// relIndex maps field names to their target FQDNs from parent relationships.
func fieldToSchema(field sentinel.FieldMetadata, relIndex map[string]string) *JSONSchema {
	switch field.Kind {
	case sentinel.KindScalar:
		return &JSONSchema{Type: scalarToJSONType(field.Type)}
	case sentinel.KindSlice:
		return &JSONSchema{
			Type:  jsonTypeArray,
			Items: sliceElementSchema(field, relIndex),
		}
	case sentinel.KindStruct:
		// Use relationship FQDN for accurate cache lookup.
		if fqdn, ok := relIndex[field.Name]; ok {
			if meta, ok := sentinel.Lookup(fqdn); ok {
				return metadataToSchema(meta)
			}
		}
		// Fallback: try field.Type directly (works when it's already a FQDN).
		if meta, ok := sentinel.Lookup(field.Type); ok {
			return metadataToSchema(meta)
		}
		return &JSONSchema{Type: jsonTypeObject}
	case sentinel.KindPointer:
		// Treat pointers as their underlying type.
		underlying := strings.TrimPrefix(field.Type, "*")
		return &JSONSchema{Type: scalarToJSONType(underlying)}
	case sentinel.KindMap:
		return &JSONSchema{Type: jsonTypeObject}
	default:
		return &JSONSchema{Type: jsonTypeString}
	}
}

// JSON Schema type constants.
const (
	jsonTypeString  = "string"
	jsonTypeInteger = "integer"
	jsonTypeNumber  = "number"
	jsonTypeBoolean = "boolean"
	jsonTypeObject  = "object"
	jsonTypeArray   = "array"
)

// scalarToJSONType maps Go scalar type names to JSON Schema types.
func scalarToJSONType(goType string) string {
	switch goType {
	case "string":
		return jsonTypeString
	case "int", "int8", "int16", "int32", "int64",
		"uint", "uint8", "uint16", "uint32", "uint64":
		return jsonTypeInteger
	case "float32", "float64":
		return jsonTypeNumber
	case "bool":
		return jsonTypeBoolean
	default:
		return jsonTypeString
	}
}

// jsonFieldName returns the JSON property name for a field.
func jsonFieldName(field sentinel.FieldMetadata) string {
	if jsonTag, ok := field.Tags["json"]; ok && jsonTag != "" {
		// Strip options like ,omitempty.
		parts := strings.Split(jsonTag, ",")
		if parts[0] != "" {
			return parts[0]
		}
	}
	return field.Name
}

// hasOmitempty checks if the json tag includes omitempty.
func hasOmitempty(field sentinel.FieldMetadata) bool {
	if jsonTag, ok := field.Tags["json"]; ok {
		return strings.Contains(jsonTag, "omitempty")
	}
	return false
}

// sliceElementSchema tries to determine the element type of a slice.
func sliceElementSchema(field sentinel.FieldMetadata, relIndex map[string]string) *JSONSchema {
	// field.Type is like "[]string" or "[]Order".
	elemType := strings.TrimPrefix(field.Type, "[]")
	if elemType == "" {
		return nil
	}

	// Check if it's a known scalar.
	jsonType := scalarToJSONType(elemType)
	if jsonType != jsonTypeString || elemType == "string" {
		return &JSONSchema{Type: jsonType}
	}

	// Use relationship FQDN if available.
	if fqdn, ok := relIndex[field.Name]; ok {
		if meta, ok := sentinel.Lookup(fqdn); ok {
			return metadataToSchema(meta)
		}
	}

	// Fallback: try element type directly.
	if meta, ok := sentinel.Lookup(elemType); ok {
		return metadataToSchema(meta)
	}

	return &JSONSchema{Type: jsonTypeString}
}
