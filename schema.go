package ago

import (
	"strconv"
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
	Enum                 []any                  `json:"enum,omitempty"`
	AdditionalProperties *bool                  `json:"additionalProperties,omitempty"`
	// Numeric constraints
	Minimum          *float64 `json:"minimum,omitempty"`
	Maximum          *float64 `json:"maximum,omitempty"`
	ExclusiveMinimum *float64 `json:"exclusiveMinimum,omitempty"`
	ExclusiveMaximum *float64 `json:"exclusiveMaximum,omitempty"`
	// String constraints
	MinLength *int   `json:"minLength,omitempty"`
	MaxLength *int   `json:"maxLength,omitempty"`
	Pattern   string `json:"pattern,omitempty"`
	Format    string `json:"format,omitempty"`
	// Array constraints
	MinItems    *int  `json:"minItems,omitempty"`
	MaxItems    *int  `json:"maxItems,omitempty"`
	UniqueItems *bool `json:"uniqueItems,omitempty"`
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

		// Apply desc tag.
		if desc, ok := field.Tags["desc"]; ok && desc != "" {
			prop.Description = desc
		}

		// Apply validate tag constraints.
		if validateTag, ok := field.Tags["validate"]; ok && validateTag != "" {
			applyValidateConstraints(prop, validateTag, field.Type)
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

// applyValidateConstraints parses go-playground/validator tags and applies
// JSON Schema constraints. Ported from rocco/docs.go parseValidateTag.
func applyValidateConstraints(schema *JSONSchema, validateTag, goType string) {
	rules := strings.Split(validateTag, ",")

	baseType := strings.TrimPrefix(goType, "*")
	baseType = strings.TrimPrefix(baseType, "[]")
	isArray := strings.HasPrefix(strings.TrimPrefix(goType, "*"), "[]")
	isNumeric := isNumericType(baseType)
	isString := baseType == goTypeString

	for _, rule := range rules {
		rule = strings.TrimSpace(rule)
		if rule == "" {
			continue
		}

		parts := strings.SplitN(rule, "=", 2)
		tag := parts[0]
		var param string
		if len(parts) > 1 {
			param = parts[1]
		}

		switch tag {
		case "min":
			if isNumeric {
				schema.Minimum = parseFloat(param)
			} else if isString {
				schema.MinLength = parseInt(param)
			}
		case "max":
			if isNumeric {
				schema.Maximum = parseFloat(param)
			} else if isString {
				schema.MaxLength = parseInt(param)
			}
		case "len":
			if isArray {
				v := parseInt(param)
				schema.MinItems = v
				schema.MaxItems = v
			} else if isString {
				v := parseInt(param)
				schema.MinLength = v
				schema.MaxLength = v
			}
		case "gte":
			if isNumeric {
				schema.Minimum = parseFloat(param)
			}
		case "lte":
			if isNumeric {
				schema.Maximum = parseFloat(param)
			}
		case "gt":
			if isNumeric {
				schema.ExclusiveMinimum = parseFloat(param)
			}
		case "lt":
			if isNumeric {
				schema.ExclusiveMaximum = parseFloat(param)
			}
		case "email":
			schema.Format = "email"
		case "url":
			schema.Format = "uri"
		case "uuid", "uuid4", "uuid5":
			schema.Format = "uuid"
		case "datetime":
			schema.Format = "date-time"
		case "ipv4":
			schema.Format = "ipv4"
		case "ipv6":
			schema.Format = "ipv6"
		case "unique":
			if isArray {
				t := true
				schema.UniqueItems = &t
			}
		case "oneof":
			if param != "" {
				values := strings.Split(param, " ")
				enumValues := make([]any, 0, len(values))
				for _, v := range values {
					v = strings.TrimSpace(v)
					if v == "" {
						continue
					}
					if isNumeric {
						if iv, err := strconv.Atoi(v); err == nil {
							enumValues = append(enumValues, iv)
						}
					} else {
						enumValues = append(enumValues, v)
					}
				}
				if len(enumValues) > 0 {
					schema.Enum = enumValues
				}
			}
		case "required":
			// No-op: required is determined by json tag.
		}
	}
}

// Go type name constant (used for type detection in validate tag parsing).
const goTypeString = "string"

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

// isNumericType checks if a Go type name is numeric.
func isNumericType(goType string) bool {
	switch goType {
	case "int", "int8", "int16", "int32", "int64",
		"uint", "uint8", "uint16", "uint32", "uint64",
		"float32", "float64":
		return true
	default:
		return false
	}
}

// jsonFieldName returns the JSON property name for a field.
func jsonFieldName(field sentinel.FieldMetadata) string {
	if jsonTag, ok := field.Tags["json"]; ok && jsonTag != "" {
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
	elemType := strings.TrimPrefix(field.Type, "[]")
	if elemType == "" {
		return nil
	}

	jsonType := scalarToJSONType(elemType)
	if jsonType != jsonTypeString || elemType == goTypeString {
		return &JSONSchema{Type: jsonType}
	}

	if fqdn, ok := relIndex[field.Name]; ok {
		if meta, ok := sentinel.Lookup(fqdn); ok {
			return metadataToSchema(meta)
		}
	}

	if meta, ok := sentinel.Lookup(elemType); ok {
		return metadataToSchema(meta)
	}

	return &JSONSchema{Type: jsonTypeString}
}

// parseFloat parses a string to a *float64.
func parseFloat(s string) *float64 {
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		return &v
	}
	return nil
}

// parseInt parses a string to a *int.
func parseInt(s string) *int {
	if v, err := strconv.Atoi(s); err == nil {
		return &v
	}
	return nil
}
