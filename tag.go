package ago

import (
	"context"

	"github.com/zoobzio/pipz"
)

// Tag adds a key/value pair to the flow's broker metadata.
func Tag[T any](name pipz.Name, key, value string) pipz.Chainable[*Flow[T]] {
	return pipz.Transform(name, func(_ context.Context, f *Flow[T]) *Flow[T] {
		if f.Metadata == nil {
			f.Metadata = make(map[string]string)
		}
		f.Metadata[key] = value
		return f
	})
}

// TagFrom adds a key/value pair where the value comes from a function.
func TagFrom[T any](name pipz.Name, key string, valueFn func(T) string) pipz.Chainable[*Flow[T]] {
	return pipz.Transform(name, func(_ context.Context, f *Flow[T]) *Flow[T] {
		if f.Metadata == nil {
			f.Metadata = make(map[string]string)
		}
		f.Metadata[key] = valueFn(f.Payload)
		return f
	})
}
