package ago

import (
	"context"

	"github.com/zoobz-io/capitan"
	"github.com/zoobz-io/pipz"
)

// Enrich fetches external data and adds it to the flow's fields.
// The enrichFn receives the payload and returns a field to add.
func Enrich[T, V any](identity pipz.Identity, key capitan.GenericKey[V], enrichFn func(context.Context, T) (V, error)) pipz.Chainable[*Flow[T]] {
	return pipz.Apply(identity, func(ctx context.Context, f *Flow[T]) (*Flow[T], error) {
		value, err := enrichFn(ctx, f.Payload)
		if err != nil {
			return f, err
		}
		f.Set(key.Field(value))
		return f, nil
	})
}

// EnrichOptional fetches external data, logging but not failing on errors.
func EnrichOptional[T, V any](identity pipz.Identity, key capitan.GenericKey[V], enrichFn func(context.Context, T) (V, error)) pipz.Chainable[*Flow[T]] {
	return pipz.Transform(identity, func(ctx context.Context, f *Flow[T]) *Flow[T] {
		value, err := enrichFn(ctx, f.Payload)
		if err != nil {
			f.AddError(err)
			return f
		}
		f.Set(key.Field(value))
		return f
	})
}
