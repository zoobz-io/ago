package ago

import (
	"context"
	"encoding/json"

	"github.com/zoobz-io/herald"
	"github.com/zoobz-io/pipz"
)

// Publish publishes the flow's payload to a message broker via herald.
func Publish[T any](identity pipz.Identity, provider herald.Provider) pipz.Chainable[*Flow[T]] {
	return pipz.Apply(identity, func(ctx context.Context, f *Flow[T]) (*Flow[T], error) {
		data, err := json.Marshal(f.Payload)
		if err != nil {
			return f, err
		}

		metadata := make(herald.Metadata, len(f.Metadata)+2)
		for k, v := range f.Metadata {
			metadata[k] = v
		}
		if f.CorrelationID != "" {
			metadata["correlation_id"] = f.CorrelationID
		}
		if f.CausationID != "" {
			metadata["causation_id"] = f.CausationID
		}

		err = provider.Publish(ctx, data, metadata)
		return f, err
	})
}
