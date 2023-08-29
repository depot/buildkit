package depotcache

import (
	"fmt"

	"github.com/moby/buildkit/cache"
	"github.com/moby/buildkit/session"
	digest "github.com/opencontainers/go-digest"
)

func descHandlersOf(opts ...cache.RefOption) cache.DescHandlers {
	for _, opt := range opts {
		if opt, ok := opt.(cache.DescHandlers); ok {
			return opt
		}
	}
	return nil
}

type DescHandlerKey digest.Digest

type NeedsRemoteProviderError []digest.Digest //nolint:errname

func (m NeedsRemoteProviderError) Error() string {
	return fmt.Sprintf("missing descriptor handlers for lazy blobs %+v", []digest.Digest(m))
}

func unlazySessionOf(opts ...cache.RefOption) session.Group {
	for _, opt := range opts {
		if opt, ok := opt.(session.Group); ok {
			return opt
		}
	}
	return nil
}
