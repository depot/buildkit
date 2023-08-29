package depotcache

import (
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

func unlazySessionOf(opts ...cache.RefOption) session.Group {
	for _, opt := range opts {
		if opt, ok := opt.(session.Group); ok {
			return opt
		}
	}
	return nil
}
