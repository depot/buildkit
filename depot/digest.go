package depot

import "context"

func WithStableDigests(ctx context.Context, digests []string) context.Context {
	return context.WithValue(ctx, StableDigestKey{}, digests)
}

type StableDigestKey struct{}

func StableDigests(ctx context.Context) []string {
	digests, ok := ctx.Value(StableDigestKey{}).([]string)
	if !ok {
		return nil
	}
	return digests
}

func WithVertexDigest(ctx context.Context, digest string) context.Context {
	return context.WithValue(ctx, VertexDigestKey{}, digest)
}

type VertexDigestKey struct{}

func VertexDigest(ctx context.Context) string {
	digest, ok := ctx.Value(VertexDigestKey{}).(string)
	if !ok {
		return ""
	}
	return digest
}
