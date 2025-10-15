package depot

import (
	"hash"

	"github.com/minio/sha256-simd"
	digest "github.com/opencontainers/go-digest"
)

type FastDigester struct {
	hash hash.Hash
}

func NewFastDigester() *FastDigester {
	return &FastDigester{
		hash: sha256.New(),
	}
}

func (d *FastDigester) Hash() hash.Hash {
	return d.hash
}

func (d *FastDigester) Digest() digest.Digest {
	return digest.NewDigest(digest.SHA256, d.hash)
}
