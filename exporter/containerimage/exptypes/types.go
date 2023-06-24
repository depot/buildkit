package exptypes

import (
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	ExporterConfigDigestKey      = "config.digest"
	ExporterImageDigestKey       = "containerimage.digest"
	ExporterImageConfigKey       = "containerimage.config"
	ExporterImageConfigDigestKey = "containerimage.config.digest"
	ExporterImageDescriptorKey   = "containerimage.descriptor"
	ExporterInlineCache          = "containerimage.inlinecache"
	ExporterPlatformsKey         = "refs.platforms"

	// DEPOT: added to receive the manifest directly for the image.
	DepotContainerImageIndex    = "depot.containerimage.index"
	DepotContainerImageManifest = "depot.containerimage.manifest"
	DepotContainerImageConfig   = "depot.containerimage.config"
)

// KnownRefMetadataKeys are the subset of exporter keys that can be suffixed by
// a platform to become platform specific
var KnownRefMetadataKeys = []string{
	ExporterImageConfigKey,
	ExporterInlineCache,
}

type Platforms struct {
	Platforms []Platform
}

type Platform struct {
	ID       string
	Platform ocispecs.Platform
}
