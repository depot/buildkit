package containerimage

import (
	"strconv"
	"time"

	cacheconfig "github.com/moby/buildkit/cache/config"
	"github.com/moby/buildkit/exporter/util/epoch"
	"github.com/moby/buildkit/util/compression"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	keyImageName               = "name"
	keyLayerCompression        = "compression"
	keyCompressionLevel        = "compression-level"
	keyForceCompression        = "force-compression"
	keyOCITypes                = "oci-mediatypes"
	keyBuildInfo               = "buildinfo"
	keyBuildInfoAttrs          = "buildinfo-attrs"
	keyForceInlineAttestations = "attestation-inline"

	// preferNondistLayersKey is an exporter option which can be used to mark a layer as non-distributable if the layer reference was
	// already found to use a non-distributable media type.
	// When this option is not set, the exporter will change the media type of the layer to a distributable one.
	keyPreferNondistLayers = "prefer-nondist-layers"

	// DepotExportImageVersion returns the manifest and config for the image via response.
	// Previously, we returned it via annotations, but that was not compatible with GCR.
	DepotExportImageVersion = "depot.export.image.version"
)

type ImageCommitOpts struct {
	ImageName   string
	RefCfg      cacheconfig.RefConfig
	OCITypes    bool
	Annotations AnnotationsGroup
	Epoch       *time.Time

	ForceInlineAttestations bool // force inline attestations to be attached

	BuildInfo      bool // Deprecated: Build information is deprecated: https://github.com/moby/buildkit/blob/master/docs/deprecated.md
	BuildInfoAttrs bool // Deprecated: Build information is deprecated: https://github.com/moby/buildkit/blob/master/docs/deprecated.md

	// DEPOT: ExportImageVersion determines the response format to the CLI.
	// Previously (aka v1) we returned the manifest and config via annotations.
	// In V2 we return the manifest and config via response.
	ExportImageVersion ExportImageVersion
}

type ExportImageVersion int

const (
	ExportImageVersionUnknown ExportImageVersion = iota
	// ExportImageVersionV1 is the default version for backwards compatibility.
	// It uses annotations to return the manifest and config.
	ExportImageVersionV1
	// ExportImageVersionV2 returns the manifest and config via response.
	ExportImageVersionV2
)

func (c *ImageCommitOpts) Load(opt map[string]string) (map[string]string, error) {
	rest := make(map[string]string)

	as, optb, err := ParseAnnotations(toBytesMap(opt))
	if err != nil {
		return nil, err
	}
	opt = toStringMap(optb)

	c.Epoch, opt, err = epoch.ParseExporterAttrs(opt)
	if err != nil {
		return nil, err
	}

	// DEPOT: This is the default version of our export response format.
	// It would add the manifest and config to the annotations.
	c.ExportImageVersion = ExportImageVersionV1

	for k, v := range opt {
		var err error
		switch k {
		case keyImageName:
			c.ImageName = v
		case keyLayerCompression:
			c.RefCfg.Compression.Type, err = compression.Parse(v)
		case keyCompressionLevel:
			ii, err2 := strconv.ParseInt(v, 10, 64)
			if err != nil {
				err = errors.Wrapf(err2, "non-int value %s specified for %s", v, k)
				break
			}
			v := int(ii)
			c.RefCfg.Compression.Level = &v
		case keyForceCompression:
			err = parseBoolWithDefault(&c.RefCfg.Compression.Force, k, v, true)
		case keyOCITypes:
			err = parseBoolWithDefault(&c.OCITypes, k, v, true)
		case keyBuildInfo:
			err = parseBoolWithDefault(&c.BuildInfo, k, v, true)
		case keyBuildInfoAttrs:
			err = parseBoolWithDefault(&c.BuildInfoAttrs, k, v, false)
		case keyForceInlineAttestations:
			err = parseBool(&c.ForceInlineAttestations, k, v)
		case keyPreferNondistLayers:
			err = parseBool(&c.RefCfg.PreferNonDistributable, k, v)
		case DepotExportImageVersion:
			var i int
			i, err = strconv.Atoi(v)
			if err != nil {
				break
			}
			c.ExportImageVersion = ExportImageVersion(i)
		default:
			rest[k] = v
		}

		if err != nil {
			return nil, err
		}
	}

	if c.RefCfg.Compression.Type.OnlySupportOCITypes() {
		c.EnableOCITypes(c.RefCfg.Compression.Type.String())
	}

	if c.RefCfg.Compression.Type.NeedsForceCompression() {
		c.EnableForceCompression(c.RefCfg.Compression.Type.String())
	}

	c.Annotations = c.Annotations.Merge(as)

	return rest, nil
}

func (c *ImageCommitOpts) EnableOCITypes(reason string) {
	if !c.OCITypes {
		message := "forcibly turning on oci-mediatype mode"
		if reason != "" {
			message += " for " + reason
		}
		logrus.Warn(message)

		c.OCITypes = true
	}
}

func (c *ImageCommitOpts) EnableForceCompression(reason string) {
	if !c.RefCfg.Compression.Force {
		message := "forcibly turning on force-compression mode"
		if reason != "" {
			message += " for " + reason
		}
		logrus.Warn(message)

		c.RefCfg.Compression.Force = true
	}
}

func parseBool(dest *bool, key string, value string) error {
	b, err := strconv.ParseBool(value)
	if err != nil {
		return errors.Wrapf(err, "non-bool value specified for %s", key)
	}
	*dest = b
	return nil
}

func parseBoolWithDefault(dest *bool, key string, value string, defaultValue bool) error {
	if value == "" {
		*dest = defaultValue
		return nil
	}
	return parseBool(dest, key, value)
}

func toBytesMap(m map[string]string) map[string][]byte {
	result := make(map[string][]byte)
	for k, v := range m {
		result[k] = []byte(v)
	}
	return result
}

func toStringMap(m map[string][]byte) map[string]string {
	result := make(map[string]string)
	for k, v := range m {
		result[k] = string(v)
	}
	return result
}
