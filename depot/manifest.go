package depot

import (
	"encoding/base64"
	"encoding/json"

	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
)

// ExportManifests is an array of base64-encoded JSON ocispec Manifests.
const ExportManifests = "depot/export.manifests"

// MarshalManifests encodes the manifests to a format that can
// be sent using the Solve Response.  Solve Response is a map[string]string.
func MarshalManifests(manifests []*ocispecs.Manifest) (string, error) {
	octets, err := json.Marshal(manifests)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(octets), nil
}

// UnmarshalManifests decodes the manifests from a Solve Response.
func UnmarshalManifests(data string) ([]ocispecs.Manifest, error) {
	octets, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return nil, err
	}

	var manifests []ocispecs.Manifest
	err = json.Unmarshal(octets, &manifests)
	return manifests, err
}
