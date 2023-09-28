package depot

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
)

// SBOMsLabel is the key for the SBOM attestation.
const SBOMsLabel = "depot/sboms"

type SBOM struct {
	// Platform is the specific platform that was scanned.
	Platform string `json:"platform"`
	// Digest is the content digest of the SBOM.
	Digest string `json:"digest"`
	// If an image was created this is the image name and digest of the scanned SBOM.
	Image *ImageSBOM `json:"image"`
}

// ImageSBOM describes an image that is described by an SBOM.
type ImageSBOM struct {
	// Name is the image name and tag.
	Name string `json:"name"`
	// ManifestDigest is the digest of the manifest and can be used
	// to pull the image such as:
	// docker pull goller/xmarks@sha256:6839c1808eab334a9b0f400f119773a0a7d494631c083aef6d3447e3798b544f
	ManifestDigest string `json:"manifest_digest"`
}

func EncodeSBOMs(sboms []SBOM) (string, error) {
	octets := new(bytes.Buffer)
	b64 := base64.NewEncoder(base64.StdEncoding, octets)
	if err := json.NewEncoder(b64).Encode(sboms); err != nil {
		return "", err
	}
	_ = b64.Close()
	return octets.String(), nil
}

func DecodeSBOMs(encodedSBOMs string) ([]SBOM, error) {
	b64 := base64.NewDecoder(base64.StdEncoding, bytes.NewBufferString(encodedSBOMs))
	var sboms []SBOM
	err := json.NewDecoder(b64).Decode(&sboms)
	return sboms, err
}
