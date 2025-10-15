package depot

// ImagesExported is the label for solve responses containing slices of
// image manifests and configs.
//
// This is a base64-encoded JSON array of objects with the following fields:
// Manifest []byte `json:"manifest"`
// Config   []byte `json:"config"`
const ImagesExported = "depot/images.exported"

type ExportedImage struct {
	// JSON-encoded ocispecs.Manifest.
	Manifest []byte `json:"manifest"`
	// JSON-encoded ocispecs.Image.
	Config []byte `json:"config"`
}
