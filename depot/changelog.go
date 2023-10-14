package depot

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"time"

	"connectrpc.com/connect"
	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/continuity/fs"
	cloudv3 "github.com/moby/buildkit/depot/api"
	"github.com/moby/buildkit/depot/api/cloudv3connect"
	"github.com/moby/buildkit/util/bklog"
	digest "github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

const ChangeLogLabel = "depot.dev/changelog"
const CreatorLabel = "depot.dev/creator"

func ChangelogFeatureEnabled() bool {
	return os.Getenv("DEPOT_CHANGELOG_ENABLED") != ""
}

// Smaller cache.RefMetadata interface avoid circ deps.
type ChangeRef interface {
	GetString(string) string
	GetStringSlice(key string) []string
}

type ChangeLog struct {
	inner fs.ChangeFunc
	Log   cloudv3.Changelog
}

func NewChangeLog(sr ChangeRef, id string) *ChangeLog {
	// Only base images have more than one stable digest. We'll choose the first one as these layers are non-base.
	ss := sr.GetStringSlice("depot.stableDigests")
	s := ""
	if len(ss) > 0 {
		s = ss[0]
	}

	return &ChangeLog{
		Log: cloudv3.Changelog{
			Id:            id,
			StableDigest:  s,
			CreatorDigest: sr.GetString("depot.vertexDigest"),
		},
	}
}

func (c *ChangeLog) New(changeFn fs.ChangeFunc) fs.ChangeFunc {
	if !ChangelogFeatureEnabled() {
		return changeFn
	}

	c.inner = changeFn
	return c.handle
}

func (c *ChangeLog) handle(change fs.ChangeKind, path string, stat os.FileInfo, statErr error) error {
	err := c.inner(change, path, stat, statErr)
	if err != nil {
		return err
	}
	if statErr != nil {
		return nil
	}

	c.Log.Paths = append(c.Log.Paths, path)

	// Only care about add, modify, and delete.
	ch := uint32(change)
	if ch > 3 {
		return nil
	}
	c.Log.Changes = append(c.Log.Changes, ch)

	var size int64
	var mode uint32
	if stat != nil {
		size = stat.Size()
		mode = uint32(stat.Mode())
	}
	c.Log.Sizes = append(c.Log.Sizes, size)
	c.Log.Modes = append(c.Log.Modes, mode)
	c.Log.Num++

	return nil
}

// WriteBlob writes the changelog to the content store and returns labels to
// add to the changed blob.
func (c *ChangeLog) WriteBlob(ctx context.Context, store content.Ingester) (map[string]string, error) {
	if !ChangelogFeatureEnabled() {
		return nil, nil
	}

	buf, _ := proto.Marshal(&c.Log)
	dgstr := NewFastDigester()
	_, _ = dgstr.Hash().Write(buf)
	dgst := dgstr.Digest()

	desc := ocispecs.Descriptor{
		Digest: dgst,
		Size:   int64(len(buf)),
		Annotations: map[string]string{
			"containerd.io/uncompressed": dgst.String(),
		},
	}

	err := content.WriteBlob(ctx, store, dgst.String(), bytes.NewReader(buf), desc)
	if err != nil {
		return nil, errors.Wrap(err, "unable to write changelog: %v")
	}

	labels := map[string]string{ChangeLogLabel: dgst.String()}
	return labels, nil
}

type SendChangeLogsOpt struct {
	SpiffeID string
	Bearer   string

	Provider   content.Provider
	Changelogs string
	ImageName  string
}

func SendChangeLogs(ctx context.Context, opt *SendChangeLogsOpt) {
	if opt.Changelogs == "" {
		return
	}

	/*
		if opt.SpiffeID == "" || opt.Bearer == "" {
			return
		}
	*/

	var descriptors []ocispecs.Descriptor
	err := json.Unmarshal([]byte(opt.Changelogs), &descriptors)
	if err != nil {
		bklog.G(ctx).WithError(err).Errorf("unable to unmarshal changelogs")
		return
	}

	var imageChangeLogs []*cloudv3.ImageChangeLogs

	// One per platform.
	for _, platformDesc := range descriptors {
		changelogArtifact, err := content.ReadBlob(ctx, opt.Provider, platformDesc)
		if err != nil {
			bklog.G(ctx).WithError(err).Errorf("unable to read platform changelog artifact blob")
			continue
		}

		var artifact ocispecs.Artifact
		err = json.Unmarshal(changelogArtifact, &artifact)
		if err != nil {
			bklog.G(ctx).WithError(err).Errorf("unable to unmarshal changelog artifact")
			return
		}

		if len(artifact.Blobs) == 0 {
			continue
		}

		imageChangeLog := cloudv3.ImageChangeLogs{
			ImageName: opt.ImageName,
		}

		for _, changelog := range artifact.Blobs {
			if changelog.Platform != nil && imageChangeLog.Platform == "" {
				imageChangeLog.Platform = platforms.Format(*changelog.Platform)
			}

			if creator, ok := changelog.Annotations[CreatorLabel]; ok {
				imageChangeLog.LayerDigests = append(imageChangeLog.LayerDigests, creator)
			}

			desc := ocispecs.Descriptor{Digest: digest.Digest(changelog.Digest)}
			buf, err := content.ReadBlob(ctx, opt.Provider, desc)
			if err != nil {
				bklog.G(ctx).WithError(err).Errorf("unable to read changelog blob")
				continue
			}

			var log cloudv3.Changelog
			err = proto.Unmarshal(buf, &log)
			if err != nil {
				bklog.G(ctx).WithError(err).Errorf("unable to unmarshal changelog")
				continue
			}
			imageChangeLog.LayerChangelogs = append(imageChangeLog.LayerChangelogs, &log)
		}

		imageChangeLogs = append(imageChangeLogs, &imageChangeLog)
	}

	{
		//octets, _ := json.Marshal(&imageChangeLogs)
		//_ = os.WriteFile("/tmp/changelogs.json", octets, 0644)
	}

	req := connect.NewRequest(&cloudv3.ReportChangeLogsRequest{
		SpiffeId: opt.SpiffeID,
		Images:   imageChangeLogs,
	})
	req.Header().Add("Authorization", opt.Bearer)

	attempts := 0
	for {
		attempts++
		client := NewDepotClient()
		if client == nil {
			break
		}

		_, err := client.ReportChangeLogs(ctx, req)
		if err == nil {
			break
		}

		if attempts > 10 {
			bklog.G(ctx).WithError(err).Errorf("unable to send SBOM to API, giving up")
			return
		}

		bklog.G(ctx).WithError(err).Errorf("unable to send SBOM to API, retrying")
		time.Sleep(100 * time.Millisecond)
	}
}

func NewDepotClient() cloudv3connect.MachineServiceClient {
	baseURL := os.Getenv("DEPOT_API_URL")
	if baseURL == "" {
		return nil
		// TODO:
		//baseURL = "https://api.depot.dev"
	}
	return cloudv3connect.NewMachineServiceClient(http.DefaultClient, baseURL)
}
