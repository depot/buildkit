package depot

import (
	"context"
	"net/http"
	"os"
	"time"

	"connectrpc.com/connect"
	cloudv3 "github.com/moby/buildkit/depot/api"
	"github.com/moby/buildkit/depot/api/cloudv3connect"
	"github.com/moby/buildkit/util/bklog"
)

const (
	BuildArgTarget      = "build-arg:DEPOT_TARGET"
	BuildDockerfile     = "depot-dockerfile"
	BuildDockerfileName = "depot-dockerfile-name"
)

type BuildContextRequest struct {
	SpiffeID string
	Bearer   string

	BuildTarget    string
	DockerfileName string
	Contents       string
}

func SendBuildContext(ctx context.Context, r *BuildContextRequest) {
	if r.SpiffeID == "" || r.Bearer == "" || r.Contents == "" || r.BuildTarget == "" {
		return
	}

	if r.DockerfileName == "" {
		r.DockerfileName = "Dockerfile"
	}

	req := connect.NewRequest(&cloudv3.ReportBuildContextRequest{
		SpiffeId: r.SpiffeID,
		Dockerfile: &cloudv3.Dockerfile{
			Target:   r.BuildTarget,
			Filename: r.DockerfileName,
			Contents: r.Contents,
		},
	})
	req.Header().Add("Authorization", r.Bearer)

	attempts := 0
	for {
		attempts++
		client := NewDepotClient()
		if client == nil {
			break
		}

		_, err := client.ReportBuildContext(ctx, req)
		if err == nil {
			break
		}

		if attempts > 10 {
			bklog.G(ctx).WithError(err).Errorf("unable to send build context to API, giving up")
			return
		}

		bklog.G(ctx).WithError(err).Errorf("unable to send build context to API, retrying")
		time.Sleep(100 * time.Millisecond)
	}
}

func NewDepotClient() cloudv3connect.MachineServiceClient {
	baseURL := os.Getenv("DEPOT_API_URL")
	if baseURL == "" {
		baseURL = "https://api.depot.dev"
	}
	return cloudv3connect.NewMachineServiceClient(http.DefaultClient, baseURL)
}

type AddMeta interface {
	AddMeta(key string, value []byte)
}

func StoreBuildContext(metadata AddMeta, dockerfile []byte, dockerfileName string) {
	metadata.AddMeta(BuildDockerfile, dockerfile)
	metadata.AddMeta(BuildDockerfileName, []byte(dockerfileName))
}
