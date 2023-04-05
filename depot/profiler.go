package depot

import (
	"os"
	"runtime"

	"github.com/moby/buildkit/version"
	"github.com/pyroscope-io/client/pyroscope"
)

type ProfilerOpts struct {
	Endpoint  string
	Token     string
	ProjectID string
	Version   string
}

// NewProfilerOptsFromEnv pulls settings from env.  If required settings
// are not in the env, it will return nil.
func NewProfilerOptsFromEnv() *ProfilerOpts {
	endpoint := os.Getenv("PROFILER_ENDPOINT")
	token := os.Getenv("PROFILER_TOKEN")
	projectID := os.Getenv("PROFILER_PROJECT_ID")
	version := version.Version

	if endpoint == "" || token == "" || projectID == "" {
		return nil
	}

	return &ProfilerOpts{
		Endpoint:  endpoint,
		Token:     token,
		ProjectID: projectID,
		Version:   version,
	}
}

// Starts exporting profiles to pyroscope if opts is not nil.
func StartProfiler(opts *ProfilerOpts) {
	if opts != nil {
		runtime.SetMutexProfileFraction(5)
		runtime.SetBlockProfileRate(10000)
		_, _ = pyroscope.Start(pyroscope.Config{
			ApplicationName: "depot-buildkitd",
			ServerAddress:   opts.Endpoint,
			Logger:          nil,
			Tags:            map[string]string{"version": opts.Version, "projectID": opts.ProjectID, "platform": runtime.GOARCH},
			AuthToken:       opts.Token,

			ProfileTypes: []pyroscope.ProfileType{
				pyroscope.ProfileCPU,
				pyroscope.ProfileGoroutines,
				pyroscope.ProfileMutexCount,
				pyroscope.ProfileMutexDuration,
				pyroscope.ProfileBlockCount,
				pyroscope.ProfileBlockDuration,
			},
		})
	}
}
