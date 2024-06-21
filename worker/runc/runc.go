package runc

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/containerd/containerd/content/local"
	"github.com/containerd/containerd/diff/apply"
	"github.com/containerd/containerd/diff/walking"
	ctdmetadata "github.com/containerd/containerd/metadata"
	"github.com/containerd/containerd/platforms"
	ctdsnapshot "github.com/containerd/containerd/snapshots"
	"github.com/docker/docker/pkg/idtools"
	"github.com/moby/buildkit/cache/metadata"
	"github.com/moby/buildkit/executor/oci"
	"github.com/moby/buildkit/executor/runcexecutor"
	containerdsnapshot "github.com/moby/buildkit/snapshot/containerd"
	"github.com/moby/buildkit/util/leaseutil"
	"github.com/moby/buildkit/util/network/netproviders"
	"github.com/moby/buildkit/util/winlayers"
	"github.com/moby/buildkit/worker/base"
	wlabel "github.com/moby/buildkit/worker/label"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sync/semaphore"
)

// SnapshotterFactory instantiates a snapshotter
type SnapshotterFactory struct {
	Name string
	New  func(root string) (ctdsnapshot.Snapshotter, error)
}

// NewWorkerOpt creates a WorkerOpt.
func NewWorkerOpt(root string, snFactory SnapshotterFactory, rootless bool, processMode oci.ProcessMode, labels map[string]string, idmap *idtools.IdentityMapping, nopt netproviders.Opt, dns *oci.DNSConfig, binary, apparmorProfile string, selinux, gpu bool, parallelismSem *semaphore.Weighted, traceSocket, defaultCgroupParent string) (base.WorkerOpt, error) {
	var opt base.WorkerOpt
	name := "runc-" + snFactory.Name
	root = filepath.Join(root, name)
	if err := os.MkdirAll(root, 0700); err != nil {
		return opt, err
	}

	np, npResolvedMode, err := netproviders.Providers(nopt)
	if err != nil {
		return opt, err
	}

	// Check if user has specified OCI worker binary; if they have, append it to cmds
	var cmds []string
	if binary != "" {
		cmds = append(cmds, binary)
	}

	exe, err := runcexecutor.New(runcexecutor.Opt{
		// Root directory
		Root: filepath.Join(root, "executor"),
		// If user has specified OCI worker binary, it will be sent to the runc executor to find and use
		// Otherwise, a nil array will be sent and the default OCI worker binary will be used
		CommandCandidates: cmds,
		// without root privileges
		Rootless:            rootless,
		ProcessMode:         processMode,
		IdentityMapping:     idmap,
		DNS:                 dns,
		ApparmorProfile:     apparmorProfile,
		SELinux:             selinux,
		GPU:                 gpu,
		TracingSocket:       traceSocket,
		DefaultCgroupParent: defaultCgroupParent,
	}, np)
	if err != nil {
		return opt, err
	}
	s, err := snFactory.New(filepath.Join(root, "snapshots"))
	if err != nil {
		return opt, err
	}

	// DEPOT: Concurrently open the boltdbs as it can be very slow.
	wg := sync.WaitGroup{}
	wg.Add(2)

	var (
		metadataDB  *metadata.Store
		metadataErr error

		containerdMeta *ctdmetadata.DB
		containerdErr  error
	)

	contentStore, err := local.NewStore(filepath.Join(root, "content"))
	if err != nil {
		return opt, err
	}

	go func() {
		defer wg.Done()

		logrus.Info("Opening metadata.db")
		metadataDB, metadataErr = metadata.NewStore(filepath.Join(root, "metadata_v2.db"))
	}()

	go func() {
		defer wg.Done()

		logrus.Info("Opening metadata containerdmeta.go")
		options := *bolt.DefaultOptions
		// Reading bbolt's freelist sometimes fails when the file has a data corruption.
		// Disabling freelist sync reduces the chance of the breakage.
		// https://github.com/etcd-io/bbolt/pull/1
		// https://github.com/etcd-io/bbolt/pull/6
		options.NoFreelistSync = true

		var containerdBolt *bolt.DB
		containerdBolt, containerdErr = bolt.Open(filepath.Join(root, "containerdmeta.db"), 0644, &options)
		if err != nil {
			return
		}

		containerdMeta = ctdmetadata.NewDB(containerdBolt, contentStore, map[string]ctdsnapshot.Snapshotter{
			snFactory.Name: s,
		})

		containerdErr = containerdMeta.Init(context.TODO())
	}()

	wg.Wait()

	if metadataErr != nil {
		return opt, metadataErr
	}
	if containerdErr != nil {
		return opt, containerdErr
	}

	contentStore = containerdsnapshot.NewContentStore(containerdMeta.ContentStore(), "buildkit")

	id, err := base.ID(root)
	if err != nil {
		return opt, err
	}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	xlabels := map[string]string{
		wlabel.Executor:       "oci",
		wlabel.Snapshotter:    snFactory.Name,
		wlabel.Hostname:       hostname,
		wlabel.Network:        npResolvedMode,
		wlabel.OCIProcessMode: processMode.String(),
		wlabel.SELinuxEnabled: strconv.FormatBool(selinux),
	}
	if apparmorProfile != "" {
		xlabels[wlabel.ApparmorProfile] = apparmorProfile
	}

	for k, v := range labels {
		xlabels[k] = v
	}
	lm := leaseutil.WithNamespace(ctdmetadata.NewLeaseManager(containerdMeta), "buildkit")
	snap := containerdsnapshot.NewSnapshotter(snFactory.Name, containerdMeta.Snapshotter(snFactory.Name), "buildkit", idmap)

	opt = base.WorkerOpt{
		ID:               id,
		Labels:           xlabels,
		MetadataStore:    metadataDB,
		NetworkProviders: np,
		Executor:         exe,
		Snapshotter:      snap,
		ContentStore:     contentStore,
		Applier:          winlayers.NewFileSystemApplierWithWindows(contentStore, apply.NewFileSystemApplier(contentStore)),
		Differ:           winlayers.NewWalkingDiffWithWindows(contentStore, walking.NewWalkingDiff(contentStore)),
		ImageStore:       nil, // explicitly
		Platforms:        []ocispecs.Platform{platforms.Normalize(platforms.DefaultSpec())},
		IdentityMapping:  idmap,
		LeaseManager:     lm,
		GarbageCollect:   containerdMeta.GarbageCollect,
		ParallelismSem:   parallelismSem,
		MountPoolRoot:    filepath.Join(root, "cachemounts"),
	}
	return opt, nil
}
