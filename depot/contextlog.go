package depot

import (
	"hash"
	"os"
	"sync"
	"time"

	"github.com/moby/buildkit/session/filesync"
	"github.com/moby/buildkit/util/progress"
	"github.com/tonistiigi/fsutil"
	fstypes "github.com/tonistiigi/fsutil/types"
)

// ContextLogFeatureEnabled returns true if the DEPOT_CONTEXTLOG_ENABLED.
func ContextLogFeatureEnabled() bool {
	return os.Getenv("DEPOT_CONTEXTLOG_ENABLED") != ""
}

var _ filesync.CacheUpdater = (*ContextLog)(nil)

// ContextLog wraps functions that are used in the fsutil package in order to
// record what files are transferred over the network.
//
// This gives visibility into which files of the build context are transferred.
type ContextLog struct {
	inner filesync.CacheUpdater
	pw    progress.Writer

	mu         sync.Mutex
	startTimes map[string]time.Time
}

func NewContextLog(inner filesync.CacheUpdater, pw progress.Writer) *ContextLog {
	return &ContextLog{
		inner:      inner,
		pw:         pw,
		startTimes: map[string]time.Time{},
	}
}

func (c *ContextLog) MarkSupported(s bool) { c.inner.MarkSupported(s) }

// ContentHasher is wrapped in order to record the start time of the transfer.
// The fsutil package seems to call this function once per file that has been
// modified or added.
func (c *ContextLog) ContentHasher() fsutil.ContentHasher {
	return func(stat *fstypes.Stat) (hash.Hash, error) {
		hash, err := c.inner.ContentHasher()(stat)
		if err != nil {
			return nil, err
		}

		now := time.Now()

		c.mu.Lock()
		c.startTimes[stat.Path] = now
		c.mu.Unlock()

		c.pw.Write(stat.Path, progress.Status{
			Started: &now,
			Current: int(stat.Size_),
			Action:  "receiving",
		})

		return hash, nil
	}
}

// HandleChange wraps the inner HandleChange function in order to record the
// end time of the build context file transfer.
func (c *ContextLog) HandleChange(kind fsutil.ChangeKind, path string, stat os.FileInfo, statErr error) error {
	err := c.inner.HandleChange(kind, path, stat, statErr)
	if err != nil {
		return err
	}
	if statErr != nil {
		return nil
	}
	// No need to record deletes on the buildkit server side.
	if kind == fsutil.ChangeKindDelete {
		return nil
	}

	var size int64
	if stat != nil {
		size = stat.Size()
	}

	now := time.Now()
	c.mu.Lock()
	started, ok := c.startTimes[path]
	c.mu.Unlock()
	if !ok {
		started = now
	}
	status := progress.Status{
		Started:   &started,
		Completed: &now,
		Current:   int(size),
		Action:    "receiving",
	}

	_ = c.pw.Write(path, status)

	return nil
}
