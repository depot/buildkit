package depot

import (
	"context"
	"hash"
	"io/fs"
	"os"
	"sync"
	"time"

	"connectrpc.com/connect"
	cloudv3 "github.com/moby/buildkit/depot/api"
	"github.com/moby/buildkit/depot/api/cloudv3connect"
	"github.com/moby/buildkit/session/filesync"
	"github.com/moby/buildkit/util/bklog"
	"github.com/tonistiigi/fsutil"
	fstypes "github.com/tonistiigi/fsutil/types"
)

// ContextLogFeatureEnabled returns true if the DEPOT_CONTEXTLOG_ENABLED.
// This will ship the changes to the buld context to the API.
//
// The design has two go routines.  The first, `Process`, reads the start and finish file transfer times
// from an input channel.  Once a file has finished transferring, it is sent to a second via a channel.
// The second, `Send`, reads the finished files from the channel, batches them, and sends them to the API
// every second.
//
// The `Process` go routine is stopped by closing the start and finish channels within the `Close` function.
// The `Process` go routine can also be stopped by canceling the context passed to `NewContextLog`.
// Once stopped, the `Process` go routine closes the `Send` channel.
// The `Send` go routine is stopped by closing the `Send` channel but it attempts to send all remaining logs within five seconds.
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

	startCh  chan *Log
	finishCh chan *Log
	sendCh   chan *ProcessedLog

	closed chan struct{}
	wg     sync.WaitGroup
}

type Log struct {
	Path string
	At   time.Time
	Size int64
	Mode fs.FileMode
}

type ProcessedLog struct {
	Path       string
	Size       int64
	StartedAt  time.Time
	FinishedAt time.Time
	Mode       fs.FileMode
}

// NewContextLog wraps the given CacheUpdater in order to record the start and finish
// of each context file modification.  The logs are sent to the API in the background.
// The returned ContextLog must be `Close`d to stop the background process.
func NewContextLog(ctx context.Context, spiffe string, inner filesync.CacheUpdater) *ContextLog {
	log := &ContextLog{
		inner:    inner,
		startCh:  make(chan *Log, 1024),
		finishCh: make(chan *Log, 1024),
		sendCh:   make(chan *ProcessedLog, 1024),
		closed:   make(chan struct{}),
	}

	log.wg.Add(2)
	go func() { log.Process(ctx); log.wg.Done() }()
	go func() { log.Send(spiffe); log.wg.Done() }()
	return log
}

// Close stops the background processes by signaling the `Process` go routine to stop via
// closing the `closed` channel. This in turn closes the `sendCh` channel which signals the
// `Send` go routine to stop.
//
// This elaborate shutdown procedure is required as `ContentHasher` and `HandleChange`
// may be called in a racy by by fsutil despite the context being canceled.
func (c *ContextLog) Close() error {
	close(c.closed)
	c.wg.Wait()
	return nil
}

func (c *ContextLog) MarkSupported(s bool) { c.inner.MarkSupported(s) }

// ContentHasher is wrapped in order to record the start time of the transfer.
// The fsutil package seems to call this function once per file that has been
// modified or added.
//
// There is no way to know when fsutil has finished calling ContentHasher nor HandleChange.
// As a result, we skip sending data to the startCh channel if the ContextLog has been `Close`d.
// This may mean that some data is not recorded as a result of fsutil's racy behavior.
func (c *ContextLog) ContentHasher() fsutil.ContentHasher {
	return func(stat *fstypes.Stat) (hash.Hash, error) {
		hash, err := c.inner.ContentHasher()(stat)
		if err != nil {
			return nil, err
		}

		if stat != nil {
			select {
			case <-c.closed:
			case c.startCh <- &Log{Path: stat.Path, At: time.Now(), Size: stat.Size_}:
			}
		}

		return hash, nil
	}
}

// HandleChange wraps the inner HandleChange function in order to record the
// end time of the build context file transfer.
//
// There is no way to know when fsutil has finished calling ContentHasher nor HandleChange.
// As a result, we skip sending data to the startCh channel if the ContextLog has been `Close`d.
// This may mean that some data is not recorded as a result of fsutil's racy behavior.
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

	if stat == nil {
		return nil
	}

	select {
	case <-c.closed:
	case c.finishCh <- &Log{Path: path, At: time.Now(), Size: stat.Size(), Mode: stat.Mode()}:
	}

	return nil
}

// Process is a background process to record the start and finish times of the build context file transfers.
// If context is canceled or the ContextLog is `Close`d, this will close the sendCh channel. This in turn
// will stop the Send goroutine.
func (c *ContextLog) Process(ctx context.Context) {
	logs := map[string]*ProcessedLog{}
	// Closing the send channel signals the the Send goroutine to finish.
	// We close here as Process is the only goroutine that writes to the channel.
	defer close(c.sendCh)

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closed:
			return
		case log := <-c.startCh:
			logs[log.Path] = &ProcessedLog{Path: log.Path, Size: log.Size, StartedAt: log.At}
		case log := <-c.finishCh:
			l, ok := logs[log.Path]
			if ok {
				l.FinishedAt = log.At
				l.Mode = log.Mode
				c.sendCh <- l
				delete(logs, log.Path)
			}
		}
	}
}

// Send is a background process to send the batches of context logs to the API.
// Cancel this by closing the sendCh channel via `Close`.
func (c *ContextLog) Send(spiffe string) {
	// Buffer 1 second before sending build timings to the server
	const (
		bufferTimeout = time.Second
	)

	var (
		once   sync.Once
		client cloudv3connect.MachineServiceClient
	)

	bearer := BearerFromEnv()

	toSend := []*ProcessedLog{}
	ticker := time.NewTicker(bufferTimeout)
	defer ticker.Stop()
	for {
		select {
		case log, ok := <-c.sendCh:
			if ok {
				toSend = append(toSend, log)
			} else {
				// Channel is closed, so send the remaining logs.

				// Lazily create the client.
				once.Do(func() {
					client = NewDepotClient()
				})
				// Requires a new context because the previous one may be canceled while we are
				// sending the build timings.  At most one will wait 5 seconds.
				ctx2, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				// If we are unable to send the context during shutdown, we will just drop it.
				_ = SendContext(ctx2, client, bearer, spiffe, toSend)

				cancel()
				return
			}
		case <-ticker.C:
			if len(toSend) > 0 {
				// Lazily create the client.
				once.Do(func() {
					client = NewDepotClient()
				})
				// Requires a new context because the previous one may be canceled while we are
				// sending the build timings.  At most one will wait 5 seconds.
				ctx2, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := SendContext(ctx2, client, bearer, spiffe, toSend); err == nil {
					// Clear all reported steps.
					toSend = toSend[:0]
				} else {
					bklog.G(context.Background()).WithError(err).Errorf("unable to send context log to API")
				}

				ticker.Reset(bufferTimeout)
				cancel()
			}
		}
	}
}

func SendContext(ctx context.Context, client cloudv3connect.MachineServiceClient, bearer, spiffe string, logs []*ProcessedLog) error {
	if len(logs) == 0 {
		return nil
	}

	items := make([]*cloudv3.ReportContextItemsRequest_Item, len(logs))
	for i, log := range logs {
		typ := cloudv3.ReportContextItemsRequest_TYPE_FILE
		if log.Mode.IsDir() {
			typ = cloudv3.ReportContextItemsRequest_TYPE_DIRECTORY
		} else if log.Mode&fs.ModeSymlink != 0 {
			typ = cloudv3.ReportContextItemsRequest_TYPE_SYMLINK
		}

		items[i] = &cloudv3.ReportContextItemsRequest_Item{
			Path:       log.Path,
			Type:       typ,
			SizeBytes:  log.Size,
			DurationMs: int32(log.FinishedAt.Sub(log.StartedAt).Milliseconds()),
		}
	}

	req := connect.NewRequest(&cloudv3.ReportContextItemsRequest{
		SpiffeId: spiffe,
		Items:    items,
	})
	req.Header().Add("Authorization", bearer)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := client.ReportContextItems(ctx, req)
	return err
}
