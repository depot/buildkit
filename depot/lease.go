package depot

import (
	"context"
	"time"

	"github.com/containerd/containerd/leases"
)

// LeaseLabel identifies the lease's ID during an export of the image.
// I happen to use the session ID as it is unique to this specific export.
const ExportLeaseLabel = "depot/session.id"

// DEPOT: We have a special lease attached to context to inhibit the GC of layers.
type DepotLeaseKey struct{}

func WithLeaseID(ctx context.Context, leaseID string) context.Context {
	return context.WithValue(ctx, DepotLeaseKey{}, leaseID)
}

func LeaseIDFrom(ctx context.Context) string {
	leaseID, ok := ctx.Value(DepotLeaseKey{}).(string)
	if !ok {
		return ""
	}
	return leaseID
}

func Lease(ctx context.Context, mgr leases.Manager, sessionID string) (leases.Lease, error) {
	return mgr.Create(
		ctx,
		leases.WithRandomID(),
		leases.WithExpiration(time.Hour),
		leases.WithLabels(map[string]string{
			ExportLeaseLabel: sessionID,
		}),
	)
}
