package depot

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

/*
 * Tunneling data from the API to the local source op is intricate.
 * The diagram is to help document the transfer.
 *
 * The following functions are helpers along this path.
 *
 *                  │
 *         mTLS extract spiffe
 *                  │
 *                  ▼
 *         ┌────────────────┐
 *         │                │
 *         │ control.Solve  │
 *         │                │
 *         └────────────────┘
 *                  │
 *          transfer over ctx
 *                  │
 *                  ▼
 *         ┌────────────────┐
 *         │                │
 *         │  Solver.Solve  │
 *         │                │
 *         └────────────────┘
 *                  │
 *      transfer over job.values
 *                  │
 *                  ▼
 *      ┌──────────────────────┐
 *      │                      │
 *      │ llbBridge.loadResult │
 *      │                      │
 *      └──────────────────────┘
 *                  │
 *  transfer over vertex description
 *                  │
 *                  ▼
 *    ┌───────────────────────────┐
 *    │                           │
 *    │localSourceHandler.snapshot│
 *    │                           │
 *    └───────────────────────────┘
 *                  │
 *                  ▼
 *    ┌───────────────────────────┐
 *    │                           │
 *    │   depot.ContextLog.Send   │
 *    │                           │
 *    └───────────────────────────┘
 */

// SpiffeFromContext returns the SPIFFE ID from context.
// This can be used for sending information to the API.
func SpiffeFromContext(ctx context.Context) string {
	var spiffeID string
	peer, ok := peer.FromContext(ctx)
	if ok {
		tlsInfo, ok := peer.AuthInfo.(credentials.TLSInfo)
		if ok && tlsInfo.SPIFFEID != nil {
			spiffeID = tlsInfo.SPIFFEID.String()
		}
	}

	return spiffeID
}

const SpiffeKey = "depot.spiffe"

// DepotSpiffeKey is used to store the spiffe in context to transfer from the control.Solve to llbsolver.Solve.
type DepotSpiffeKey struct{}

func WithSpiffe(ctx context.Context, spiffe string) context.Context {
	if spiffe == "" {
		return ctx
	}
	return context.WithValue(ctx, DepotSpiffeKey{}, spiffe)
}

func SpiffeFrom(ctx context.Context) string {
	spiffe, ok := ctx.Value(DepotSpiffeKey{}).(string)
	if !ok {
		return ""
	}
	return spiffe
}

// Used to set the spiffe on a job without cyclic imports.
type JobSetValue interface {
	SetValue(key string, value interface{})
}

// JobSetValueWithSpiffe sets the spiffe on a job while within llbsolver.Solve.
func JobSetValueWithSpiffe(j JobSetValue, spiffe string) {
	if spiffe != "" {
		j.SetValue(SpiffeKey, spiffe)
	}
}

// Used to load the spiffe from a solver.Builder without cyclic imports.
type Builder interface {
	EachValue(ctx context.Context, key string, fn func(interface{}) error) error
}

// SpiffeFromBuilder returns the spiffe from a solver.Builder while within llbBridge.loadResult.
func SpiffeFromBuilder(b Builder) (string, error) {
	var spiffe string
	err := b.EachValue(context.TODO(), SpiffeKey, func(v interface{}) error {
		s, ok := v.(string)
		if !ok {
			return errors.Errorf("invalid spiffe %T", v)
		}
		spiffe = s
		return nil
	})
	if err != nil {
		return "", err
	}
	return spiffe, nil
}

type VertexOptions interface {
	Set(key, value string)
}

// VertexOptionsWithSpiffe sets the spiffe on the vertex description to transfer it from the llbBridge to the local source.
func VertexOptionsWithSpiffe(vtx VertexOptions, spiffe string) {
	if spiffe != "" {
		vtx.Set(SpiffeKey, spiffe)
	}
}

// Used to get the the descriptions from a solver.Vertex without cyclic imports.
type Describer interface {
	Description() map[string]string
}

// SpiffeFromVertex returns the spiffe from a solver.Vertex while within localSource.snapshot.
func SpiffeFromVertex(d Describer) string {
	if d == nil {
		return ""
	}
	desc := d.Description()
	if desc == nil {
		return ""
	}
	return d.Description()[SpiffeKey]
}
