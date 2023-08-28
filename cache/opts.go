package cache

import (
	"fmt"
	"time"

	"github.com/containerd/containerd/content"
	"github.com/hashicorp/go-multierror"
	"github.com/moby/buildkit/client"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/util/progress"
	digest "github.com/opencontainers/go-digest"
)

type Options struct {
	UnlazySession          session.Group
	SetSnapshotID          *string
	AppendImageRef         *string
	UpdateDescription      *string
	UpdateCreatedAt        *time.Time
	UpdateCachePolicy      *CachePolicy
	UpdateRecordType       *client.UsageRecordType
	DescHandlers           DescHandlers
	SkipUpdatingLastUsedAt bool
}

func (o *Options) Apply(ref RefMetadata) error {
	var rerr error

	if o.SetSnapshotID != nil {
		err := ref.SetString(keySnapshot, *o.SetSnapshotID, "")
		rerr = multierror.Append(rerr, err)
	}
	if o.AppendImageRef != nil {
		err := ref.AppendImageRef(*o.AppendImageRef)
		rerr = multierror.Append(rerr, err)
	}

	if o.UpdateDescription != nil {
		err := ref.SetDescription(*o.UpdateDescription)
		rerr = multierror.Append(rerr, err)
	}

	if o.UpdateCreatedAt != nil {
		err := ref.SetCreatedAt(*o.UpdateCreatedAt)
		rerr = multierror.Append(rerr, err)
	}

	if o.UpdateCachePolicy != nil {
		switch *o.UpdateCachePolicy {
		case CachePolicyDefault:
			err := ref.SetCachePolicyDefault()
			rerr = multierror.Append(rerr, err)
		case CachePolicyRetain:
			err := ref.SetCachePolicyRetain()
			rerr = multierror.Append(rerr, err)
		}
	}

	if o.UpdateRecordType != nil {
		err := ref.SetRecordType(*o.UpdateRecordType)
		rerr = multierror.Append(rerr, err)
	}

	return rerr
}

type DescHandler struct {
	Provider       func(session.Group) content.Provider
	Progress       progress.Controller
	SnapshotLabels map[string]string
	Annotations    map[string]string
	Ref            string // string representation of desc origin, can be used as a sync key
}

type DescHandlers map[digest.Digest]*DescHandler

type DescHandlerKey digest.Digest

type NeedsRemoteProviderError []digest.Digest //nolint:errname

func (m NeedsRemoteProviderError) Error() string {
	return fmt.Sprintf("missing descriptor handlers for lazy blobs %+v", []digest.Digest(m))
}

type Unlazy session.Group
