package cache

import (
	"context"
	"time"

	"github.com/moby/buildkit/cache/metadata"
	"github.com/moby/buildkit/client"
	"github.com/moby/buildkit/util/bklog"
	digest "github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
)

const sizeUnknown int64 = -1
const keySize = "snapshot.size"
const keyEqualMutable = "cache.equalMutable"
const keyCachePolicy = "cache.cachePolicy"
const keyDescription = "cache.description"
const keyCreatedAt = "cache.createdAt"
const keyLastUsedAt = "cache.lastUsedAt"
const keyUsageCount = "cache.usageCount"
const keyLayerType = "cache.layerType"
const keyRecordType = "cache.recordType"
const keyCommitted = "snapshot.committed"
const keyParent = "cache.parent"
const keyMergeParents = "cache.mergeParents"
const keyLowerDiffParent = "cache.lowerDiffParent"
const keyUpperDiffParent = "cache.upperDiffParent"
const keyDiffID = "cache.diffID"
const keyChainID = "cache.chainID"
const keyBlobChainID = "cache.blobChainID"
const keyBlob = "cache.blob"
const keySnapshot = "cache.snapshot"
const keyBlobOnly = "cache.blobonly"
const keyMediaType = "cache.mediatype"
const keyImageRefs = "cache.imageRefs"
const keyDeleted = "cache.deleted"
const keyBlobSize = "cache.blobsize" // the packed blob size as specified in the oci descriptor
const keyURLs = "cache.layer.urls"

// Indexes
const blobchainIndex = "blobchainid:"
const chainIndex = "chainid:"

type MetadataStore interface {
	Search(context.Context, string) ([]RefMetadata, error)
}

type RefMetadata interface {
	ID() string

	GetDescription() string
	SetDescription(string) error

	GetCreatedAt() time.Time
	SetCreatedAt(time.Time) error

	HasCachePolicyDefault() bool
	SetCachePolicyDefault() error
	HasCachePolicyRetain() bool
	SetCachePolicyRetain() error

	GetLayerType() string
	SetLayerType(string) error

	GetRecordType() client.UsageRecordType
	SetRecordType(client.UsageRecordType) error

	// GetEqualMutable returns a "shallow" RefMetadata that only includes ID.
	// It only supports Get/Set External
	GetEqualMutable() (RefMetadata, bool)

	// generic getters/setters for external packages
	GetString(string) string
	SetString(key, val string) error
	SetIndexedString(key, val, index string) error

	GetExternal(string) ([]byte, error)
	SetExternal(string, []byte) error

	ClearValueAndIndex(string) error
}

func (cm *cacheManager) Search(ctx context.Context, idx string) ([]RefMetadata, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.search(ctx, idx)
}

// callers must hold cm.mu lock
func (cm *cacheManager) search(ctx context.Context, idx string) ([]RefMetadata, error) {
	sis, err := cm.MetadataStore.Search(idx)
	if err != nil {
		return nil, err
	}
	var mds []RefMetadata
	for _, si := range sis {
		// calling getMetadata ensures we return the same storage item object that's cached in memory
		md, ok := cm.getMetadata(si.ID())
		if !ok {
			bklog.G(ctx).Warnf("missing metadata for storage item %q during search for %q", si.ID(), idx)
			continue
		}
		if md.getDeleted() {
			continue
		}
		mds = append(mds, md)
	}
	return mds, nil
}

// callers must hold cm.mu lock
func (cm *cacheManager) getMetadata(id string) (*cacheMetadata, bool) {
	if rec, ok := cm.records[id]; ok {
		return rec.cacheMetadata, true
	}
	si, ok := cm.MetadataStore.Get(id)
	md := &cacheMetadata{si: si, store: cm.MetadataStore}
	return md, ok
}

type cacheMetadata struct {
	si    *metadata.StorageItem
	store metadata.MetadataStore
}

func (md *cacheMetadata) ID() string {
	return md.si.ID()
}

func (md *cacheMetadata) commitMetadata() error {
	return md.si.Commit()
}

func (md *cacheMetadata) GetDescription() string {
	return md.GetString(keyDescription)
}

func (md *cacheMetadata) SetDescription(descr string) error {
	return md.SetValue(keyDescription, descr)
}

func (md *cacheMetadata) queueDescription(descr string) error {
	return md.queueValue(keyDescription, descr)
}

func (md *cacheMetadata) queueCommitted(b bool) error {
	return md.queueValue(keyCommitted, b)
}

func (md *cacheMetadata) getCommitted() bool {
	return md.getBool(keyCommitted)
}

func (md *cacheMetadata) GetLayerType() string {
	return md.GetString(keyLayerType)
}

func (md *cacheMetadata) SetLayerType(value string) error {
	return md.SetValue(keyLayerType, value)
}

func (md *cacheMetadata) GetRecordType() client.UsageRecordType {
	return client.UsageRecordType(md.GetString(keyRecordType))
}

func (md *cacheMetadata) SetRecordType(value client.UsageRecordType) error {
	return md.SetValue(keyRecordType, value)
}

func (md *cacheMetadata) queueRecordType(value client.UsageRecordType) error {
	return md.queueValue(keyRecordType, value)
}

func (md *cacheMetadata) SetCreatedAt(tm time.Time) error {
	return md.setTime(keyCreatedAt, tm)
}

func (md *cacheMetadata) queueCreatedAt(tm time.Time) error {
	return md.queueTime(keyCreatedAt, tm)
}

func (md *cacheMetadata) GetCreatedAt() time.Time {
	return md.getTime(keyCreatedAt)
}

func (md *cacheMetadata) HasCachePolicyDefault() bool {
	return md.getCachePolicy() == cachePolicyDefault
}

func (md *cacheMetadata) SetCachePolicyDefault() error {
	return md.setCachePolicy(cachePolicyDefault)
}

func (md *cacheMetadata) HasCachePolicyRetain() bool {
	return md.getCachePolicy() == cachePolicyRetain
}

func (md *cacheMetadata) SetCachePolicyRetain() error {
	return md.setCachePolicy(cachePolicyRetain)
}

func (md *cacheMetadata) GetExternal(s string) ([]byte, error) {
	return md.si.GetExternal(s)
}

func (md *cacheMetadata) SetExternal(s string, dt []byte) error {
	return md.si.SetExternal(s, dt)
}

// GetEqualMutable returns a shallow RefMetadata that only includes ID and none of the id's key/values.
func (md *cacheMetadata) GetEqualMutable() (RefMetadata, bool) {
	id := md.getEqualMutable()
	ok := md.store.Exists(id)
	if !ok {
		return nil, false
	}

	return &cacheMetadata{
		si:    metadata.NewStorageItem(id, md.store),
		store: md.store,
	}, true
}

func (md *cacheMetadata) getEqualMutable() string {
	return md.GetString(keyEqualMutable)
}

func (md *cacheMetadata) setEqualMutable(s string) error {
	return md.queueValue(keyEqualMutable, s)
}

func (md *cacheMetadata) clearEqualMutable() error {
	md.si.Queue(keyEqualMutable, nil)
	return nil
}

func (md *cacheMetadata) queueDiffID(str digest.Digest) error {
	return md.queueValue(keyDiffID, str)
}

func (md *cacheMetadata) getMediaType() string {
	return md.GetString(keyMediaType)
}

func (md *cacheMetadata) queueMediaType(str string) error {
	return md.queueValue(keyMediaType, str)
}

func (md *cacheMetadata) getSnapshotID() string {
	sid := md.GetString(keySnapshot)
	// Note that historic buildkit releases did not always set the snapshot ID.
	// Fallback to record ID is needed for old build cache compatibility.
	if sid == "" {
		return md.ID()
	}
	return sid
}

func (md *cacheMetadata) queueSnapshotID(str string) error {
	return md.queueValue(keySnapshot, str)
}

func (md *cacheMetadata) getDiffID() digest.Digest {
	return digest.Digest(md.GetString(keyDiffID))
}

func (md *cacheMetadata) queueChainID(str digest.Digest) error {
	return md.queueIndexedValue(keyChainID, str, chainIndex+str.String())
}

func (md *cacheMetadata) getBlobChainID() digest.Digest {
	return digest.Digest(md.GetString(keyBlobChainID))
}

func (md *cacheMetadata) queueBlobChainID(str digest.Digest) error {
	return md.queueIndexedValue(keyBlobChainID, str, blobchainIndex+str.String())
}

func (md *cacheMetadata) getChainID() digest.Digest {
	return digest.Digest(md.GetString(keyChainID))
}

func (md *cacheMetadata) queueBlob(str digest.Digest) error {
	return md.queueValue(keyBlob, str)
}

func (md *cacheMetadata) appendURLs(urls []string) error {
	if len(urls) == 0 {
		return nil
	}
	return md.appendStringSlice(keyURLs, urls...)
}

func (md *cacheMetadata) getURLs() []string {
	return md.GetStringSlice(keyURLs)
}

func (md *cacheMetadata) getBlob() digest.Digest {
	return digest.Digest(md.GetString(keyBlob))
}

func (md *cacheMetadata) queueBlobOnly(b bool) error {
	return md.queueValue(keyBlobOnly, b)
}

func (md *cacheMetadata) getBlobOnly() bool {
	return md.getBool(keyBlobOnly)
}

func (md *cacheMetadata) queueDeleted() error {
	return md.queueValue(keyDeleted, true)
}

func (md *cacheMetadata) getDeleted() bool {
	return md.getBool(keyDeleted)
}

func (md *cacheMetadata) queueParent(parent string) error {
	return md.queueValue(keyParent, parent)
}

func (md *cacheMetadata) getParent() string {
	return md.GetString(keyParent)
}

func (md *cacheMetadata) queueMergeParents(parents []string) error {
	return md.queueValue(keyMergeParents, parents)
}

func (md *cacheMetadata) getMergeParents() []string {
	return md.getStringSlice(keyMergeParents)
}

func (md *cacheMetadata) queueLowerDiffParent(parent string) error {
	return md.queueValue(keyLowerDiffParent, parent)
}

func (md *cacheMetadata) getLowerDiffParent() string {
	return md.GetString(keyLowerDiffParent)
}

func (md *cacheMetadata) queueUpperDiffParent(parent string) error {
	return md.queueValue(keyUpperDiffParent, parent)
}

func (md *cacheMetadata) getUpperDiffParent() string {
	return md.GetString(keyUpperDiffParent)
}

func (md *cacheMetadata) queueSize(s int64) error {
	return md.queueValue(keySize, s)
}

func (md *cacheMetadata) getSize() int64 {
	if size, ok := md.getInt64(keySize); ok {
		return size
	}
	return sizeUnknown
}

func (md *cacheMetadata) appendImageRef(s string) error {
	return md.appendStringSlice(keyImageRefs, s)
}

func (md *cacheMetadata) getImageRefs() []string {
	return md.getStringSlice(keyImageRefs)
}

func (md *cacheMetadata) queueBlobSize(s int64) error {
	return md.queueValue(keyBlobSize, s)
}

func (md *cacheMetadata) getBlobSize() int64 {
	if size, ok := md.getInt64(keyBlobSize); ok {
		return size
	}
	return sizeUnknown
}

func (md *cacheMetadata) setCachePolicy(p cachePolicy) error {
	return md.SetValue(keyCachePolicy, p)
}

func (md *cacheMetadata) getCachePolicy() cachePolicy {
	if i, ok := md.getInt64(keyCachePolicy); ok {
		return cachePolicy(i)
	}
	return cachePolicyDefault
}

func (md *cacheMetadata) getLastUsed() (int, *time.Time) {
	v := md.si.Get(keyUsageCount)
	if v == nil {
		return 0, nil
	}
	var usageCount int
	if err := v.Unmarshal(&usageCount); err != nil {
		return 0, nil
	}
	v = md.si.Get(keyLastUsedAt)
	if v == nil {
		return usageCount, nil
	}
	var lastUsedTs int64
	if err := v.Unmarshal(&lastUsedTs); err != nil || lastUsedTs == 0 {
		return usageCount, nil
	}
	tm := time.Unix(lastUsedTs/1e9, lastUsedTs%1e9)
	return usageCount, &tm
}

func (md *cacheMetadata) updateLastUsed() error {
	count, _ := md.getLastUsed()
	count++

	err := md.queueValue(keyUsageCount, count)
	if err != nil {
		return errors.Wrap(err, "failed to create usageCount value")
	}

	err = md.queueValue(keyLastUsedAt, time.Now().UnixNano())
	if err != nil {
		return errors.Wrap(err, "failed to create lastUsedAt value")
	}

	return md.commitMetadata()
}

func (md *cacheMetadata) queueValue(key string, value interface{}) error {
	v, err := metadata.NewValue(value)
	if err != nil {
		return errors.Wrap(err, "failed to create value")
	}
	md.si.Queue(key, v)
	return nil
}

func (md *cacheMetadata) queueIndexedValue(key string, value interface{}, index string) error {
	v, err := metadata.NewValue(value)
	if err != nil {
		return errors.Wrap(err, "failed to create value")
	}
	v.Index = index
	md.si.Queue(key, v)
	return nil
}

func (md *cacheMetadata) SetString(key, value string) error {
	return md.SetValue(key, value)
}

func (md *cacheMetadata) SetIndexedString(key, value string, index string) error {
	v, err := metadata.NewValue(value)
	if err != nil {
		return errors.Wrap(err, "failed to create value")
	}
	v.Index = index
	return md.si.Set(key, v)
}

func (md *cacheMetadata) SetValue(key string, value interface{}) error {
	v, err := metadata.NewValue(value)
	if err != nil {
		return errors.Wrap(err, "failed to create value")
	}
	return md.si.Set(key, v)
}

func (md *cacheMetadata) ClearValueAndIndex(key string) error {
	return md.si.ClearValue(key)
}

func (md *cacheMetadata) GetString(key string) string {
	v := md.si.Get(key)
	if v == nil {
		return ""
	}
	var str string
	if err := v.Unmarshal(&str); err != nil {
		return ""
	}
	return str
}

func (md *cacheMetadata) GetStringSlice(key string) []string {
	v := md.si.Get(key)
	if v == nil {
		return nil
	}
	var val []string
	if err := v.Unmarshal(&val); err != nil {
		return nil
	}
	return val
}

func (md *cacheMetadata) setTime(key string, value time.Time) error {
	return md.SetValue(key, value.UnixNano())
}

func (md *cacheMetadata) queueTime(key string, value time.Time) error {
	return md.queueValue(key, value.UnixNano())
}

func (md *cacheMetadata) getTime(key string) time.Time {
	v := md.si.Get(key)
	if v == nil {
		return time.Time{}
	}
	var tm int64
	if err := v.Unmarshal(&tm); err != nil {
		return time.Time{}
	}
	return time.Unix(tm/1e9, tm%1e9)
}

func (md *cacheMetadata) getBool(key string) bool {
	v := md.si.Get(key)
	if v == nil {
		return false
	}
	var b bool
	if err := v.Unmarshal(&b); err != nil {
		return false
	}
	return b
}

func (md *cacheMetadata) getInt64(key string) (int64, bool) {
	v := md.si.Get(key)
	if v == nil {
		return 0, false
	}
	var i int64
	if err := v.Unmarshal(&i); err != nil {
		return 0, false
	}
	return i, true
}

// TODO: move this into the db as a helper.  This is so we don't need to pass functions around.
func (md *cacheMetadata) appendStringSlice(key string, values ...string) error {
	return md.si.AppendStrings(key, values)
}

func (md *cacheMetadata) getStringSlice(key string) []string {
	v := md.si.Get(key)
	if v == nil {
		return nil
	}
	var s []string
	if err := v.Unmarshal(&s); err != nil {
		return nil
	}
	return s
}
