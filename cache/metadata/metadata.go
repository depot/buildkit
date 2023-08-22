package metadata

import (
	"database/sql"
	"encoding/json"
	"strings"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"
)

type Bucket int

const (
	mainBucket     = "_main"
	indexBucket    = "_index"
	externalBucket = "_external"
)

const (
	Main Bucket = iota
	Index
	External
)

func (b Bucket) String() string {
	switch b {
	case Main:
		return mainBucket
	case Index:
		return indexBucket
	case External:
		return externalBucket
	default:
		panic("unknown bucket")
	}
}

func (b Bucket) Table() string {
	switch b {
	case Main:
		return "main"
	case External:
		return "external"
	default:
		panic("unknown bucket")
	}
}

var errNotFound = errors.Errorf("not found")

type MetadataStore interface {
	// Close closes the store.
	Close() error

	// KeyIDs returns all the keys in the _main store.
	KeyIDs() ([]string, error)
	// Checks if an id exists in the _main store.
	Exists(id string) bool

	// Search returns all the StorageItems that match the index.
	Search(index string) ([]*StorageItem, error)

	// Get returns a StorageItem and a bool indicating if it was found.
	Get(id string) (*StorageItem, bool)
	// SetValues sets the values for the StorageItem.  This overwrites any existing values.
	SetValues(values []VVVVV) error

	// GetExternal returns the value for the key in the _external store.
	GetExternal(id, key string) ([]byte, error)

	// Delete removes the StorageItem from the _main, _external and _index stores.
	Delete(id string) error
	// ClearValue sets the value of the key to nil keeping the key.
	ClearValue(id string, bucket Bucket, key string) error
	// ClearValue sets the value of the key to nil keeping the key.  Also deletes the index if the value has an index.
	ClearIndexedValue(id string, bucket Bucket, index, key string) error
}

type MyStore struct {
	DB    *sql.DB
	Store *Store
}

// Close closes the store.
func (s *MyStore) Close() error {
	return s.DB.Close()
}

// KeyIDs returns all the keys in the _main store.
func (s *MyStore) KeyIDs() ([]string, error) {
	return selectAllIDs(s.DB)
}

func selectAllIDs(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SELECT id FROM main")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	return ids, nil
}

// Checks if an id exists in the _main store.
func (s *MyStore) Exists(id string) bool {
	exists, err := checkIfIDExists(s.DB, id)
	if err != nil {
		logrus.Errorf("failed to check if id exists: %v", err)
		return false
	}

	return exists
}

func checkIfIDExists(db *sql.DB, id string) (bool, error) {
	var exists bool
	err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM main WHERE id = ?)", id).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

// Search returns all the StorageItems that match the index.
func (s *MyStore) Search(index string) ([]*StorageItem, error) {
	idValues, err := selectIDsWhereIndexJoinOnMain(s.DB, index)
	if err != nil {
		return nil, err
	}

	items := make([]*StorageItem, 0, len(idValues))
	for id, values := range idValues {
		items = append(items, NewStorageItemWithValues(id, s, values))
	}

	return items, nil
}

// This selects all the ids and puts them in a map of id to all values.
func selectIDsWhereIndexJoinOnMain(db *sql.DB, index string) (map[string]map[string]*Value, error) {
	rows, err := db.Query("SELECT main.id, main.column, main.value FROM main INNER JOIN index ON main.id = index.id WHERE index.column = ?", index)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	idValues := make(map[string]map[string]*Value)
	for rows.Next() {
		var id string
		var column string
		var value []byte
		if err := rows.Scan(&id, &column, &value); err != nil {
			return nil, err
		}

		var v Value
		if err := json.Unmarshal(value, &v); err != nil {
			return nil, err
		}

		if _, ok := idValues[id]; !ok {
			idValues[id] = make(map[string]*Value)
		}

		idValues[id][column] = &v
	}

	return idValues, nil
}

// Get returns a StorageItem and a bool indicating if it was found.
func (s *MyStore) Get(id string) (*StorageItem, bool) {
	values, err := selectValuesWhereID(s.DB, id)
	if err != nil {
		logrus.Errorf("failed to get values for id %s: %v", id, err)
		return NewStorageItem(id, s), false
	}

	if len(values) == 0 {
		return NewStorageItem(id, s), false
	}

	return NewStorageItemWithValues(id, s, values), true
}

func selectValuesWhereID(db *sql.DB, id string) (map[string]*Value, error) {
	rows, err := db.Query("SELECT column, value FROM main WHERE id = ?", id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	values := make(map[string]*Value)
	for rows.Next() {
		var column string
		var value []byte
		if err := rows.Scan(&column, &value); err != nil {
			return nil, err
		}

		var v Value
		if err := json.Unmarshal(value, &v); err != nil {
			return nil, err
		}

		values[column] = &v
	}

	return values, nil
}

// SetValues sets the values for the StorageItem.  This overwrites any existing values.
func (s *MyStore) SetValues(values []VVVVV) error {
	for _, v := range values {
		if v.Bucket == Index {
			err := updateOrInsertIndex(s.DB, v.ID, v.Column)
			if err != nil {
				return err
			}
			continue
		}

		// TODO: rewrite into single insert
		err := updateOrInsert(s.DB, v.Bucket, v.ID, v.Column, v.Value)
		if err != nil {
			return err
		}
	}

	return nil
}

func updateOrInsertIndex(db *sql.DB, id string, key string) error {
	_, err := db.Exec("INSERT INTO index (id, column) VALUES (?, ?) ON DUPLICATE KEY UPDATE column=?", id, key)
	return err
}

func updateOrInsert(db *sql.DB, bucket Bucket, id string, key string, value []byte) error {
	_, err := db.Exec("INSERT INTO ? (id, column, value) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE column=?", bucket.Table(), id, key, value, key)
	return err
}

func updateOrInsertMultiple(db *sql.DB, bucket Bucket, id string, values []VVVVV) {
	// Insert all the values
	// TODO:
}

// GetExternal returns the value for the key in the _external store.
func (s *MyStore) GetExternal(id string, key string) ([]byte, error) {
	return selectFromExternal(s.DB, id, key)
}

func selectFromExternal(db *sql.DB, id string, key string) ([]byte, error) {
	var value []byte
	err := db.QueryRow("SELECT value FROM external WHERE id = ? AND column = ?", id, key).Scan(&value)
	if err != nil {
		return nil, err
	}

	return value, nil
}

// Delete removes the StorageItem from the _main, _external and _index stores.
func (s *MyStore) Delete(id string) error {
	var rerr error
	err := deleteIDFromMain(s.DB, id)
	if err != nil {
		rerr = multierror.Append(rerr, err)
	}

	err = deleteIDFromExternal(s.DB, id)
	if err != nil {
		rerr = multierror.Append(rerr, err)
		return err
	}

	return rerr
}

func deleteIDFromMain(db *sql.DB, id string) error {
	_, err := db.Exec("DELETE FROM main WHERE id = ?", id)
	return err
}

func deleteIDFromIndex(db *sql.DB, id string) error {
	_, err := db.Exec("DELETE FROM index WHERE id = ?", id)
	return err
}

func deleteIDFromExternal(db *sql.DB, id string) error {
	_, err := db.Exec("DELETE FROM external WHERE id = ?", id)
	return err
}

// ClearValue sets the value of the key to nil keeping the key.
func (s *MyStore) ClearValue(id string, bucket Bucket, key string) error {
	return updateKeyToNullWhereID(s.DB, id, bucket, key)
}

func updateKeyToNullWhereID(db *sql.DB, id string, bucket Bucket, key string) error {
	_, err := db.Exec("UPDATE ? SET value = NULL WHERE id = ? AND column = ?", bucket.Table(), id, key)
	return err

}

// ClearValue sets the value of the key to nil keeping the key.  Also deletes the index if the value has an index.
func (s *MyStore) ClearIndexedValue(id string, bucket Bucket, _ string, key string) error {
	// MySQL has indexing built in.  So we don't need to do anything special here as we do with bolt.
	err := deleteIDFromIndex(s.DB, id)
	if err != nil {
		return err
	}

	return updateKeyToNullWhereID(s.DB, id, bucket, key)
}

type Store struct {
	DB *bolt.DB
}

type VVVVV struct {
	Bucket Bucket
	ID     string
	Column string
	Value  []byte
}

func NewIndexVVVV(id, index string) VVVVV {
	return VVVVV{
		Bucket: Index,
		ID:     id,
		Column: index,
		Value:  nil, // Ignored for indexes
	}
}

func NewVVVV(id, column string, value []byte) VVVVV {
	return VVVVV{
		Bucket: Main,
		ID:     id,
		Column: column,
		Value:  value,
	}
}

func NewExternalVVVV(id, column string, value []byte) VVVVV {
	return VVVVV{
		Bucket: External,
		ID:     id,
		Column: column,
		Value:  value,
	}
}

func boltIndexKey(index, target string) string {
	return index + "::" + target
}

// TODO: Here we can add mysql.
func NewStore(dbPath string) (*Store, error) {
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open database file %s", dbPath)
	}

	// Initialize top-level buckets.
	// mainBucket contains most of the data.
	// indexBucket indexes from some data to StorageItem.ID()
	// externalBucket contains  "filelist" and buildkit.contenthash.v0.
	for _, bucket := range []string{mainBucket, indexBucket, externalBucket} {
		err = db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte(bucket))
			return errors.WithStack(err)
		})
		if err != nil {
			return nil, err
		}
	}

	return &Store{DB: db}, nil
}

// Called by cacheManager.init to load all records.
func (s *Store) KeyIDs() ([]string, error) {
	var out []string
	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(mainBucket))
		if b == nil {
			return nil
		}
		return b.ForEach(func(key, _ []byte) error {
			out = append(out, string(key))
			return nil
		})
	})
	return out, errors.WithStack(err)
}

// TODO: Used once in cacheManager.search.  Seems like it is called quite a bit.
// Seems as if we only need to return the ids.
func (s *Store) Search(index string) ([]*StorageItem, error) {
	var out []*StorageItem
	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(indexBucket))
		if b == nil {
			return nil
		}
		main := tx.Bucket([]byte(mainBucket))
		if main == nil {
			return nil
		}
		index = boltIndexKey(index, "")
		c := b.Cursor()
		k, _ := c.Seek([]byte(index))
		for {
			if k != nil && strings.HasPrefix(string(k), index) {
				itemID := strings.TrimPrefix(string(k), index)
				k, _ = c.Next()
				b := main.Bucket([]byte(itemID))
				if b == nil {
					logrus.Errorf("index pointing to missing record %s", itemID)
					continue
				}
				values, err := s.load(itemID)
				if err != nil {
					return err
				}
				si := NewStorageItemWithValues(itemID, s, values)
				out = append(out, si)
			} else {
				break
			}
		}
		return nil
	})
	return out, errors.WithStack(err)
}

// TODO: used in several places.  Some of them appear to be crash cleanups.
func (s *Store) Delete(id string) error {
	return errors.WithStack(s.DB.Update(func(tx *bolt.Tx) error {
		external := tx.Bucket([]byte(externalBucket))
		if external != nil {
			external.DeleteBucket([]byte(id))
		}
		main := tx.Bucket([]byte(mainBucket))
		if main == nil {
			return nil
		}
		b := main.Bucket([]byte(id))
		if b == nil {
			return nil
		}

		// Load all key/values.  If the value.Index is not an empty string, then we need to remove the index.
		values, err := s.load(id)
		if err != nil {
			return err
		}

		indexes := tx.Bucket([]byte(indexBucket))
		for _, v := range values {
			if v.Index == "" {
				continue
			}
			if err := indexes.Delete([]byte(boltIndexKey(v.Index, id))); err != nil {
				return err
			}
		}
		return main.DeleteBucket([]byte(id))
	}))
}

func (s *Store) SetValues(values []VVVVV) error {
	err := s.DB.Update(func(tx *bolt.Tx) error {
		for _, v := range values {
			// Indexes are key only and are used to seek to the first "key" and then linear
			// scan until the prefix no longer matches.
			if v.Bucket == Index {
				bucket := tx.Bucket([]byte(v.Bucket.String()))
				key := boltIndexKey(v.Column, v.ID)
				return bucket.Put([]byte(key), []byte{})
			}

			bucket := tx.Bucket([]byte(v.Bucket.String()))
			idBucket, err := bucket.CreateBucketIfNotExists([]byte(v.ID))
			if err != nil {
				return errors.WithStack(err)
			}
			err = idBucket.Put([]byte(v.Column), v.Value)
			if err != nil {
				return errors.WithStack(err)
			}
		}
		return nil
	})
	return errors.WithStack(err)
}

func (s *Store) ClearValue(id string, bucket Bucket, key string) error {
	err := s.DB.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucket.String()))
		return bucket.Put([]byte(key), nil)
	})

	return errors.WithStack(err)
}

func (s *Store) ClearIndexedValue(id string, bucket Bucket, index, key string) error {
	indexKey := boltIndexKey(index, id)
	err := s.DB.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucket.String()))
		err := bucket.Put([]byte(key), nil)
		if err != nil {
			return err
		}

		bucket = tx.Bucket([]byte(indexBucket))
		_ = bucket.Delete([]byte(indexKey)) // ignore error
		return nil
	})

	return errors.WithStack(err)
}

func (s *Store) GetExternal(id, key string) ([]byte, error) {
	var buf []byte
	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(externalBucket))
		if b == nil {
			return errors.WithStack(errNotFound)
		}
		b = b.Bucket([]byte(id))
		if b == nil {
			return errors.WithStack(errNotFound)
		}
		buf2 := b.Get([]byte(key))
		if buf2 == nil {
			return errors.WithStack(errNotFound)
		}
		// data needs to be copied as boltdb can reuse the buffer after View returns
		buf = make([]byte, len(buf2))
		copy(buf, buf2)
		return nil
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return buf, nil
}

// TODO: Called from cacheMetadata.GetEqualMutable and cacheMetadata.getMetadata.
func (s *Store) Get(id string) (*StorageItem, bool) {
	empty := func() *StorageItem {
		return NewStorageItem(id, s)
	}
	tx, err := s.DB.Begin(false)
	if err != nil {
		return empty(), false
	}
	defer tx.Rollback()
	b := tx.Bucket([]byte(mainBucket))
	if b == nil {
		return empty(), false
	}
	b = b.Bucket([]byte(id))
	if b == nil {
		return empty(), false
	}

	values, _ := s.load(id)
	return NewStorageItemWithValues(id, s, values), true
}

func (s *Store) load(id string) (map[string]*Value, error) {
	out := make(map[string]*Value)
	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(mainBucket))
		if b == nil {
			return nil
		}
		b = b.Bucket([]byte(id))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			var sv Value
			if len(v) > 0 {
				if err := json.Unmarshal(v, &sv); err != nil {
					return errors.WithStack(err)
				}
				out[string(k)] = &sv
			}
			return nil
		})
	})
	return out, errors.WithStack(err)

}

func (s *Store) Exists(id string) bool {
	tx, err := s.DB.Begin(false)
	if err != nil {
		return false
	}
	defer tx.Rollback()
	b := tx.Bucket([]byte(mainBucket))
	if b == nil {
		return false
	}
	b = b.Bucket([]byte(id))
	return b != nil
}

func (s *Store) Close() error {
	return errors.WithStack(s.DB.Close())
}

type StorageItem struct {
	id      string
	storage MetadataStore

	vmu    sync.RWMutex
	values map[string]*Value

	qmu   sync.Mutex
	queue []TxSetValue
}

func NewStorageItem(id string, s MetadataStore) *StorageItem {
	return &StorageItem{
		id:      id,
		storage: s,
		values:  make(map[string]*Value),
	}
}

func NewStorageItemWithValues(id string, s MetadataStore, values map[string]*Value) *StorageItem {
	return &StorageItem{
		id:      id,
		storage: s,
		values:  values,
	}
}

type TxSetValue struct {
	Key   string
	Value *Value
}

func (s *StorageItem) ID() string {
	return s.id
}

func (s *StorageItem) Set(key string, value *Value) error {
	s.vmu.Lock()
	defer s.vmu.Unlock()

	buf, err := json.Marshal(value)
	if err != nil {
		return errors.WithStack(err)
	}

	values := []VVVVV{}
	if value.Index != "" {
		idx := NewIndexVVVV(s.id, value.Index)
		values = append(values, idx)
	} else {
		v := NewVVVV(s.id, key, buf)
		values = append(values, v)
	}

	err = s.storage.SetValues(values)
	if err != nil {
		return err
	}

	s.values[key] = value
	return nil
}

func (s *StorageItem) AppendStrings(key string, elems []string) error {
	s.vmu.Lock()
	defer s.vmu.Unlock()

	cur := s.values[key]
	var curStrs []string
	if err := cur.Unmarshal(&curStrs); err != nil {
		return err
	}

	indices := make(map[string]struct{}, len(elems))
	for _, v := range elems {
		indices[v] = struct{}{}
	}

	for _, existing := range curStrs {
		delete(indices, existing)
	}

	if len(indices) == 0 {
		return nil
	}

	for index := range indices {
		curStrs = append(curStrs, index)
	}

	v, err := NewValue(curStrs)
	if err != nil {
		return err
	}

	buf, err := json.Marshal(v)
	if err != nil {
		return errors.WithStack(err)
	}

	err = s.storage.SetValues([]VVVVV{NewVVVV(s.id, key, buf)})
	if err != nil {
		return err
	}

	s.values[key] = v
	return nil
}

func (s *StorageItem) Get(k string) *Value {
	s.vmu.RLock()
	v := s.values[k]
	s.vmu.RUnlock()
	return v
}

func (s *StorageItem) Queue(key string, value *Value) {
	s.qmu.Lock()
	defer s.qmu.Unlock()
	s.queue = append(s.queue, TxSetValue{Key: key, Value: value})
}

func (s *StorageItem) Commit() error {
	s.qmu.Lock()
	defer s.qmu.Unlock()
	if len(s.queue) == 0 {
		return nil
	}

	s.vmu.Lock()
	defer s.vmu.Unlock()

	values := []VVVVV{}
	for _, kv := range s.queue {
		buf, err := json.Marshal(kv.Value)
		if err != nil {
			return errors.WithStack(err)
		}

		values = append(values, NewVVVV(s.id, kv.Key, buf))
		s.values[kv.Key] = kv.Value

		if kv.Value.Index != "" {
			values = append(values, NewIndexVVVV(s.id, kv.Value.Index))
		}
	}

	err := s.storage.SetValues(values)
	if err != nil {
		return err
	}

	s.queue = s.queue[:0]
	return errors.WithStack(err)
}

func (s *StorageItem) ClearValue(key string) error {
	s.vmu.Lock()
	defer s.vmu.Unlock()

	old, ok := s.values[key]
	if ok && old.Index != "" {
		err := s.storage.ClearIndexedValue(s.id, Main, old.Index, key)
		if err != nil {
			return err
		}
	} else {
		err := s.storage.ClearValue(s.id, Main, key)
		if err != nil {
			return err
		}
	}

	delete(s.values, key)
	return nil
}

func (s *StorageItem) GetExternal(k string) ([]byte, error) {
	return s.storage.GetExternal(s.id, k)
}

func (s *StorageItem) SetExternal(k string, dt []byte) error {
	v := NewExternalVVVV(s.id, k, dt)
	return s.storage.SetValues([]VVVVV{v})
}

type Value struct {
	Value json.RawMessage `json:"value,omitempty"`
	Index string          `json:"index,omitempty"`
}

func NewValue(v interface{}) (*Value, error) {
	dt, err := json.Marshal(v)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Value{Value: json.RawMessage(dt)}, nil
}

func (v *Value) Unmarshal(target interface{}) error {
	return errors.WithStack(json.Unmarshal(v.Value, target))
}
