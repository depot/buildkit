package bboltcachestorage

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/bklog"
	digest "github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/exp/slices"
)

const (
	resultBucket    = "_result"
	linksBucket     = "_links"
	byResultBucket  = "_byresult"
	backlinksBucket = "_backlinks"
)

type MyKeyStorage struct {
	DB    *sql.DB
	Store *Store
}

// TODO: used once in cachemanager.Query
func (s *MyKeyStorage) Exists(id string) bool {
	bklog.G(context.TODO()).Warnf("MyKeyStorage.Exists(%s)", id)
	boltexists := s.Store.Exists(id)
	dbexists := linkIDExists(s.DB, id)
	if boltexists != dbexists {
		bklog.G(context.TODO()).Errorf("MyKeyStorage.Exists(%s) boltexists != dbexists: %v != %v", id, boltexists, dbexists)
	}

	return boltexists
}

func linkIDExists(db *sql.DB, id string) bool {
	var v bool
	err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM link WHERE link_id = ?)", id).Scan(&v)
	if err != nil {
		bklog.G(context.TODO()).Errorf("linkIDExists(%s) failed: %v", id, err)
	}
	return v
}

// Only ever used in ReleaseUnreferenced.
func (s *MyKeyStorage) Walk(fn func(id string) error) error {
	bklog.G(context.TODO()).Warnf("MyKeyStorage.Walk()")
	boltids := []string{}
	err := s.Store.Walk(func(id string) error {
		boltids = append(boltids, id)
		return nil
	})
	if err != nil {
		return err
	}

	for _, id := range boltids {
		err := fn(id)
		if err != nil {
			return err
		}
	}

	ids, err := selectDistinctLinkIDs(s.DB)
	if err != nil {
		return err
	}
	sort.Strings(boltids)
	sort.Strings(ids)
	if !slices.Equal(boltids, ids) {
		bklog.G(context.TODO()).Errorf("MyKeyStorage.Walk boltids != ids: %v != %v", boltids, ids)
	}

	return nil
}

func selectDistinctLinkIDs(db *sql.DB) ([]string, error) {
	var ids []string
	rows, err := db.Query("SELECT DISTINCT link_id FROM link")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		err := rows.Scan(&id)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// TODO: Three calls
// 1. It is called from Walk in ReleaseUnreferenced.  For each id of the walk if it is not in the set of results then release from backend.
// 2. In Records it gets all CacheRecords if it exists in results, otherwise it releases them. Looks like opportunistic cleanup.
// 3. filterResults just lists the cache results.
func (s *MyKeyStorage) WalkResults(id string, fn func(solver.CacheResult) error) error {
	bklog.G(context.TODO()).Warnf("MyKeyStorage.WalkResults(%s)", id)
	boltresults := []solver.CacheResult{}
	err := s.Store.WalkResults(id, func(res solver.CacheResult) error {
		boltresults = append(boltresults, res)
		return nil
	})
	if err != nil {
		return err
	}

	for _, res := range boltresults {
		err := fn(res)
		if err != nil {
			return err
		}
	}

	results, err := selectAllCacheResultWhereStoreID(s.DB, id)
	if err != nil {
		return err
	}

	sort.Slice(boltresults, func(i, j int) bool {
		return boltresults[i].ID < boltresults[j].ID
	})
	sort.Slice(results, func(i, j int) bool {
		return results[i].ID < results[j].ID
	})

	if len(boltresults) != len(results) {
		bklog.G(context.TODO()).Errorf("MyKeyStorage.WalkResults(%s) len(boltresults) != len(results): %v != %v", id, len(boltresults), len(results))
		for i := range boltresults {
			bklog.G(context.TODO()).Errorf("MyKeyStorage.WalkResults(%s) boltresults[i].ID: %v", id, boltresults[i].ID)
		}
		for i := range results {
			bklog.G(context.TODO()).Errorf("MyKeyStorage.WalkResults(%s) results[i].ID: %v", id, results[i].ID)
		}
	}

	for i := range boltresults {
		if boltresults[i].ID != results[i].ID {
			bklog.G(context.TODO()).Errorf("MyKeyStorage.WalkResults(%s) boltresults[i].ID != results[i].ID: %v != %v", id, boltresults[i].ID, results[i].ID)
		}

		if boltresults[i].CreatedAt.UTC().Round(time.Microsecond) != results[i].CreatedAt.UTC() {
			bklog.G(context.TODO()).Errorf("MyKeyStorage.WalkResults(%s) boltresults[i].CreatedAt != results[i].CreatedAt: %v != %v", id, boltresults[i].CreatedAt.UTC().Round(time.Microsecond), results[i].CreatedAt.UTC())
		}
	}

	return s.Store.WalkResults(id, fn)
}

func selectAllCacheResultWhereStoreID(db *sql.DB, storeID string) ([]solver.CacheResult, error) {
	var results []solver.CacheResult
	rows, err := db.Query("SELECT created_at, result_id FROM result WHERE store_id = ?", storeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var res solver.CacheResult
		err := rows.Scan(&res.CreatedAt, &res.ID)
		if err != nil {
			return nil, err
		}
		results = append(results, res)
	}
	return results, nil
}

func (s *MyKeyStorage) Load(id string, resultID string) (solver.CacheResult, error) {
	bklog.G(context.TODO()).Warnf("MyKeyStorage.Load(%s, %s)", id, resultID)

	boltres, err := s.Store.Load(id, resultID)
	if err != nil {
		bklog.G(context.TODO()).Errorf("MyKeyStorage.Load(%s, %s) failed: %v", id, resultID, err)
	}

	res, err := selectCacheResultWhereStoreIDAndResultID(s.DB, id, resultID)
	if err != nil {
		bklog.G(context.TODO()).Errorf("MyKeyStorage.Load(%s, %s) failed: %v", id, resultID, err)
	}

	if boltres.ID != res.ID {
		bklog.G(context.TODO()).Errorf("MyKeyStorage.Load(%s, %s) boltres.ID != res.ID: %v != %v", id, resultID, boltres.ID, res.ID)
	}
	if boltres.CreatedAt.UTC().Round(time.Microsecond) != res.CreatedAt.UTC() {
		bklog.G(context.TODO()).Errorf("MyKeyStorage.Load(%s, %s) boltres.CreatedAt != res.CreatedAt: %v != %v", id, resultID, boltres.CreatedAt.UTC().Round(time.Microsecond), res.CreatedAt.UTC())
	}

	return boltres, err
}

func selectCacheResultWhereStoreIDAndResultID(db *sql.DB, storeID string, resultID string) (solver.CacheResult, error) {
	var res solver.CacheResult
	row := db.QueryRow("SELECT created_at, result_id FROM result WHERE store_id = ? AND result_id = ?", storeID, resultID)
	err := row.Scan(&res.CreatedAt, &res.ID)
	if err != nil {
		return solver.CacheResult{}, err
	}
	return res, nil
}

func (s *MyKeyStorage) AddResult(id string, res solver.CacheResult) error {
	bklog.G(context.TODO()).Warnf("MyKeyStorage.AddResult(%s, %v)", id, res)
	err := addResult(s.DB, id, res)
	if err != nil {
		bklog.G(context.TODO()).Errorf("MyKeyStorage.AddResult(%s, %v) failed: %v", id, res, err)
	}
	err = addResultIntoLinks(s.DB, id)
	if err != nil {
		bklog.G(context.TODO()).Errorf("MyKeyStorage.AddResult(%s, %v) failed: %v", id, res, err)
	}
	return s.Store.AddResult(id, res)
}

func addResult(db *sql.DB, id string, res solver.CacheResult) error {
	_, err := db.Exec("INSERT INTO result (store_id, result_id, created_at) VALUES (?, ?, ?)", id, res.ID, res.CreatedAt)
	return err
}

func addResultIntoLinks(db *sql.DB, id string) error {
	_, err := db.Exec("INSERT INTO link (link_id) VALUES (?)", id)
	return err
}

func (s *MyKeyStorage) Release(resultID string) error {
	// TODO: i think this may be recursive... too tired will check later
	bklog.G(context.TODO()).Warnf("MyKeyStorage.Release(%s)", resultID)
	return s.Store.Release(resultID)
}

/*
func deleteReleaseID(db *sql.DB, id string) error {
	_, err := db.Exec("DELETE FROM result WHERE result_id = ?", id)
	return err
}
*/

// TODO: This is only used in one place.  If the id does not match a key It just wants the entire list of IDs.
func (s *MyKeyStorage) WalkIDsByResult(resultID string, fn func(string) error) error {
	bklog.G(context.TODO()).Warnf("MyKeyStorage.WalkIDsByResult(%s)", resultID)
	boltIds := []string{}
	err := s.Store.WalkIDsByResult(resultID, func(id string) error {
		boltIds = append(boltIds, id)
		return nil
	})
	if err != nil {
		return err
	}

	for _, id := range boltIds {
		err := fn(id)
		if err != nil {
			return err
		}
	}

	ids, err := selectStoreIdsWhereResult(s.DB, resultID)
	if err != nil {
		return err
	}
	sort.Strings(boltIds)
	sort.Strings(ids)
	if !slices.Equal(boltIds, ids) {
		bklog.G(context.TODO()).Errorf("MyKeyStorage.WalkIDsByResult(%s) boltIds != ids: %v != %v", resultID, boltIds, ids)
	}

	return nil
}

func selectStoreIdsWhereResult(db *sql.DB, resultID string) ([]string, error) {
	var ids []string
	rows, err := db.Query("SELECT store_id FROM result WHERE result_id = ?", resultID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		err := rows.Scan(&id)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (s *MyKeyStorage) AddLink(id string, link solver.CacheInfoLink, target string) error {
	// INSERT INTO links (link_id, digest, selector, link.input, link.output, target) VALUES (id, link.Digest, link.selector, link.input, link.output, target)
	bklog.G(context.TODO()).Warnf("MyKeyStorage.AddLink(%s, %v, %s)", id, link, target)
	err := addLink(s.DB, id, link, target)
	if err != nil {
		bklog.G(context.TODO()).Errorf("MyKeyStorage.AddLink(%s, %v, %s) failed: %v", id, link, target, err)
	}
	return s.Store.AddLink(id, link, target)
}

func addLink(db *sql.DB, id string, link solver.CacheInfoLink, target string) error {
	_, err := db.Exec("INSERT INTO link (link_id, digest, selector, input, output, target) VALUES (?, ?, ?, ?, ?, ?)", id, link.Digest, link.Selector, link.Input, link.Output, target)
	return err
}

// TODO: used twice. In both cases it creates lists of the ids.
func (s *MyKeyStorage) WalkLinks(id string, link solver.CacheInfoLink, fn func(id string) error) error {
	bklog.G(context.TODO()).Warnf("MyKeyStorage.WalkLinks(%s, %v)", id, link)
	targets, err := selectTarget(s.DB, id, link)
	if err != nil {
		bklog.G(context.TODO()).Errorf("MyKeyStorage.WalkLinks(%s, %v) failed: %v", id, link, err)
	}

	boltTargets := []string{}
	err = s.Store.WalkLinks(id, link, func(id string) error {
		boltTargets = append(boltTargets, id)
		return nil
	})
	if err != nil {
		bklog.G(context.TODO()).Errorf("MyKeyStorage.WalkLinks(%s, %v) failed: %v", id, link, err)
	}

	for _, t := range boltTargets {
		err := fn(t)
		if err != nil {
			bklog.G(context.TODO()).Errorf("MyKeyStorage.WalkLinks(%s, %v) failed: %v", id, link, err)
			return err
		}
	}

	sort.Strings(boltTargets)
	sort.Strings(targets)
	if !slices.Equal(boltTargets, targets) {
		bklog.G(context.TODO()).Errorf("MyKeyStorage.WalkLinks(%s, %v) boltTargets != targets: %v != %v", id, link, boltTargets, targets)
	}

	return nil
}

func selectTarget(db *sql.DB, id string, link solver.CacheInfoLink) ([]string, error) {
	var targets []string
	rows, err := db.Query("SELECT target FROM link WHERE link_id = ? AND digest = ? AND selector = ? AND input = ? AND output = ?", id, link.Digest, link.Selector, link.Input, link.Output)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var target string
		err := rows.Scan(&target)
		if err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	return targets, nil
}

func (s *MyKeyStorage) HasLink(id string, link solver.CacheInfoLink, target string) bool {
	bklog.G(context.TODO()).Warnf("MyKeyStorage.HasLink(%s, %v, %s)", id, link, target)
	return hasLink(s.DB, id, link, target) || s.Store.HasLink(id, link, target)
	//return s.Store.HasLink(id, link, target)
}

func hasLink(db *sql.DB, id string, link solver.CacheInfoLink, target string) bool {
	var v bool
	err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM link WHERE link_id = ? AND digest = ? AND selector = ? AND input = ? AND output = ? AND target = ?)", id, link.Digest, link.Selector, link.Input, link.Output, target).Scan(&v)
	if err != nil {
		bklog.G(context.TODO()).Errorf("hasLink(%s, %v, %s) failed: %v", id, link, target, err)
	}
	return v
}

// TODO: used only one time.  Recursively called.  Looks like a depth first search that accumulates the link digest, input and selector.
func (s *MyKeyStorage) WalkBacklinks(id string, fn func(id string, link solver.CacheInfoLink) error) error {
	bklog.G(context.TODO()).Warnf("MyKeyStorage.WalkBacklinks(%s)", id)
	boltIDs := []string{}
	boltLinks := []solver.CacheInfoLink{}
	err := s.Store.WalkBacklinks(id, func(id string, link solver.CacheInfoLink) error {
		boltIDs = append(boltIDs, id)
		boltLinks = append(boltLinks, link)
		return nil
	})
	if err != nil {
		return err
	}

	for i := range boltIDs {
		err := fn(boltIDs[i], boltLinks[i])
		if err != nil {
			return err
		}
	}

	ids, links, err := selectAllLinksWhereTarget(s.DB, id)
	if err != nil {
		return err
	}
	_ = links

	sort.Strings(boltIDs)
	sort.Strings(ids)
	if !slices.Equal(boltIDs, ids) {
		bklog.G(context.TODO()).Errorf("MyKeyStorage.WalkBacklinks boltIDs != ids: %v != %v", boltIDs, ids)
	}

	return nil
}

func selectAllLinksWhereTarget(db *sql.DB, target string) ([]string, []solver.CacheInfoLink, error) {
	var (
		ids   []string
		links []solver.CacheInfoLink
	)

	rows, err := db.Query("SELECT link_id, digest, selector, input, output FROM link WHERE target = ?", target)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			link solver.CacheInfoLink
			id   string
		)

		err := rows.Scan(&id, &link.Digest, &link.Selector, &link.Input, &link.Output)
		if err != nil {
			return nil, nil, err
		}
		ids = append(ids, id)
		links = append(links, link)
	}
	return ids, links, nil
}

type Store struct {
	db *bolt.DB
}

func NewStore(dbPath string) (*Store, error) {
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open database file %s", dbPath)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, b := range []string{resultBucket, linksBucket, byResultBucket, backlinksBucket} {
			if _, err := tx.CreateBucketIfNotExists([]byte(b)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Exists(id string) bool {
	exists := false
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(linksBucket)).Bucket([]byte(id))
		exists = b != nil
		return nil
	})
	if err != nil {
		return false
	}
	return exists
}

func (s *Store) Walk(fn func(id string) error) error {
	ids := make([]string, 0)
	if err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(linksBucket))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if v == nil {
				ids = append(ids, string(k))
			}
		}
		return nil
	}); err != nil {
		return err
	}
	for _, id := range ids {
		if err := fn(id); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) WalkResults(id string, fn func(solver.CacheResult) error) error {
	var list []solver.CacheResult
	if err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(resultBucket))
		if b == nil {
			return nil
		}
		b = b.Bucket([]byte(id))
		if b == nil {
			return nil
		}

		return b.ForEach(func(k, v []byte) error {
			var res solver.CacheResult
			if err := json.Unmarshal(v, &res); err != nil {
				return err
			}
			list = append(list, res)
			return nil
		})
	}); err != nil {
		return err
	}
	for _, res := range list {
		if err := fn(res); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Load(id string, resultID string) (solver.CacheResult, error) {
	var res solver.CacheResult
	if err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(resultBucket))
		if b == nil {
			return errors.WithStack(solver.ErrNotFound)
		}
		b = b.Bucket([]byte(id))
		if b == nil {
			return errors.WithStack(solver.ErrNotFound)
		}

		v := b.Get([]byte(resultID))
		if v == nil {
			return errors.WithStack(solver.ErrNotFound)
		}

		return json.Unmarshal(v, &res)
	}); err != nil {
		return solver.CacheResult{}, err
	}
	return res, nil
}

func (s *Store) AddResult(id string, res solver.CacheResult) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.Bucket([]byte(linksBucket)).CreateBucketIfNotExists([]byte(id))
		if err != nil {
			return err
		}

		b, err := tx.Bucket([]byte(resultBucket)).CreateBucketIfNotExists([]byte(id))
		if err != nil {
			return err
		}
		dt, err := json.Marshal(res)
		if err != nil {
			return err
		}
		if err := b.Put([]byte(res.ID), dt); err != nil {
			return err
		}
		b, err = tx.Bucket([]byte(byResultBucket)).CreateBucketIfNotExists([]byte(res.ID))
		if err != nil {
			return err
		}
		if err := b.Put([]byte(id), []byte{}); err != nil {
			return err
		}

		return nil
	})
}

func (s *Store) WalkIDsByResult(resultID string, fn func(string) error) error {
	ids := map[string]struct{}{}
	if err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(byResultBucket))
		if b == nil {
			return nil
		}
		b = b.Bucket([]byte(resultID))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			ids[string(k)] = struct{}{}
			return nil
		})
	}); err != nil {
		return err
	}
	for id := range ids {
		if err := fn(id); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Release(resultID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(byResultBucket))
		if b == nil {
			return errors.WithStack(solver.ErrNotFound)
		}
		b = b.Bucket([]byte(resultID))
		if b == nil {
			return errors.WithStack(solver.ErrNotFound)
		}
		if err := b.ForEach(func(k, v []byte) error {
			return s.releaseHelper(tx, string(k), resultID)
		}); err != nil {
			return err
		}
		return nil
	})
}

func (s *Store) releaseHelper(tx *bolt.Tx, id, resultID string) error {
	results := tx.Bucket([]byte(resultBucket)).Bucket([]byte(id))
	if results == nil {
		return nil
	}

	if err := results.Delete([]byte(resultID)); err != nil {
		return err
	}

	ids := tx.Bucket([]byte(byResultBucket))

	ids = ids.Bucket([]byte(resultID))
	if ids == nil {
		return nil
	}

	if err := ids.Delete([]byte(id)); err != nil {
		return err
	}

	if isEmptyBucket(ids) {
		if err := tx.Bucket([]byte(byResultBucket)).DeleteBucket([]byte(resultID)); err != nil {
			return err
		}
	}

	return s.emptyBranchWithParents(tx, []byte(id))
}

func (s *Store) emptyBranchWithParents(tx *bolt.Tx, id []byte) error {
	results := tx.Bucket([]byte(resultBucket)).Bucket(id)
	if results == nil {
		return nil
	}

	isEmptyLinks := true
	links := tx.Bucket([]byte(linksBucket)).Bucket(id)
	if links != nil {
		isEmptyLinks = isEmptyBucket(links)
	}

	if !isEmptyBucket(results) || !isEmptyLinks {
		return nil
	}

	if backlinks := tx.Bucket([]byte(backlinksBucket)).Bucket(id); backlinks != nil {
		if err := backlinks.ForEach(func(k, v []byte) error {
			if subLinks := tx.Bucket([]byte(linksBucket)).Bucket(k); subLinks != nil {
				if err := subLinks.ForEach(func(k, v []byte) error {
					parts := bytes.Split(k, []byte("@"))
					if len(parts) != 2 {
						return errors.Errorf("invalid key %s", k)
					}
					if bytes.Equal(id, parts[1]) {
						return subLinks.Delete(k)
					}
					return nil
				}); err != nil {
					return err
				}

				if isEmptyBucket(subLinks) {
					if err := tx.Bucket([]byte(linksBucket)).DeleteBucket(k); err != nil {
						return err
					}
				}
			}
			return s.emptyBranchWithParents(tx, k)
		}); err != nil {
			return err
		}
		if err := tx.Bucket([]byte(backlinksBucket)).DeleteBucket(id); err != nil {
			return err
		}
	}

	// intentionally ignoring errors
	tx.Bucket([]byte(linksBucket)).DeleteBucket([]byte(id))
	tx.Bucket([]byte(resultBucket)).DeleteBucket([]byte(id))

	return nil
}

func (s *Store) AddLink(id string, link solver.CacheInfoLink, target string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.Bucket([]byte(linksBucket)).CreateBucketIfNotExists([]byte(id))
		if err != nil {
			return err
		}

		dt, err := json.Marshal(link)
		if err != nil {
			return err
		}

		if err := b.Put(bytes.Join([][]byte{dt, []byte(target)}, []byte("@")), []byte{}); err != nil {
			return err
		}

		b, err = tx.Bucket([]byte(backlinksBucket)).CreateBucketIfNotExists([]byte(target))
		if err != nil {
			return err
		}

		if err := b.Put([]byte(id), []byte{}); err != nil {
			return err
		}

		return nil
	})
}

func (s *Store) WalkLinks(id string, link solver.CacheInfoLink, fn func(id string) error) error {
	var links []string
	if err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(linksBucket))
		if b == nil {
			return nil
		}
		b = b.Bucket([]byte(id))
		if b == nil {
			return nil
		}

		dt, err := json.Marshal(link)
		if err != nil {
			return err
		}
		index := bytes.Join([][]byte{dt, {}}, []byte("@"))
		c := b.Cursor()
		k, _ := c.Seek([]byte(index))
		for {
			if k != nil && bytes.HasPrefix(k, index) {
				target := bytes.TrimPrefix(k, index)
				links = append(links, string(target))
				k, _ = c.Next()
			} else {
				break
			}
		}

		return nil
	}); err != nil {
		return err
	}
	for _, l := range links {
		if err := fn(l); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) HasLink(id string, link solver.CacheInfoLink, target string) bool {
	var v bool
	if err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(linksBucket))
		if b == nil {
			return nil
		}
		b = b.Bucket([]byte(id))
		if b == nil {
			return nil
		}

		dt, err := json.Marshal(link)
		if err != nil {
			return err
		}
		v = b.Get(bytes.Join([][]byte{dt, []byte(target)}, []byte("@"))) != nil
		return nil
	}); err != nil {
		return false
	}
	return v
}

func (s *Store) WalkBacklinks(id string, fn func(id string, link solver.CacheInfoLink) error) error {
	var outIDs []string
	var outLinks []solver.CacheInfoLink

	if err := s.db.View(func(tx *bolt.Tx) error {
		links := tx.Bucket([]byte(linksBucket))
		if links == nil {
			return nil
		}
		backLinks := tx.Bucket([]byte(backlinksBucket))
		if backLinks == nil {
			return nil
		}
		backlinksBucket := backLinks.Bucket([]byte(id))
		if backlinksBucket == nil {
			return nil
		}

		if err := backlinksBucket.ForEach(func(bid, v []byte) error {
			backlinksBucket = links.Bucket(bid)
			if backlinksBucket == nil {
				return nil
			}
			if err := backlinksBucket.ForEach(func(k, v []byte) error {
				parts := bytes.Split(k, []byte("@"))
				if len(parts) == 2 {
					if string(parts[1]) != id {
						return nil
					}
					var l solver.CacheInfoLink
					if err := json.Unmarshal(parts[0], &l); err != nil {
						return err
					}
					l.Digest = digest.FromBytes([]byte(fmt.Sprintf("%s@%d", l.Digest, l.Output)))
					l.Output = 0
					outIDs = append(outIDs, string(bid))
					outLinks = append(outLinks, l)
				}
				return nil
			}); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	for i := range outIDs {
		if err := fn(outIDs[i], outLinks[i]); err != nil {
			return err
		}
	}
	return nil
}

func isEmptyBucket(b *bolt.Bucket) bool {
	if b == nil {
		return true
	}
	k, _ := b.Cursor().First()
	return k == nil
}
