package metadata

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetSetSearch(t *testing.T) {
	t.Parallel()

	tmpdir := t.TempDir()

	dbPath := filepath.Join(tmpdir, "storage.db")

	s, err := NewStore(dbPath)
	require.NoError(t, err)
	defer s.Close()

	si, ok := s.Get("foo")
	require.False(t, ok)

	v := si.Get("bar")
	require.Nil(t, v)

	v, err = NewValue("foobar")
	require.NoError(t, err)

	si.Queue("bar", v)

	err = si.Commit()
	require.NoError(t, err)

	v = si.Get("bar")
	require.NotNil(t, v)

	var str string
	err = v.Unmarshal(&str)
	require.NoError(t, err)
	require.Equal(t, "foobar", str)

	err = s.Close()
	require.NoError(t, err)

	s, err = NewStore(dbPath)
	require.NoError(t, err)
	defer s.Close()

	si, ok = s.Get("foo")
	require.True(t, ok)

	v = si.Get("bar")
	require.NotNil(t, v)

	str = ""
	err = v.Unmarshal(&str)
	require.NoError(t, err)
	require.Equal(t, "foobar", str)

	// add second item to test Search

	si, ok = s.Get("foo2")
	require.False(t, ok)

	v, err = NewValue("foobar2")
	require.NoError(t, err)

	si.Queue("bar2", v)

	err = si.Commit()
	require.NoError(t, err)

	ids, err := s.KeyIDs()
	require.NoError(t, err)
	require.Equal(t, 2, len(ids))

	require.Equal(t, "foo", ids[0])
	require.Equal(t, "foo2", ids[1])

	si, ok = s.Get(ids[0])
	require.True(t, ok)
	v = si.Get("bar")
	require.NotNil(t, v)

	str = ""
	err = v.Unmarshal(&str)
	require.NoError(t, err)
	require.Equal(t, "foobar", str)

	// clear foo, check that only foo2 exists
	err = s.Delete(ids[0])
	require.NoError(t, err)

	ids, err = s.KeyIDs()
	require.NoError(t, err)
	require.Equal(t, 1, len(ids))

	require.Equal(t, "foo2", ids[0])

	_, ok = s.Get("foo")
	require.False(t, ok)
}

func TestIndexes(t *testing.T) {
	t.Parallel()

	tmpdir := t.TempDir()

	dbPath := filepath.Join(tmpdir, "storage.db")

	s, err := NewStore(dbPath)
	require.NoError(t, err)
	defer s.Close()

	var tcases = []struct {
		key, valueKey, value, index string
	}{
		{"foo1", "bar", "val1", "tag:baz"},
		{"foo2", "bar", "val2", "tag:bax"},
		{"foo3", "bar", "val3", "tag:baz"},
	}

	for _, tcase := range tcases {
		si, ok := s.Get(tcase.key)
		require.False(t, ok)

		v, err := NewValue(tcase.valueKey)
		require.NoError(t, err)
		v.Index = tcase.index

		si.Queue(tcase.value, v)

		err = si.Commit()
		require.NoError(t, err)
	}

	sis, err := s.Search("tag:baz")
	require.NoError(t, err)
	require.Equal(t, 2, len(sis))

	require.Equal(t, sis[0].ID(), "foo1")
	require.Equal(t, sis[1].ID(), "foo3")

	sis, err = s.Search("tag:bax")
	require.NoError(t, err)
	require.Equal(t, 1, len(sis))

	require.Equal(t, sis[0].ID(), "foo2")

	err = s.Delete("foo1")
	require.NoError(t, err)

	sis, err = s.Search("tag:baz")
	require.NoError(t, err)
	require.Equal(t, 1, len(sis))

	require.Equal(t, sis[0].ID(), "foo3")
}

func TestExternalData(t *testing.T) {
	t.Parallel()

	tmpdir := t.TempDir()

	dbPath := filepath.Join(tmpdir, "storage.db")

	s, err := NewStore(dbPath)
	require.NoError(t, err)
	defer s.Close()

	si, ok := s.Get("foo")
	require.False(t, ok)

	err = si.SetExternal("ext1", []byte("data"))
	require.NoError(t, err)

	dt, err := si.GetExternal("ext1")
	require.NoError(t, err)
	require.Equal(t, "data", string(dt))

	si, ok = s.Get("bar")
	require.False(t, ok)

	_, err = si.GetExternal("ext1")
	require.Error(t, err)

	si, _ = s.Get("foo")
	dt, err = si.GetExternal("ext1")
	require.NoError(t, err)
	require.Equal(t, "data", string(dt))

	err = s.Delete("foo")
	require.NoError(t, err)

	si, _ = s.Get("foo")
	_, err = si.GetExternal("ext1")
	require.Error(t, err)
}
