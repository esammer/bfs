package ns

import (
	"bfs/test"
	"bfs/util/size"
	"bytes"
	"fmt"
	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
	"time"
)

func TestNamespace_Open(t *testing.T) {
	defer glog.Flush()

	testDir := test.New("build", "test", t.Name())
	err := testDir.Create()
	require.NoError(t, err)

	ns := New(filepath.Join(testDir.Path, "/db"))

	err = ns.Open()
	require.NoError(t, err, "Open failed")

	err = ns.Add(
		&Entry{
			VolumeName: "/",
			Path:       "/a.txt",
			Blocks: []*BlockMetadata{
				{Block: "1", LVName: "/", PVID: "1"},
				{Block: "2", LVName: "/", PVID: "1"},
			},
		},
	)
	require.NoError(t, err)
	err = ns.Add(
		&Entry{
			VolumeName: "/",
			Path:       "/b.txt",
			Blocks: []*BlockMetadata{
				{Block: "3", LVName: "/", PVID: "1"},
				{Block: "4", LVName: "/", PVID: "1"},
				{Block: "5", LVName: "/", PVID: "1"},
				{Block: "6", LVName: "/", PVID: "1"},
			},
		},
	)
	require.NoError(t, err)
	err = ns.Add(&Entry{VolumeName: "/", Path: "/c.txt", Blocks: []*BlockMetadata{}})
	require.NoError(t, err)

	entry, err := ns.Get("/a.txt")
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, entry, &Entry{
		VolumeName: "/",
		Path:       "/a.txt",
		Blocks: []*BlockMetadata{
			{Block: "1", LVName: "/", PVID: "1"},
			{Block: "2", LVName: "/", PVID: "1"},
		},
		Permissions: 0,
		Status:      FileStatus_Unknown,
	})

	entries := make([]*Entry, 0, 8)
	err = ns.List("/", "/z", func(entry *Entry, err error) (bool, error) {
		if err != nil {
			return false, err
		}

		entries = append(entries, entry)
		return true, nil
	})
	require.NoError(t, err)
	require.Len(t, entries, 3)
	require.Equal(
		t,
		[]*Entry{
			{VolumeName: "/", Path: "/a.txt", Blocks: []*BlockMetadata{
				{Block: "1", LVName: "/", PVID: "1"},
				{Block: "2", LVName: "/", PVID: "1"},
			}, Permissions: 0, Status: FileStatus_Unknown},
			{VolumeName: "/", Path: "/b.txt", Blocks: []*BlockMetadata{
				{Block: "3", LVName: "/", PVID: "1"},
				{Block: "4", LVName: "/", PVID: "1"},
				{Block: "5", LVName: "/", PVID: "1"},
				{Block: "6", LVName: "/", PVID: "1"},
			}, Permissions: 0, Status: FileStatus_Unknown},
			{VolumeName: "/", Path: "/c.txt", Blocks: []*BlockMetadata{}, Permissions: 0, Status: FileStatus_Unknown},
		},
		entries,
	)
	require.NoError(t, err)

	deleted, err := ns.Delete("/a.txt", false)
	require.NoError(t, err)
	require.Equal(t, uint32(1), deleted)

	now := time.Now()
	for i := 0; i < 2; i++ {
		for j := 0; j < 10; j++ {
			require.NoError(t, ns.Add(&Entry{
				VolumeName:       "/",
				Path:             fmt.Sprintf("/c%d/%d.txt", i, j),
				Size:             size.MB,
				BlockSize:        512 * size.KB,
				ReplicationLevel: 1,
				Status:           FileStatus_OK,
				Permissions:      0,
				Ctime:            now,
				Mtime:            now,
				Blocks: []*BlockMetadata{
					{LVName: "/", Block: "1", PVID: "1"},
					{LVName: "/", Block: "2", PVID: "2"},
					{LVName: "/", Block: "3", PVID: "1"},
				},
			}))
		}

		deleted, err = ns.Delete(fmt.Sprintf("/c%d/", i), true)
		require.NoError(t, err)
		require.Equal(t, uint32(10), deleted)
	}

	remainingFiles := 0
	ns.List("/c", "", func(entry *Entry, err error) (bool, error) {
		if err != nil {
			return false, err
		}

		remainingFiles++
		return true, nil
	})

	require.Equal(t, 1, remainingFiles)

	ok, err := ns.Rename("/b.txt", "/a.txt")
	require.NoError(t, err)
	require.True(t, ok)

	entry, err = ns.Get("/a.txt")
	require.NoError(t, err)
	require.Len(t, entry.Blocks, 4)

	err = ns.Close()
	require.NoError(t, err, "Close failed")

	glog.Flush()

	err = testDir.Destroy()
	require.NoError(t, err)
}

func TestNamespace_keyFor(t *testing.T) {
	key := keyFor(dbPrefix_Entry, "a")
	require.NotNil(t, key)

	require.Equal(
		t,
		bytes.Join([][]byte{
			{dbPrefix_Entry},
			[]byte("a"),
		},
			nil,
		),
		key,
	)
}

func BenchmarkNamespace(b *testing.B) {
	testDir := test.New("build", "test", b.Name())
	err := testDir.Create()
	require.NoError(b, err)

	ns := New(filepath.Join(testDir.Path, "/db"))

	err = ns.Open()
	require.NoError(b, err, "Open failed")
	defer ns.Close()

	b.ResetTimer()

	b.Run("Add", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			now := time.Now()
			err = ns.Add(
				&Entry{
					VolumeName:       "/",
					Path:             fmt.Sprintf("/a%d.txt", i),
					BlockSize:        size.MB,
					Size:             10 * size.MB,
					Permissions:      0,
					ReplicationLevel: 1,
					Status:           FileStatus_OK,
					Blocks: []*BlockMetadata{
						{Block: "1", LVName: "/", PVID: "1"},
						{Block: "2", LVName: "/", PVID: "1"},
						{Block: "3", LVName: "/", PVID: "1"},
						{Block: "4", LVName: "/", PVID: "1"},
						{Block: "5", LVName: "/", PVID: "1"},
						{Block: "5", LVName: "/", PVID: "1"},
						{Block: "6", LVName: "/", PVID: "1"},
						{Block: "7", LVName: "/", PVID: "1"},
						{Block: "8", LVName: "/", PVID: "1"},
						{Block: "9", LVName: "/", PVID: "1"},
						{Block: "10", LVName: "/", PVID: "1"},
					},
					Ctime: now,
					Mtime: now,
				},
			)
			require.NoError(b, err)
		}
	})
	b.Run("Delete", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			deleted, err := ns.Delete(fmt.Sprintf("/b%d.txt", i), false)
			require.NoError(b, err)
			require.Equal(b, 1, deleted)
		}
	})
	b.Run("List", func(b *testing.B) {
		b.StopTimer()
		for i := 0; i < 1000; i++ {
			now := time.Now()
			err = ns.Add(
				&Entry{
					VolumeName:       "/",
					Path:             fmt.Sprintf("/c%d.txt", i),
					BlockSize:        size.MB,
					Size:             10 * size.MB,
					Permissions:      0,
					ReplicationLevel: 1,
					Status:           FileStatus_OK,
					Blocks: []*BlockMetadata{
						{Block: "1", LVName: "/", PVID: "1"},
						{Block: "2", LVName: "/", PVID: "1"},
						{Block: "3", LVName: "/", PVID: "1"},
						{Block: "4", LVName: "/", PVID: "1"},
						{Block: "5", LVName: "/", PVID: "1"},
						{Block: "5", LVName: "/", PVID: "1"},
						{Block: "6", LVName: "/", PVID: "1"},
						{Block: "7", LVName: "/", PVID: "1"},
						{Block: "8", LVName: "/", PVID: "1"},
						{Block: "9", LVName: "/", PVID: "1"},
						{Block: "10", LVName: "/", PVID: "1"},
					},
					Ctime: now,
					Mtime: now,
				},
			)
			require.NoError(b, err)
		}
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			ns.List("", "", func(entry *Entry, err error) (bool, error) {
				return true, nil
			})
		}
	})
	b.Run("Get", func(b *testing.B) {
		b.StopTimer()
		for i := 0; i < 1000; i++ {
			now := time.Now()
			err = ns.Add(
				&Entry{
					VolumeName:       "/",
					Path:             fmt.Sprintf("/d%d.txt", i),
					BlockSize:        size.MB,
					Size:             10 * size.MB,
					Permissions:      0,
					ReplicationLevel: 1,
					Status:           FileStatus_OK,
					Blocks: []*BlockMetadata{
						{Block: "1", LVName: "/", PVID: "1"},
						{Block: "2", LVName: "/", PVID: "1"},
						{Block: "3", LVName: "/", PVID: "1"},
						{Block: "4", LVName: "/", PVID: "1"},
						{Block: "5", LVName: "/", PVID: "1"},
						{Block: "5", LVName: "/", PVID: "1"},
						{Block: "6", LVName: "/", PVID: "1"},
						{Block: "7", LVName: "/", PVID: "1"},
						{Block: "8", LVName: "/", PVID: "1"},
						{Block: "9", LVName: "/", PVID: "1"},
						{Block: "10", LVName: "/", PVID: "1"},
					},
					Ctime: now,
					Mtime: now,
				},
			)
			require.NoError(b, err)
		}
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			// Purposefully cause a small number of misses.
			ns.Get(fmt.Sprintf("/d%d.txt", i%1010))
		}
	})
	b.Run("Rename", func(b *testing.B) {
		b.StopTimer()
		now := time.Now()
		err = ns.Add(
			&Entry{
				VolumeName:       "/",
				Path:             "0",
				BlockSize:        size.MB,
				Size:             10 * size.MB,
				Permissions:      0,
				ReplicationLevel: 1,
				Status:           FileStatus_OK,
				Blocks: []*BlockMetadata{
					{Block: "1", LVName: "/", PVID: "1"},
					{Block: "2", LVName: "/", PVID: "1"},
					{Block: "3", LVName: "/", PVID: "1"},
					{Block: "4", LVName: "/", PVID: "1"},
					{Block: "5", LVName: "/", PVID: "1"},
					{Block: "5", LVName: "/", PVID: "1"},
					{Block: "6", LVName: "/", PVID: "1"},
					{Block: "7", LVName: "/", PVID: "1"},
					{Block: "8", LVName: "/", PVID: "1"},
					{Block: "9", LVName: "/", PVID: "1"},
					{Block: "10", LVName: "/", PVID: "1"},
				},
				Ctime: now,
				Mtime: now,
			},
		)
		require.NoError(b, err)
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			ok, err := ns.Rename(fmt.Sprint(i), fmt.Sprint(i+1))
			require.NoError(b, err)
			require.True(b, ok)
		}
	})
}
