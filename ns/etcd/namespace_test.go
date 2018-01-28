package etcd

import (
	"bfs/ns"
	"bfs/test"
	"bfs/util/size"
	"fmt"
	"github.com/golang/glog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
	"time"
)

func TestEtcdNamespace(t *testing.T) {
	defer glog.Flush()

	testDir := test.New("build", "test", t.Name())
	require.NoError(t, testDir.Create())
	defer testDir.Destroy()

	namespace := New(&Config{
		Path:    testDir.Path,
		GroupId: "ns-shard-1",
		Self:    0,
		Nodes: []*NsNode{
			{Id: "localhost", Hostname: "localhost", BindAddress: "0.0.0.0", ClientPort: 7000,
				PeerPort: 7001},
		},
	})

	assert.NoError(t, namespace.Open())

	for i := 0; i < 10; i++ {
		namespace.Add(&ns.Entry{
			Path: fmt.Sprintf("/%d.txt", i),
			Blocks: []*ns.BlockMetadata{
				{PVID: "1", Block: "1", LVName: "/"},
				{PVID: "2", Block: "2", LVName: "/"},
				{PVID: "1", Block: "3", LVName: "/"},
				{PVID: "2", Block: "4", LVName: "/"},
			},
		})
	}

	entry, err := namespace.Get("/0.txt")
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, &ns.Entry{
		Path: "/0.txt",
		Blocks: []*ns.BlockMetadata{
			{PVID: "1", Block: "1", LVName: "/"},
			{PVID: "2", Block: "2", LVName: "/"},
			{PVID: "1", Block: "3", LVName: "/"},
			{PVID: "2", Block: "4", LVName: "/"},
		},
	}, entry)

	entriesFound := 0
	namespace.List("/", func(entry *ns.Entry, err error) (bool, error) {
		if err == io.EOF {
			return false, nil
		} else if err != nil {
			return false, err
		}

		entriesFound++
		return true, nil
	})
	assert.Equal(t, 10, entriesFound)

	deleted, err := namespace.Remove("/0.txt", false)
	assert.NoError(t, err)
	assert.Equal(t, 1, deleted)

	namespace.Remove("/", true)
	assert.NoError(t, err)

	entriesFound = 0
	namespace.List("/", func(entry *ns.Entry, err error) (bool, error) {
		if err == io.EOF {
			return false, nil
		} else if err != nil {
			return false, err
		}

		entriesFound++
		return true, nil
	})
	assert.Equal(t, 0, entriesFound)

	err = namespace.Add(&ns.Entry{
		Path: "a",
	})
	require.NoError(t, err)

	err = namespace.Rename("a", "b")
	require.NoError(t, err)

	err = namespace.Rename("a", "b")
	require.Error(t, err)

	entry, err = namespace.Get("a")
	require.Error(t, err)

	entry, err = namespace.Get("b")
	require.NoError(t, err)
	require.NotNil(t, entry)

	assert.NoError(t, namespace.Close())
}

func BenchmarkEtcdNamespace(b *testing.B) {
	testDir := test.New("build", "test", b.Name())
	err := testDir.Create()
	require.NoError(b, err)

	n := New(&Config{
		Path:    testDir.Path,
		GroupId: "ns-shard-1",
		Self:    0,
		Nodes: []*NsNode{
			{Id: "localhost", Hostname: "localhost", BindAddress: "0.0.0.0", ClientPort: 7000,
				PeerPort: 7001},
		},
	})

	err = n.Open()
	require.NoError(b, err, "Open failed")
	defer n.Close()

	b.ResetTimer()

	b.Run("Add", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			now := time.Now()
			err = n.Add(
				&ns.Entry{
					VolumeName:       "/",
					Path:             fmt.Sprintf("/a%d.txt", i),
					BlockSize:        size.MB,
					Size:             10 * size.MB,
					Permissions:      0,
					ReplicationLevel: 1,
					Status:           ns.FileStatus_OK,
					Blocks: []*ns.BlockMetadata{
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
			deleted, err := n.Remove(fmt.Sprintf("/b%d.txt", i), false)
			require.NoError(b, err)
			require.Equal(b, 0, deleted)
		}
	})
	b.Run("List", func(b *testing.B) {
		b.StopTimer()
		for i := 0; i < 1000; i++ {
			now := time.Now()
			err = n.Add(
				&ns.Entry{
					VolumeName:       "/",
					Path:             fmt.Sprintf("/c%d.txt", i),
					BlockSize:        size.MB,
					Size:             10 * size.MB,
					Permissions:      0,
					ReplicationLevel: 1,
					Status:           ns.FileStatus_OK,
					Blocks: []*ns.BlockMetadata{
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
			n.List("", func(entry *ns.Entry, err error) (bool, error) {
				return true, nil
			})
		}
	})
	b.Run("Get", func(b *testing.B) {
		b.StopTimer()
		for i := 0; i < 1000; i++ {
			now := time.Now()
			err = n.Add(
				&ns.Entry{
					VolumeName:       "/",
					Path:             fmt.Sprintf("/d%d.txt", i),
					BlockSize:        size.MB,
					Size:             10 * size.MB,
					Permissions:      0,
					ReplicationLevel: 1,
					Status:           ns.FileStatus_OK,
					Blocks: []*ns.BlockMetadata{
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
			n.Get(fmt.Sprintf("/d%d.txt", i%1010))
		}
	})
	b.Run("Rename", func(b *testing.B) {
		b.StopTimer()
		now := time.Now()
		err = n.Add(
			&ns.Entry{
				VolumeName:       "/",
				Path:             "0",
				BlockSize:        size.MB,
				Size:             10 * size.MB,
				Permissions:      0,
				ReplicationLevel: 1,
				Status:           ns.FileStatus_OK,
				Blocks: []*ns.BlockMetadata{
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
			err := n.Rename(fmt.Sprint(i), fmt.Sprint(i+1))
			require.NoError(b, err)
		}
	})
}
