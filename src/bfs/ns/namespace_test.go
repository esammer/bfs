package ns

import (
	"bfs/test"
	"bytes"
	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

func TestNamespace_Open(t *testing.T) {
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

	entries, err := ns.List("/", "/z")
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
