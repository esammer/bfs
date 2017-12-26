package ns

import (
	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestNamespace_Open(t *testing.T) {
	ns := New("build/test/" + t.Name() + "/db")

	err := ns.Open()
	require.NoError(t, err, "Open failed")

	err = ns.Add("/a.txt", []string{"1", "2"})
	require.NoError(t, err)
	err = ns.Add("/b.txt", []string{"3", "4", "5", "6"})
	require.NoError(t, err)
	err = ns.Add("/c.txt", []string{})
	require.NoError(t, err)

	entry, err := ns.Get("/a.txt")
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, entry, &Entry{
		Path:        "/a.txt",
		Blocks:      []string{"1", "2"},
		Permissions: 0,
		Status:      FileStatus_Unknown,
	})

	entries, err := ns.List("/", "/z")
	require.Len(t, entries, 3)
	require.Equal(
		t,
		[]*Entry{
			{Path: "/a.txt", Blocks: []string{"1", "2"}},
			{Path: "/b.txt", Blocks: []string{"3", "4", "5", "6"}},
			{Path: "/c.txt", Blocks: []string{}},
		},
		entries,
	)
	require.NoError(t, err)

	err = ns.Close()
	require.NoError(t, err, "Close failed")

	glog.Flush()

	err = os.RemoveAll("build/test/" + t.Name())
	require.NoError(t, err)
}
