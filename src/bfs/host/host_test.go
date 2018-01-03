package host

import (
	"bfs/test"
	"bfs/volume"
	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestHost_New(t *testing.T) {
	testDir := test.New("build", "test", t.Name())
	err := testDir.Create()
	require.NoError(t, err)

	os.Mkdir(filepath.Join(testDir.Path, "a"), 0755)
	pv := volume.NewPhysicalVolume(filepath.Join(testDir.Path, "pv1"), nil)
	require.NoError(t, pv.Open(true))
	require.NoError(t, pv.Close())

	config := &HostConfig{
		RootPath: testDir.Path,
	}

	host, err := New(config)
	require.NoError(t, err)
	require.Equal(t, 1, len(host.Volumes))
	require.Equal(t, pv.RootPath, host.Volumes[0].RootPath)

	glog.Infof("Host: %v", host)

	for _, pv := range host.Volumes {
		require.NoError(t, pv.Open(false))
		require.NoError(t, pv.Close())
	}

	err = testDir.Destroy()
	require.NoError(t, err)
}
