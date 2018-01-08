package host

import (
	"bfs/test"
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

	config := &HostConfig{
		RootPath: testDir.Path,
	}

	host, err := New(config)
	require.NoError(t, err)

	glog.Infof("Host: %v", host)

	err = testDir.Destroy()
	require.NoError(t, err)
}
