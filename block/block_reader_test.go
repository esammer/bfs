package block

import (
	"bfs/test"
	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalBlockReader(t *testing.T) {
	defer glog.Flush()

	testDir := test.New("build", "test", t.Name())
	require.NoError(t, testDir.Create())
	defer testDir.Destroy()

	writer, err := NewWriter(testDir.Path, "1")
	require.NoError(t, err)

	writeLen, err := writer.Write([]byte("Hello world"))
	require.NoError(t, err)
	require.Equal(t, 11, writeLen)

	require.NoError(t, writer.Close())

	info, err := os.Stat(filepath.Join(testDir.Path, "1"))
	require.NoError(t, err)
	require.True(t, info.Mode().IsRegular())

	reader, err := NewReader(testDir.Path, "1")
	require.NoError(t, err)

	buffer := make([]byte, 512)
	readLen, err := reader.Read(buffer)
	require.NoError(t, err)
	require.Equal(t, 11, readLen)
	require.Equal(t, "Hello world", string(buffer[:readLen]))

	require.NoError(t, reader.Close())
}
