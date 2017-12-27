package volume

import (
	"bfs/ns"
	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestFileSystem(t *testing.T) {
	testDir := filepath.Join("build/test", t.Name())
	err := os.MkdirAll(testDir, 0700)
	require.NoError(t, err)

	pv1 := NewPhysicalVolume(filepath.Join(testDir, "data", "pv1"))
	err = pv1.Open(true)
	require.NoError(t, err)

	pv2 := NewPhysicalVolume(filepath.Join(testDir, "data", "pv2"))
	err = pv2.Open(true)
	require.NoError(t, err)

	pv3 := NewPhysicalVolume(filepath.Join(testDir, "data", "pv3"))
	err = pv3.Open(true)
	require.NoError(t, err)

	lv1 := NewLogicalVolume("/logs", []*PhysicalVolume{pv1, pv2})
	lv2 := NewLogicalVolume("/txs", []*PhysicalVolume{pv3})

	fs := &FileSystem{
		Namespace: ns.New(filepath.Join(testDir, "ns")),
		Volumes:   []*LogicalVolume{lv1, lv2},
	}

	err = fs.Open()
	require.NoError(t, err)

	writer, err := fs.OpenWrite("/a.log", 4)
	require.Nil(t, writer)
	require.Error(t, err)

	writer, err = fs.OpenWrite("/logs/a.log", 4)
	require.NoError(t, err)
	require.NotNil(t, writer)

	_, err = io.WriteString(writer, "Hello world 123 456")
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	reader, err := fs.OpenRead("/logs/a.log")
	require.NoError(t, err)
	require.NotNil(t, reader)

	err = reader.Open()
	require.NoError(t, err)

	buffer := make([]byte, 16)

	for {
		read, err := reader.Read(buffer)
		glog.Infof("Read %d bytes - %s - err %v", read, string(buffer[:read]), err)

		if err != nil {
			if err == io.EOF {
				break
			} else {
				require.NoError(t, err)
			}
		}
	}

	err = reader.Close()
	require.NoError(t, err)

	writer, err = fs.OpenWrite("/txs/a.log", 4)
	require.NoError(t, err)
	require.NotNil(t, writer)

	_, err = io.WriteString(writer, "Hello world 123 456")
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	err = fs.Close()
	require.NoError(t, err)

	err = os.RemoveAll(testDir)
	require.NoError(t, err)
}
