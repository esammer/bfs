package volume

import (
	"bfs/block"
	"bfs/ns"
	"bfs/test"
	"bytes"
	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
	"io"
	"path/filepath"
	"testing"
)

func BenchmarkFileSystem_Write(b *testing.B) {
	glog.Info("Starting write benchmark")

	eventChannel := make(chan interface{}, 1024)

	go func() {
		for event := range eventChannel {
			glog.Infof("Received event %v", event)

			switch val := event.(type) {
			case *block.BlockWriteEvent:
				val.AckChannel <- event
				glog.Infof("Acknowledged block write %v", val)
			}
		}

		glog.Info("Response loop ended")
	}()

	testDir := test.New("build", "test", b.Name())
	err := testDir.Create()
	require.NoError(b, err)

	pv1 := NewPhysicalVolume(filepath.Join(testDir.Path, "data", "pv1"), eventChannel)
	err = pv1.Open(true)
	require.NoError(b, err)

	pv2 := NewPhysicalVolume(filepath.Join(testDir.Path, "data", "pv2"), eventChannel)
	err = pv2.Open(true)
	require.NoError(b, err)

	lv1 := NewLogicalVolume("/logs", []*PhysicalVolume{pv1, pv2}, eventChannel)

	fs := &LocalFileSystem{
		Namespace:    ns.New(filepath.Join(testDir.Path, "ns")),
		Volumes:      []*LogicalVolume{lv1},
		EventChannel: eventChannel,
	}

	err = fs.Open()
	require.NoError(b, err)

	sourceBuf := bytes.Repeat([]byte{0}, 1024*1024)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		writer, err := fs.OpenWrite("/logs/a.log", 1024*1024)
		require.NoError(b, err)

		for j := 0; j < 10; j++ {
			_, err = writer.Write(sourceBuf)
			require.NoError(b, err)
		}

		err = writer.Close()
		require.NoError(b, err)
	}

	b.StopTimer()

	close(eventChannel)

	err = fs.Close()
	require.NoError(b, err)

	err = testDir.Destroy()
	require.NoError(b, err)

	glog.Info("Ending write benchmark")
}

func TestFileSystem(t *testing.T) {
	testDir := test.New("build", "test", t.Name())
	err := testDir.Create()
	require.NoError(t, err)

	eventChannel := make(chan interface{}, 1024)

	go func() {
		for event := range eventChannel {
			glog.Infof("Received event %v", event)

			switch val := event.(type) {
			case *block.BlockWriteEvent:
				val.AckChannel <- event
				glog.Infof("Acknowledged block write %v", val)
			}
		}

		glog.Info("Response loop ended")
	}()

	pv1 := NewPhysicalVolume(filepath.Join(testDir.Path, "data", "pv1"), eventChannel)
	err = pv1.Open(true)
	require.NoError(t, err)

	pv2 := NewPhysicalVolume(filepath.Join(testDir.Path, "data", "pv2"), eventChannel)
	err = pv2.Open(true)
	require.NoError(t, err)

	pv3 := NewPhysicalVolume(filepath.Join(testDir.Path, "data", "pv3"), eventChannel)
	err = pv3.Open(true)
	require.NoError(t, err)

	lv1 := NewLogicalVolume("/logs", []*PhysicalVolume{pv1, pv2}, eventChannel)
	lv2 := NewLogicalVolume("/txs", []*PhysicalVolume{pv3}, eventChannel)

	fs := &LocalFileSystem{
		Namespace: ns.New(filepath.Join(testDir.Path, "ns")),
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

	close(eventChannel)

	err = testDir.Destroy()
	require.NoError(t, err)
}
