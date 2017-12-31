package volume

import (
	"bfs/block"
	"bfs/test"
	"fmt"
	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
	"io"
	"path/filepath"
	"testing"
)

func TestLogicalVolume_OpenClose(t *testing.T) {
	eventChannel := make(chan interface{}, 1024)

	lv := NewLogicalVolume("/", nil, eventChannel)

	err := lv.Open()
	require.NoError(t, err)

	err = lv.Close()
	require.NoError(t, err)
}

func TestLogicalVolume_ReaderWriter(t *testing.T) {
	testDir := test.New("build", "test", t.Name())
	err := testDir.Create()
	require.NoError(t, err)

	eventChannel := make(chan interface{}, 1024)

	go func() {
		for event := range eventChannel {
			glog.Infof("Received event %#v", event)

			switch val := event.(type) {
			case *block.BlockWriteEvent:
				val.AckChannel <- event
				glog.Infof("Acknowledged block write %v", val)
			case *FileWriteEvent:
				// Ignore file write events.
			default:
				require.Fail(t, "Received unknown event", "Event was: %v", event)
			}
		}

		glog.Info("Response loop ended")
	}()

	pv := NewPhysicalVolume(filepath.Join(testDir.Path, "pv1"), eventChannel)

	err = pv.Open(true)
	require.NoError(t, err, "Failed to open physical volume - %v", err)

	lv := NewLogicalVolume("/", []*PhysicalVolume{pv}, eventChannel)

	err = lv.Open()
	require.NoError(t, err, "Failed to open volume - %v", err)

	writer, err := lv.OpenWrite(new(MockFileSystem), "test1.txt", 1024*1024)
	require.NoError(t, err, "Unable to open writer - %v", err)

	for i := 0; i < 1000000; i++ {
		_, err := io.WriteString(writer, fmt.Sprintf("Test %d\n", i))
		require.NoError(t, err, "Failed to write to writer - %v", err)
	}

	err = writer.Close()
	require.NoError(t, err, "Failed to close writer - %v", err)

	err = lv.Close()
	require.NoError(t, err, "Failed to close volume - %v", err)

	err = pv.Close()
	require.NoError(t, err, "Failed to close physical volume - %v", err)

	close(eventChannel)

	err = testDir.Destroy()
	require.NoError(t, err)
}
