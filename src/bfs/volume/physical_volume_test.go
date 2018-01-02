package volume

import (
	"bfs/block"
	"bfs/test"
	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestPhysicalVolume_Open(t *testing.T) {
	t.Run("autoInitialize=true", func(t *testing.T) {
		//t.Parallel()

		testDir := test.New("build", "test", t.Name())
		err := testDir.Create()
		require.NoError(t, err)

		eventChannel := make(chan interface{}, 1024)

		pv := NewPhysicalVolume(filepath.Join(testDir.Path, "pv1"), eventChannel)

		err = pv.Open(true)
		require.NoError(t, err, "Open failed for non-existant path - %v", err)

		err = pv.Close()
		require.NoError(t, err, "Failed to close volume - %v", err)

		close(eventChannel)

		err = testDir.Destroy()
		require.NoError(t, err)
	})

	t.Run("autoInitialize=false", func(t *testing.T) {
		//t.Parallel()

		testDir := test.New("build", "test", t.Name())
		err := testDir.Create()
		require.NoError(t, err)

		eventChannel := make(chan interface{}, 1024)

		pv := NewPhysicalVolume(filepath.Join(testDir.Path, "pv1"), eventChannel)

		err = pv.Open(false)
		require.Error(t, err, "Open succeeded for non-existent path")

		// No call to pv.Close() because the volume shouldn't open.

		close(eventChannel)

		err = testDir.Destroy()
		require.NoError(t, err)
	})

	t.Run("volume-is-file", func(t *testing.T) {
		//t.Parallel()

		testDir := test.New("build", "test", "TestPhysicalVolume_Open")
		err := testDir.Create()
		require.NoError(t, err)

		filePath := filepath.Join(testDir.BaseDir, t.Name())

		eventChannel := make(chan interface{}, 1024)

		pv := NewPhysicalVolume(filePath, eventChannel)

		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0600)
		require.NoError(t, err, "Failed to create test file - %v", err)
		err = f.Close()
		require.NoError(t, err)

		err = pv.Open(false)
		require.Error(t, err, "Open succeeded for volume at file")

		// No call to pv.Close() because the volume shouldn't open.

		close(eventChannel)

		err = testDir.Destroy()
		require.NoError(t, err)
	})
}

func TestPhysicalVolume_StateTransitions(t *testing.T) {
	t.Run("new-reader", func(t *testing.T) {
		t.Parallel()

		eventChannel := make(chan interface{}, 1024)

		pv := NewPhysicalVolume(filepath.Join("build", "test", t.Name()), eventChannel)

		_, err := pv.OpenRead("1")
		require.Error(t, err, "Created a reader on unopen volume")
	})

	t.Run("new-writer", func(t *testing.T) {
		t.Parallel()

		eventChannel := make(chan interface{}, 1024)

		pv := NewPhysicalVolume("build/test/"+t.Name(), eventChannel)

		_, err := pv.OpenWrite("1")
		require.Error(t, err, "Created a writer on unopen volume")

		close(eventChannel)
	})
}

func TestPhysicalVolume_Delete(t *testing.T) {
	testDir := test.New("build", "test", t.Name())
	require.NoError(t, testDir.Create())

	eventChannel := make(chan interface{}, 1024)
	pv := NewPhysicalVolume(testDir.Path, eventChannel)

	require.NoError(t, pv.Open(true))

	go func() {
		for event := range eventChannel {
			switch e := event.(type) {
			case *block.BlockWriteEvent:
				e.ResponseChannel <- e
			}
		}
	}()

	writer, err := pv.OpenWrite("1")
	require.NoError(t, err)
	_, err = writer.Write([]byte{0})
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	blockPath := filepath.Join(testDir.Path, "1")

	_, err = os.Stat(blockPath)
	require.NoError(t, err)

	require.NoError(t, pv.Delete("1"))

	_, err = os.Stat(blockPath)
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))

	require.NoError(t, pv.Close())

	close(eventChannel)

	require.NoError(t, testDir.Destroy())
}

func TestPhysicalVolume_ReaderWriter(t *testing.T) {
	eventChannel := make(chan interface{}, 1024)

	go func() {
		for event := range eventChannel {
			glog.Infof("Received event %v", event)

			switch val := event.(type) {
			case *block.BlockWriteEvent:
				val.ResponseChannel <- event
				glog.Infof("Acknowledged block write %v", val)
			}
		}

		glog.Info("Response loop ended")
	}()

	testDir := test.New("build", "test", t.Name())
	err := testDir.Create()
	require.NoError(t, err)

	pv := NewPhysicalVolume(testDir.Path, eventChannel)

	err = pv.Open(true)
	require.NoError(t, err, "Open failed for non-existent path - %v", err)

	writer, err := pv.OpenWrite("1")
	require.NoError(t, err)

	_, err = io.WriteString(writer, "Test 1")
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err, "Failed to close writer - %v", err)

	reader, err := pv.OpenRead("1")
	require.NoError(t, err, "Failed to create reader - %v", err)

	buffer := make([]byte, 16)

	size, err := reader.Read(buffer)
	require.NoError(t, err, "Failed to read from new block - %v", err)

	if string(buffer[:size]) != "Test 1" {
		t.Fatalf("Buffer not as expected: %v", string(buffer[:size]))
	}

	err = reader.Close()
	require.NoError(t, err, "Failed to close reader - %v", err)

	err = pv.Close()
	require.NoError(t, err, "Failed to close volume - %v", err)

	close(eventChannel)

	err = testDir.Destroy()
	require.NoError(t, err)
}
