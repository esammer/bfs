package volume

import (
	"io"
	"os"
	"testing"
)

func TestPhysicalVolume_Open(t *testing.T) {
	t.Run("autoInitialize=true", func(t *testing.T) {
		t.Parallel()

		if err := os.RemoveAll("build/test/" + t.Name()); err != nil {
			t.Fatalf("Failed to remove temp directory - %v", err)
		}

		pv := NewPhysicalVolume("build/test/" + t.Name())

		if err := pv.Open(true); err != nil {
			t.Fatal("Open failed for non-existant path - %v", err)
		}

		if err := pv.Close(); err != nil {
			t.Fatalf("Failed to close volume - %v", err)
		}

		if err := os.RemoveAll("build/test/" + t.Name()); err != nil {
			t.Fatalf("Failed to remove temp directory - %v", err)
		}
	})

	t.Run("autoInitialize=false", func(t *testing.T) {
		t.Parallel()

		if err := os.RemoveAll("build/test/" + t.Name()); err != nil {
			t.Fatalf("Failed to remove temp directory - %v", err)
		}

		pv := NewPhysicalVolume("build/test/" + t.Name())

		if err := pv.Open(false); err == nil {
			t.Fatal("Open succeeded for non-existant path")
		}

		// No call to pv.Close() because the volume shouldn't open.

		if err := os.RemoveAll("build/test/" + t.Name()); err != nil {
			t.Fatalf("Failed to remove temp directory - %v", err)
		}
	})

	t.Run("volume-is-file", func(t *testing.T) {
		t.Parallel()

		if err := os.MkdirAll("build/test/TestPhysicalVolume_Open", 0700); err != nil {
			t.Fatalf("Unable to create test directory - %v", err)
		}

		if f, err := os.OpenFile("build/test/"+t.Name(), os.O_CREATE|os.O_WRONLY, 0600); err != nil {
			t.Fatalf("Failed to create test file - %v", err)
		} else {
			f.Close()
		}

		pv := NewPhysicalVolume("build/test/" + t.Name())

		if err := pv.Open(false); err == nil {
			t.Fatal("Open succeeded for volume at file")
		}

		// No call to pv.Close() because the volume shouldn't open.

		if err := os.RemoveAll("build/test/TestPhysicalVolume_Open" + t.Name()); err != nil {
			t.Fatalf("Failed to remove test file - %v", err)
		}
	})
}

func TestPhysicalVolume_StateTransitions(t *testing.T) {
	t.Run("new-reader", func(t *testing.T) {
		t.Parallel()

		pv := NewPhysicalVolume("build/test/" + t.Name())

		if _, err := pv.ReaderFor("1"); err == nil {
			t.Fatalf("Created a reader on unopen volume")
		}
	})

	t.Run("new-writer", func(t *testing.T) {
		t.Parallel()

		pv := NewPhysicalVolume("build/test/" + t.Name())

		if _, err := pv.WriterFor("1"); err == nil {
			t.Fatalf("Created a writer on unopen volume")
		}
	})
}

func TestPhysicalVolume_ReaderWriter(t *testing.T) {
	pv := NewPhysicalVolume("build/test/" + t.Name())

	if err := pv.Open(true); err != nil {
		t.Fatal("Open failed for non-existant path - %v", err)
	}

	if writer, err := pv.WriterFor("1"); err == nil {
		if _, err := io.WriteString(writer, "Test 1"); err != nil {
			t.Fatalf("Faled to write to new block - %v", err)
		}

		if err := writer.Close(); err != nil {
			t.Fatalf("Failed to close writer - %v", err)
		}
	} else {
		t.Fatalf("Failed to create writer - %v", err)
	}

	if reader, err := pv.ReaderFor("1"); err == nil {
		buffer := make([]byte, 16)

		if size, err := reader.Read(buffer); err == nil {
			if string(buffer[:size]) != "Test 1" {
				t.Fatalf("Buffer not as expected: %v", string(buffer[:size]))
			}
		} else {
			t.Fatalf("Failed to read from new block - %v", err)
		}

		if err := reader.Close(); err != nil {
			t.Fatalf("Failed to close reader - %v", err)
		}
	} else {
		t.Fatalf("Failed to create reader - %v", err)
	}

	if err := pv.Close(); err != nil {
		t.Fatalf("Failed to close volume - %v", err)
	}

	if err := os.RemoveAll("build/test/" + t.Name()); err != nil {
		t.Fatalf("Failed to remove test directory - %v", err)
	}
}
