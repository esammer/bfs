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
			t.Error("Open failed for non-existant path - %v", err)
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
			t.Error("Open succeeded for non-existant path")
		}

		// No call to pv.Close() because the volume shouldn't open.

		if err := os.RemoveAll("build/test/" + t.Name()); err != nil {
			t.Fatalf("Failed to remove temp directory - %v", err)
		}
	})

	t.Run("volume-is-file", func(t *testing.T) {
		t.Parallel()

		if f, err := os.OpenFile("build/test/" + t.Name(), os.O_CREATE | os.O_WRONLY, 0600); err != nil {
			t.Fatalf("Failed to create test file - %v", err)
			defer f.Close()
		}

		pv := NewPhysicalVolume("build/test/" + t.Name())

		if err := pv.Open(false); err == nil {
			t.Error("Open succeeded for volume at file")
		} else {
			t.Logf("Properly got %v", err)
		}

		// No call to pv.Close() because the volume shouldn't open.

		if err := os.RemoveAll("build/test/" + t.Name()); err != nil {
			t.Fatalf("Failed to remove test file - %v", err)
		}
	})
}

func TestPhysicalVolume_StateTransitions(t *testing.T) {
	t.Run("new-reader", func(t *testing.T) {
		t.Parallel()

		pv := NewPhysicalVolume("build/test/" + t.Name())

		if _, err := pv.ReaderFor("1"); err == nil {
			t.Errorf("Created a reader on unopen volume")
		} else {
			t.Logf("Properly got %v", err)
		}
	})

	t.Run("new-writer", func(t *testing.T) {
		t.Parallel()

		pv := NewPhysicalVolume("build/test/" + t.Name())

		if _, err := pv.WriterFor("1"); err == nil {
			t.Errorf("Created a writer on unopen volume")
		} else {
			t.Logf("Properly got %v", err)
		}
	})
}
