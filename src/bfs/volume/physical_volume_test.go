package volume

import (
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

		pv.Close()

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

		pv.Close()

		if err := os.RemoveAll("build/test/" + t.Name()); err != nil {
			t.Fatalf("Failed to remove temp directory - %v", err)
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
