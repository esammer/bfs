package volume

import (
	"fmt"
	"io"
	"os"
	"testing"
)

func TestLogicalVolume_OpenClose(t *testing.T) {
	lv := NewLogicalVolume("/", nil)

	if err := lv.Open(); err != nil {
		t.Fatalf("Failed to open volume - %v", err)
	}

	if err := lv.Close(); err != nil {
		t.Fatalf("Failed to close volume - %v", err)
	}
}

func TestLogicalVolume_ReaderWriter(t *testing.T) {
	if err := os.RemoveAll("build/test/" + t.Name()); err != nil {
		t.Fatalf("Failed to remove test directory - %v", err)
	}

	pv := NewPhysicalVolume("build/test/" + t.Name())

	if err := pv.Open(true); err != nil {
		t.Fatalf("Failed to open physical volume - %v", err)
	}

	lv := NewLogicalVolume("/", []*PhysicalVolume{pv})

	if err := lv.Open(); err != nil {
		t.Fatalf("Failed to open volume - %v", err)
	}

	if err := os.MkdirAll("build/test/"+t.Name(), 0700); err != nil {
		t.Fatalf("Failed to create test directory - %v", err)
	}

	if writer, err := lv.WriterFor(nil, "test1.txt", 1024*1024); err == nil {
		for i := 0; i < 1000000; i++ {
			if _, err := io.WriteString(writer, fmt.Sprintf("Test %d\n", i)); err != nil {
				t.Fatalf("Failed to write to writer - %v", err)
			}
		}

		if err := writer.Close(); err != nil {
			t.Fatalf("Failed to close writer - %v", err)
		}
	} else {
		t.Fatalf("Unable to open writer - %v", err)
	}

	if err := lv.Close(); err != nil {
		t.Fatalf("Failed to close volume - %v", err)
	}

	if err := pv.Close(); err != nil {
		t.Fatalf("Failed to close physical volume - %v", err)
	}

	/*
		if err := os.RemoveAll("build/test/" + t.Name()); err != nil {
			t.Fatalf("Failed to remove test directory - %v", err)
		}
	*/
}
