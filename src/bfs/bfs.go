package main

import (
	"flag"
	"github.com/golang/glog"
	"io"

	"bfs/volume"
)

func main() {
	flag.Parse()

	pv := volume.NewPhysicalVolume("tmp/1")

	if err := pv.Open(true); err != nil {
		glog.Fatalf("Unable to open volume - %v", err)
	}

	lv := volume.NewLogicalVolume("/", []*volume.PhysicalVolume{pv})

	lv.Open()

	for i := 1; i <= 3; i++ {
		if writer, err := lv.WriterFor(nil, "/hello.txt", 8); err != nil {
			glog.Fatalf("Unable to open writer - %v", err)
		} else {
			io.WriteString(writer, "Hello world. How do you do?")
			writer.Close()
		}
	}

	lv.Close()

	if err := pv.Close(); err != nil {
		glog.Fatalf("Unable to close volume - %v", err)
	}

	glog.Flush()
}
