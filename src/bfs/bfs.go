package main

import (
	"bfs/block"
	"bfs/ns"
	"bytes"
	"flag"
	"fmt"
	"github.com/golang/glog"
	"os"
	"strings"

	"bfs/volume"
)

type Config struct {
	NamespacePath       string
	VolumePaths         ListValue
	AllowInitialization bool
}

type ListValue []string

func (this *ListValue) Set(value string) error {
	*this = append(*this, value)
	return nil
}

func (this *ListValue) String() string {
	return strings.Join(*this, ",")
}

func main() {
	config := &Config{}

	flag.StringVar(&config.NamespacePath, "ns", "", "namespace directory")
	flag.BoolVar(&config.AllowInitialization, "autoinit", false, "allow auto-initialization of physical volumes")
	flag.Var(&config.VolumePaths, "volume", "physical volume directory (repeatable)")

	flag.Parse()

	if len(config.NamespacePath) == 0 {
		glog.Error("--ns is required")
		os.Exit(1)
	}
	if len(config.VolumePaths) == 0 {
		glog.Error("At least one --volume is required")
		os.Exit(1)
	}

	pvs := make([]*volume.PhysicalVolume, 0, len(config.VolumePaths))
	eventChannel := make(chan interface{}, 1024)

	namespace := ns.New(config.NamespacePath)

	go func() {
		for event := range eventChannel {
			switch e := event.(type) {
			case *block.BlockWriteEvent:
				e.ResponseChannel <- e
			}
		}
	}()

	for _, path := range config.VolumePaths {
		pv := volume.NewPhysicalVolume(path, eventChannel)
		pvs = append(pvs, pv)
	}

	for _, pv := range pvs {
		err := pv.Open(config.AllowInitialization)
		if err != nil {
			glog.Errorf("Failed to open volume %s - %s", pv.RootPath, err)
			os.Exit(1)
		}
	}

	fs := &volume.LocalFileSystem{
		Namespace:    namespace,
		Volumes:      []*volume.LogicalVolume{volume.NewLogicalVolume("/", pvs, eventChannel)},
		EventChannel: eventChannel,
	}

	if err := fs.Open(); err != nil {
		glog.Errorf("Unable to open filesystem - %s", err)
		os.Exit(1)
	}

	zeroBuf := bytes.Repeat([]byte{0}, 1024*1024)

	for i := 1; i <= 10; i++ {
		if writer, err := fs.OpenWrite(fmt.Sprintf("/tmp/%d", i), 256*1024); err != nil {
			glog.Fatalf("Unable to open writer - %v", err)
		} else {
			if _, err := writer.Write(zeroBuf); err != nil {
				glog.Errorf("Unable to write to file - %v", err)
			}

			if err := writer.Close(); err != nil {
				glog.Errorf("Unable to close writer for file - %v", err)
			}
		}
	}

	if err := fs.Close(); err != nil {
		glog.Errorf("Unable to close filesystem - %s", err)
		os.Exit(1)
	}

	for _, pv := range pvs {
		err := pv.Close()
		if err != nil {
			glog.Errorf("Failed to close volume %s - %s", pv.RootPath, err)
			os.Exit(1)
		}
	}

	close(eventChannel)

	glog.Flush()
}
