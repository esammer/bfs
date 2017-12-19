package main

import (
	"fmt"
	"io"
	"log"

	"bfs/volume"
)

func main() {
	pv := volume.NewPhysicalVolume("tmp/1")

	if err := pv.Open(true); err != nil {
		log.Fatalf("Unable to open volume - %v", err)
	}

	for i := 1; i <= 3; i++ {
		if writer, err := pv.WriterFor(fmt.Sprintf("%d", i)); err != nil {
			log.Fatalf("Unable to open block for write - %v", err)
		} else {
			io.WriteString(writer, fmt.Sprintf("Hello %v", i))
			writer.Close()

			buffer := make([]byte, 16)

			if reader, err := pv.ReaderFor(fmt.Sprintf("%d", i)); err == nil {
				if size, err := reader.Read(buffer); err == nil {
					log.Printf("Read %v bytes", size)

					if fmt.Sprintf("Hello %v", i) != string(buffer[:size]) {
						log.Fatalf("Strings are not equals")
					}
				} else {
					log.Fatalf("Unable to read from block - %v", err)
				}

				reader.Close()
			}
		}
	}

	if err := pv.Close(); err != nil {
		log.Fatalf("Unable to close volume - %v", err)
	}
}
