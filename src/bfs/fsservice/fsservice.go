package fsservice

import (
	"bfs/nameservice"
	"fmt"
	"github.com/golang/glog"
	"io"
)

type FsService struct {
	FsServiceServer
}

func (this *FsService) Create(stream FsService_CreateServer) error {
	var entry *nameservice.Entry

	for i := 0; ; i++ {
		request, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if i == 0 {
			metadata, ok := request.Request.(*CreateRequest_Metadata)
			if !ok {
				return fmt.Errorf("first request of create is not metadata")
			}

			entry = &nameservice.Entry{
				Path:        metadata.Metadata.Path,
				Permissions: metadata.Metadata.Permissions,
			}

			glog.Infof("Metadata: %#v", metadata)
		} else {
			packet, ok := request.Request.(*CreateRequest_Packet)

			if !ok {
				return fmt.Errorf("request %d is not a data packet", i)
			}

			glog.Infof("Packet size: %d", len(packet.Packet.Buffer))
		}
	}

	if entry != nil {
		return stream.SendAndClose(&CreateResponse{})
	}

	return nil
}
