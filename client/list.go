package client

import (
	"bfs/nameservice"
	"bfs/util/logging"
	"context"
	"github.com/golang/glog"
	"io"
)

type ListEntry struct {
	Entry *nameservice.Entry
	Err   error
}

func (this *Client) List(startKey string, endKey string) <-chan *ListEntry {
	resultChan := make(chan *ListEntry, 1024)

	go func() {
		err := this.VisitNameShards(func(name string, conn nameservice.NameServiceClient) (bool, error) {
			listStream, err := conn.List(context.Background(), &nameservice.ListRequest{StartKey: startKey, EndKey: endKey})
			if err != nil {
				glog.V(logging.LogLevelTrace).Infof("Closing list stream due to %v", err)
				close(resultChan)
				return false, err
			}

			for {
				resp, err := listStream.Recv()
				if err == io.EOF {
					glog.V(logging.LogLevelTrace).Infof("Finished list receive chunk")
					break
				} else if err != nil {
					glog.V(logging.LogLevelTrace).Infof("Closing list stream due to %v", err)
					close(resultChan)
					return false, err
				}

				for _, entry := range resp.Entries {
					resultChan <- &ListEntry{Entry: entry}
				}
			}

			return true, nil
		})

		if err != nil {
			resultChan <- &ListEntry{Err: err}
		}

		close(resultChan)
		glog.V(logging.LogLevelTrace).Infof("List stream complete")
	}()

	return resultChan
}
