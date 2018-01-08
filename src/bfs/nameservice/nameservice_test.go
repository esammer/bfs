package nameservice

import (
	"bfs/ns"
	"bfs/test"
	"context"
	"fmt"
	"github.com/golang/glog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"net"
	"sync"
	"testing"
)

func TestNameService(t *testing.T) {
	defer glog.Flush()

	testDir := test.New("build", "test", t.Name())
	err := testDir.Create()
	require.NoError(t, err)
	defer testDir.Destroy()

	namespace := ns.New(testDir.Path)
	err = namespace.Open()
	require.NoError(t, err)
	defer namespace.Close()

	service := New(namespace)

	listener, err := net.Listen("tcp", "127.0.0.1:8084")
	require.NoError(t, err)
	server := grpc.NewServer()
	defer server.GracefulStop()

	RegisterNameServiceServer(server, service)

	go func() {
		if err := server.Serve(listener); err != nil {
			glog.Errorf("RPC server failed - %v", err)
		}

		glog.V(1).Infof("RPC server complete")
	}()

	conn, err := grpc.Dial("127.0.0.1:8084", grpc.WithBlock(), grpc.WithInsecure())
	require.NoError(t, err)
	defer conn.Close()

	serviceClient := NewNameServiceClient(conn)

	t.Run("Add", func(t *testing.T) {
		defer glog.Flush()

		wg := sync.WaitGroup{}

		for i := 0; i < 10; i++ {
			wg.Add(1)

			go func(i int) {
				addResp, err := serviceClient.Add(context.Background(), &AddRequest{
					Entry: &Entry{
						LvId:        "1",
						Path:        fmt.Sprintf("/test%d.txt", i),
						Permissions: 0,
						Blocks:      []*BlockMetadata{
							{PvId:"a", BlockId:"1"},
							{PvId:"a", BlockId:"2"},
						},
					},
				})
				assert.NoError(t, err)

				glog.V(2).Infof("Response %d: %v", i, addResp)
				wg.Done()
			}(i)
		}

		wg.Wait()
	})

	t.Run("Get", func(t *testing.T) {
		defer glog.Flush()

		wg := sync.WaitGroup{}

		for i := 0; i < 10; i++ {
			wg.Add(1)

			go func(i int) {
				getResp, err := serviceClient.Get(context.Background(), &GetRequest{
					Path: fmt.Sprintf("/test%d.txt", i),
				})
				assert.NoError(t, err)
				assert.Len(t, getResp.Entry.Blocks, 2)

				glog.V(2).Infof("Response %d: %v", i, getResp)
				wg.Done()
			}(i)
		}

		wg.Wait()
	})

	t.Run("GetMissing", func(t *testing.T) {
		defer glog.Flush()

		getResp, err := serviceClient.Get(context.Background(), &GetRequest{
			Path: "/missing.txt",
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "no such entry")

		glog.V(2).Infof("Response: %v", getResp)
	})

	t.Run("Delete", func(t *testing.T) {
		defer glog.Flush()

		deleteResp, err := serviceClient.Delete(context.Background(), &DeleteRequest{
			Path: "/test1.txt",
		})
		require.NoError(t, err)

		glog.V(2).Infof("Response: %v", deleteResp)
	})

	t.Run("Rename", func(t *testing.T) {
		defer glog.Flush()

		renameResp, err := serviceClient.Rename(context.Background(), &RenameRequest{
			SourcePath:      "/test2.txt",
			DestinationPath: "/test1.txt",
		})
		require.NoError(t, err)
		require.True(t, renameResp.Success)

		glog.V(2).Infof("Response: %v", renameResp)

		_, err = serviceClient.Get(context.Background(), &GetRequest{
			Path: "/test2.txt",
		})
		require.Error(t, err)

		_, err = serviceClient.Get(context.Background(), &GetRequest{
			Path: "/test1.txt",
		})
		require.NoError(t, err)
	})
}
