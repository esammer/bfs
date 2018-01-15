package registryservice

import (
	"bfs/config"
	"bfs/ns"
	"bfs/test"
	"context"
	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"net"
	"testing"
)

func TestRegistryService(t *testing.T) {
	defer glog.Flush()

	testDir := test.New("build", "test", t.Name())
	err := testDir.Create()
	require.NoError(t, err)
	defer testDir.Destroy()

	namespace := ns.New(testDir.Path)
	err = namespace.Open()
	require.NoError(t, err)
	defer namespace.Close()

	service := New()

	listener, err := net.Listen("tcp", "127.0.0.1:8084")
	require.NoError(t, err)
	server := grpc.NewServer()
	defer server.GracefulStop()

	RegisterRegistryServiceServer(server, service)

	go func() {
		if err := server.Serve(listener); err != nil {
			glog.Errorf("RPC server failed - %v", err)
		}

		glog.V(1).Infof("RPC server complete")
	}()

	conn, err := grpc.Dial("127.0.0.1:8084", grpc.WithBlock(), grpc.WithInsecure())
	require.NoError(t, err)
	defer conn.Close()

	serviceClient := NewRegistryServiceClient(conn)

	volumeStats := []*PhysicalVolumeStatus{
		{Id: "1", Path: "pv/1", FileSystemStatus: &FileSystemStatus{}},
		{Id: "2", Path: "pv/2", FileSystemStatus: &FileSystemStatus{}},
		{Id: "3", Path: "pv/3", FileSystemStatus: &FileSystemStatus{}},
	}

	t.Run("HostReportAndHosts", func(t *testing.T) {
		defer glog.Flush()

		serviceClient.RegisterHost(context.Background(), &RegisterHostRequest{
			HostConfig: &config.HostConfig{
				Id:       "1",
				Hostname: "hostname",
				Labels: []*config.Label{
					{Key: "env", Value: "production"},
					{Key: "os", Value: "linux"},
				},
			},
		})
		_, err := serviceClient.HostStatus(context.Background(), &HostStatusRequest{
			Id:          "1",
			VolumeStats: volumeStats,
		})
		require.NoError(t, err)

		hostResp, err := serviceClient.Hosts(context.Background(), &HostsRequest{Selector: "env = production"})
		require.NoError(t, err)
		require.Len(t, hostResp.Hosts, 1)
		require.True(t, hostResp.Hosts[0].FirstSeen > 0)
		require.True(t, hostResp.Hosts[0].LastSeen > 0)

		// We can't control the timestamps injected so we zero them out for comparison.
		hostResp.Hosts[0].FirstSeen = 0
		hostResp.Hosts[0].LastSeen = 0

		require.Contains(t, hostResp.Hosts, &HostStatus{
			Id:          "1",
			VolumeStats: volumeStats,
		})
	})
}
