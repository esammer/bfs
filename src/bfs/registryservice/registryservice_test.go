package registryservice

import (
	"bfs/config"
	"bfs/ns"
	"bfs/test"
	"context"
	"crypto/tls"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/pkg/transport"
	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"net"
	"net/url"
	"path/filepath"
	"testing"
	"time"
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

	etcdConfig := embed.NewConfig()
	etcdConfig.Dir = filepath.Join(testDir.Path, "etcd")
	etcdConfig.LCUrls = []url.URL{{Host: "localhost:2379", Scheme: "https"}}
	etcdConfig.ClientTLSInfo = transport.TLSInfo{}
	etcdConfig.ClientAutoTLS = true

	etcd, err := embed.StartEtcd(etcdConfig)
	require.NoError(t, err)
	defer etcd.Close()
	<-etcd.Server.ReadyNotify()

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:        []string{"https://localhost:2379"},
		RejectOldCluster: true,
		TLS:              &tls.Config{InsecureSkipVerify: true},
	})
	require.NoError(t, err)

	service := New(etcdClient)

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

	etcdClient.Put(context.Background(), "/bfs/volumes/1", `id: "1", pvIds: [ "1","2","3" ], labels: [{key: "mount", value: "/a"}]`)
	etcdClient.Put(context.Background(), "/bfs/volumes/2", `id: "2", pvIds: [ "4", "5" ], labels: [{key: "mount", value: "/b"}]`)

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

	t.Run("LogicalVolumeInfo", func(t *testing.T) {
		defer glog.Flush()

		lv1 := &config.LogicalVolumeConfig{
			Id:    "1",
			PvIds: []string{"1", "2", "3"},
			Labels: []*config.Label{
				{Key: "mount", Value: "/a"},
			},
		}
		lv2 := &config.LogicalVolumeConfig{
			Id:    "2",
			PvIds: []string{"4", "5"},
			Labels: []*config.Label{
				{Key: "mount", Value: "/b"},
			},
		}

		for _, lv := range []*config.LogicalVolumeConfig{lv1, lv2} {
			t.Run("lv="+lv.Id, func(t *testing.T) {
				var resp *LogicalVolumeInfoResponse
				var err error

				testWithRetry(t, 10, 500*time.Millisecond, func(t *testing.T, attempt int) bool {
					if resp, err = serviceClient.LogicalVolumeInfo(context.Background(),
						&LogicalVolumeInfoRequest{Id: lv.Id}); err != nil {
						glog.V(2).Infof("Attempt %d received error %v", attempt, err)
					} else {
						return true
					}

					return false
				})

				require.NoError(t, err)
				require.Equal(t, lv, resp.Config)
			})
		}
	})
}

func testWithRetry(t *testing.T, attempts int, sleepDuration time.Duration, testFunc func(t *testing.T, attempt int) bool) {
	for i := 0; i < attempts; i++ {
		glog.V(2).Infof("Attempt %d of %v", i, testFunc)
		if testFunc(t, i) {
			break
		} else {
			time.Sleep(time.Duration(sleepDuration))
		}
	}
}
