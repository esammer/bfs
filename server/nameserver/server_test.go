package nameserver

import (
	"bfs/config"
	"bfs/service/nameservice"
	"bfs/test"
	"bfs/util/size"
	"context"
	"fmt"
	"github.com/golang/glog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestNameServer(t *testing.T) {
	defer glog.Flush()

	testDir := test.New("build", "test", t.Name())
	require.NoError(t, testDir.Create())
	defer testDir.Destroy()

	rpcPort := 8083
	etcdPortBase := 7008

	nsc := &config.NameServiceConfig{
		Hostname: "localhost",
		Port:     int32(rpcPort),
		Path:     filepath.Join(testDir.Path, "ns"),
		GroupId:  "ns-shard-1",
		Nodes: []*config.NameServiceNodeConfig{
			{Id: "localhost", Hostname: "localhost", BindAddress: "0.0.0.0", ClientPort: int32(etcdPortBase),
				PeerPort: int32(etcdPortBase) + 1},
		},
	}

	bindAddress := fmt.Sprintf("%s:%d", "localhost", rpcPort)
	listener, err := net.Listen("tcp", bindAddress)
	require.NoError(t, err)
	rpcServer := grpc.NewServer()
	defer rpcServer.GracefulStop()

	server := New(nsc, rpcServer)
	require.NoError(t, server.Start())
	defer func() { assert.NoError(t, server.Stop()) }()

	go func() {
		err := rpcServer.Serve(listener)
		assert.NoError(t, err)
	}()

	conn, err := grpc.Dial(bindAddress, grpc.WithInsecure(), grpc.WithBlock())
	require.NoError(t, err)
	defer conn.Close()

	nsClient := nameservice.NewNameServiceClient(conn)

	now := time.Now()
	nowTime := &nameservice.Time{Seconds: now.Unix(), Nanos: int64(now.Nanosecond())}

	_, err = nsClient.Add(
		context.Background(),
		&nameservice.AddRequest{
			Entry: &nameservice.Entry{
				LvId:             "/",
				Path:             "/a.txt",
				BlockSize:        size.MB,
				Size:             4 * size.MB,
				ReplicationLevel: 1,
				Blocks: []*nameservice.BlockMetadata{
					{PvId: "pv1", BlockId: "b1"},
					{PvId: "pv2", BlockId: "b2"},
					{PvId: "pv1", BlockId: "b3"},
					{PvId: "pv2", BlockId: "b4"},
				},
				Ctime: nowTime,
				Mtime: nowTime,
			},
		},
	)
	require.NoError(t, err)
}
