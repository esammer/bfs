package client

import (
	"bfs/test"
	"context"
	"errors"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestWatcher(t *testing.T) {
	defer glog.Flush()

	testDir := test.New("build", "test", t.Name())
	require.NoError(t, testDir.Create())
	defer testDir.Destroy()

	etcdConfig := embed.NewConfig()
	etcdConfig.Dir = filepath.Join(testDir.Path, "etcd")

	var (
		etcd           *embed.Etcd
		err            error
		etcdClientPort int
	)
	for i := 13000; i < 14000; i++ {
		etcdConfig.LCUrls = []url.URL{{Host: fmt.Sprintf("localhost:%d", i), Scheme: "http"}}
		etcdConfig.ACUrls = []url.URL{{Host: fmt.Sprintf("localhost:%d", i), Scheme: "http"}}
		etcdConfig.LPUrls = []url.URL{{Host: fmt.Sprintf("localhost:%d", i+1), Scheme: "http"}}
		etcdConfig.APUrls = []url.URL{{Host: fmt.Sprintf("localhost:%d", i+1), Scheme: "http"}}
		// Apparently this is required for Reasons(tm).
		etcdConfig.InitialCluster = etcdConfig.InitialClusterFromName("")

		etcd, err = embed.StartEtcd(etcdConfig)
		if err == nil || !strings.Contains(err.Error(), "bind: address already in use") {
			etcdClientPort = i
			break
		}
	}
	require.NoError(t, err)
	defer etcd.Close()
	<-etcd.Server.ReadyNotify()

	client, err := clientv3.New(clientv3.Config{Endpoints: []string{fmt.Sprintf("http://localhost:%d", etcdClientPort)}})
	require.NoError(t, err)
	defer client.Close()

	wg := sync.WaitGroup{}
	wg.Add(2)

	var deleteCalls int32
	var putCalls int32
	var doneCalls int32

	watcher := NewWatcher(
		client,
		"/test-key",
		true,
		func(kv *mvccpb.KeyValue) error {
			glog.V(2).Infof("Put key: %s = %s", string(kv.Key), string(kv.Value))
			atomic.AddInt32(&putCalls, 1)
			return nil
		},
		func(kv *mvccpb.KeyValue) error {
			glog.V(2).Infof("Delete key: %s", string(kv.Key))
			wg.Done()
			if atomic.AddInt32(&deleteCalls, 1) > 1 {
				return errors.New("boom")
			}
			return nil
		},
		func(err error) {
			glog.V(2).Infof("Complete with error: %v", err)
			atomic.AddInt32(&doneCalls, 1)
		},
		true,
		clientv3.WithPrefix(),
	)

	_, err = client.Put(context.Background(), "/test-key/k1", "v1")
	require.NoError(t, err)
	_, err = client.Put(context.Background(), "/test-key/k2", "v1")
	require.NoError(t, err)

	require.NoError(t, watcher.Start())

	_, err = client.Put(context.Background(), "/test-key/k2", "v2")
	require.NoError(t, err)
	_, err = client.Put(context.Background(), "/test-key/k2", "v3")
	require.NoError(t, err)
	_, err = client.Delete(context.Background(), "/test-key/k1")
	require.NoError(t, err)
	_, err = client.Delete(context.Background(), "/test-key/k2")
	require.NoError(t, err)

	wg.Wait()

	require.Equal(t, int32(4), atomic.LoadInt32(&putCalls))
	require.Equal(t, int32(2), atomic.LoadInt32(&deleteCalls))
	// Exact testing is racy: it's possible for the system to retry before we get here.
	require.True(t, atomic.LoadInt32(&doneCalls) >= 1)

	watcher.Stop()
}
