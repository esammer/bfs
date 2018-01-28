package client

import (
	"bfs/blockservice"
	"bfs/config"
	"bfs/file"
	"bfs/lru"
	"bfs/nameservice"
	"bfs/util"
	"bfs/util/etcd"
	"bfs/util/logging"
	"bfs/util/size"
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"io"
	"path/filepath"
	"stathat.com/c/consistent"
	"strings"
	"time"
	"unsafe"
)

type Client struct {
	etcdClient *clientv3.Client
	clientLRU  *lru.LRUCache
	hash       *consistent.Consistent

	clusterState *ClusterState

	volumeWatcher *etcd.Watcher
	hostWatcher   *etcd.Watcher
}

func New(endpoints []string) (*Client, error) {
	etcdConfig := clientv3.Config{}
	etcdConfig.Endpoints = endpoints

	etcdClient, err := clientv3.New(etcdConfig)
	if err != nil {
		return nil, err
	}

	return NewWithEtcd(etcdClient)
}

func NewWithEtcd(etcdClient *clientv3.Client) (*Client, error) {
	client := &Client{
		etcdClient:   etcdClient,
		clusterState: NewClusterState(),
		hash:         consistent.New(),
	}

	client.hash.NumberOfReplicas = 10

	// FIXME: Extract these deserializers into top level private functions.
	volumeConfigDeser := func(kv *mvccpb.KeyValue) *config.LogicalVolumeConfig {
		lvConfig := &config.LogicalVolumeConfig{}

		if err := proto.UnmarshalText(string(kv.Value), lvConfig); err != nil {
			glog.Warningf("Unable to deserialize host config from %s - %v", string(kv.Key), err)
		} else {
			if _, ok := lvConfig.Labels["mount"]; !ok {
				glog.Warningf("Volume %s has no mount label", lvConfig.Id)
			} else {
				return lvConfig
			}
		}

		return nil
	}
	hostConfigDeser := func(kv *mvccpb.KeyValue) *config.HostConfig {
		hostConfig := &config.HostConfig{}

		if err := proto.UnmarshalText(string(kv.Value), hostConfig); err != nil {
			glog.Warningf("Unable to deserialize host config from %s - %v", string(kv.Key), err)
			return nil
		}

		return hostConfig
	}
	hostStatusDeser := func(kv *mvccpb.KeyValue) *config.HostStatus {
		hostStatus := &config.HostStatus{}

		if err := proto.UnmarshalText(string(kv.Value), hostStatus); err != nil {
			glog.Warningf("Unable to deserialize host status from %s - %v", string(kv.Key), err)
			return nil
		}

		return hostStatus
	}

	client.volumeWatcher = etcd.NewWatcher(
		etcdClient,
		filepath.Join(DefaultEtcdPrefix, EtcdVolumesPrefix),
		true,
		func(kv *mvccpb.KeyValue) error {
			glog.V(logging.LogLevelTrace).Infof("Found volume: %s", string(kv.Key))

			lvConfig := volumeConfigDeser(kv)

			if lvConfig != nil {
				if _, ok := lvConfig.Labels["mount"]; ok {
					client.clusterState.AddLogicalVolumeConfig(lvConfig)
				}
			}

			return nil
		},
		func(kv *mvccpb.KeyValue) error {
			lvConfig := volumeConfigDeser(kv)

			client.clusterState.RemoveLogicalVolumeConfig(lvConfig.Id)

			return nil
		},
		nil,
		true,
		clientv3.WithPrefix(),
	)

	client.hostWatcher = etcd.NewWatcher(
		etcdClient,
		filepath.Join(DefaultEtcdPrefix, EtcdHostsPrefix),
		true,
		func(kv *mvccpb.KeyValue) error {
			glog.V(logging.LogLevelTrace).Infof("Found host %s", string(kv.Key))

			pathComponents := strings.Split(string(kv.Key), string(filepath.Separator))
			if len(pathComponents) < 4 {
				glog.Warningf("Host key entry %s (components: %v) is of the wrong format - skipping", string(kv.Key), pathComponents)
				return nil
			}

			entryType := pathComponents[len(pathComponents)-2]

			if entryType == "config" {
				hostConfig := hostConfigDeser(kv)

				if hostConfig == nil {
					glog.Warningf("Unable to deserialize host config from %s", string(kv.Key))
					return nil
				} else {
					client.clusterState.AddHostConfig(hostConfig)
				}
			} else if entryType == "status" {
				status := hostStatusDeser(kv)

				if status == nil {
					glog.Warningf("Unable to deserialize host status from %s", string(kv.Key))
					return nil
				} else {
					client.clusterState.AddHostStatus(status)
					client.hash.Add(status.Id)
				}
			}

			return nil
		},
		func(kv *mvccpb.KeyValue) error {
			pathComponents := strings.Split(string(kv.Key), string(filepath.Separator))
			if len(pathComponents) < 4 {
				glog.Warningf("Host key entry %s (components: %v) is of the wrong format - skipping", string(kv.Key), pathComponents)
				return nil
			}

			entryType := pathComponents[len(pathComponents)-2]

			if entryType == "config" {
				hostConfig := hostConfigDeser(kv)

				if hostConfig != nil {
					glog.Warningf("Unable to deserialize host config from %s", string(kv.Key))
					return nil
				}

				client.clusterState.RemoveHostConfig(hostConfig.Id)
			} else if entryType == "status" {
				status := hostStatusDeser(kv)

				client.hash.Remove(status.Id)
				client.clusterState.RemoveHostStatus(status.Id)
			}

			return nil
		},
		nil,
		true,
		clientv3.WithPrefix(),
	)

	if err := client.volumeWatcher.Start(); err != nil {
		return nil, err
	}

	if err := client.hostWatcher.Start(); err != nil {
		return nil, err
	}

	client.clientLRU = lru.NewCache(
		2,
		func(name string) (interface{}, error) {
			glog.V(logging.LogLevelTrace).Infof("Creating new connection for %s", name)

			ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
			conn, err := grpc.DialContext(ctx, name, grpc.WithBlock(), grpc.WithInsecure())
			if err != nil {
				return nil, err
			}

			c := &util.ServiceCtx{
				Conn:               conn,
				NameServiceClient:  nameservice.NewNameServiceClient(conn),
				BlockServiceClient: blockservice.NewBlockServiceClient(conn),
			}

			return c, nil
		},
		func(name string, value interface{}) error {
			glog.V(logging.LogLevelTrace).Infof("Destroying connection for %s", name)

			return value.(*util.ServiceCtx).Conn.Close()
		},
	)

	return client, nil
}

func (this *Client) Hosts() []*config.HostConfig {
	return this.clusterState.HostConfigs()
}

func (this *Client) HostStatus() []*config.HostStatus {
	return this.clusterState.HostStatus()
}

func (this *Client) Create(path string, blockSize int) (file.Writer, error) {
	var pvConfigs []*config.PhysicalVolumeConfig

	for _, lvConfig := range this.clusterState.LogicalVolumeConfigs() {
		mount := lvConfig.Labels["mount"]

		glog.V(logging.LogLevelTrace).Infof("Checking volume mount %s for file %s", mount, path)
		if strings.HasPrefix(path, mount) {
			pvConfigs = this.clusterState.PhysicalVolumesForLogicalVolume(lvConfig.Id)
			break
		}
	}

	if len(pvConfigs) == 0 {
		return nil, fmt.Errorf("unable to find volume for file %s", path)
	}

	conn, _, err := this.connectionForPath(path)
	if err != nil {
		return nil, err
	}

	placementPolicy := file.NewLabelAwarePlacementPolicy(
		pvConfigs,
		"hostname",
		false,
		1,
		1,
		this.blockAcceptFunc,
	)

	return file.NewWriter(conn.NameServiceClient, this.clientLRU, placementPolicy, path, blockSize)
}

func (this *Client) Open(path string) (file.Reader, error) {
	conn, _, err := this.connectionForPath(path)
	if err != nil {
		return nil, err
	}

	reader := file.NewReader(conn.NameServiceClient, conn.BlockServiceClient, path)
	return reader, reader.Open()
}

func (this *Client) Stat(path string) (*nameservice.Entry, error) {
	conn, _, err := this.connectionForPath(path)

	getResp, err := conn.NameServiceClient.Get(context.Background(), &nameservice.GetRequest{Path: path})
	if err != nil {
		return nil, err
	}

	return getResp.Entry, nil
}

func (this *Client) Remove(path string, recursive bool) (uint32, error) {
	if recursive {
		var deletedTotal uint32 = 0

		// Recursive deletes must be issued to all shards.
		this.VisitNameShards(
			func(name string, conn nameservice.NameServiceClient) (bool, error) {
				resp, err := conn.Delete(
					context.Background(),
					&nameservice.DeleteRequest{Path: path, Recursive: recursive},
				)
				if err != nil {
					glog.Warningf("Failed to issue delete on shard %s - %v", name, err)
				}

				deletedTotal += resp.EntriesDeleted
				return true, nil
			},
		)

		return deletedTotal, nil
	} else {
		conn, _, err := this.connectionForPath(path)
		if err != nil {
			return 0, err
		}
		resp, err := conn.NameServiceClient.Delete(
			context.Background(),
			&nameservice.DeleteRequest{Path: path, Recursive: recursive},
		)
		if err != nil {
			return resp.EntriesDeleted, err
		}

		return resp.EntriesDeleted, nil
	}
}

func (this *Client) Rename(sourcePath string, destinationPath string) error {
	sourceConn, sourceHostId, err := this.connectionForPath(sourcePath)
	if err != nil {
		return err
	}

	destConn, destHostId, err := this.connectionForPath(destinationPath)
	if err != nil {
		return err
	}

	if sourceHostId != destHostId {
		// Rename requires relocation.
		getResp, err := sourceConn.NameServiceClient.Get(context.Background(), &nameservice.GetRequest{Path: sourcePath})
		if err != nil {
			return err
		}

		_, err = destConn.NameServiceClient.Add(context.Background(), &nameservice.AddRequest{
			Entry: getResp.Entry,
		})
		if err != nil {
			return err
		}

		_, err = sourceConn.NameServiceClient.Delete(context.Background(), &nameservice.DeleteRequest{Path: sourcePath})
		if err != nil {
			return err
		}
	} else {
		// Rename is on the same host.
		_, err := sourceConn.NameServiceClient.Rename(
			context.Background(),
			&nameservice.RenameRequest{
				SourcePath:      sourcePath,
				DestinationPath: destinationPath,
			},
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (this *Client) List(startKey string, endKey string) <-chan *nameservice.Entry {
	iterChan := make(chan *nameservice.Entry, 1024)

	go func() {
		for _, hostConfig := range this.clusterState.HostConfigs() {
			glog.V(logging.LogLevelTrace).Infof("List on %s", hostConfig.Hostname)

			connectionAddress := fmt.Sprintf("%s:%d", hostConfig.NameServiceConfig.Hostname, hostConfig.NameServiceConfig.Port)

			o, err := this.clientLRU.Get(connectionAddress)
			if err != nil {
				close(iterChan)
				return
			}
			conn := o.(*util.ServiceCtx)

			listStream, err := conn.NameServiceClient.List(context.Background(), &nameservice.ListRequest{StartKey: startKey, EndKey: endKey})
			if err != nil {
				glog.V(logging.LogLevelTrace).Infof("Closing list stream due to %v", err)
				close(iterChan)
				return
			}

			for {
				resp, err := listStream.Recv()
				if err == io.EOF {
					glog.V(logging.LogLevelTrace).Infof("Finished list receive chunk")
					break
				} else if err != nil {
					glog.V(logging.LogLevelTrace).Infof("Closing list stream due to %v", err)
					close(iterChan)
					break
				}

				for _, entry := range resp.Entries {
					iterChan <- entry
				}
			}
		}

		close(iterChan)
		glog.V(logging.LogLevelTrace).Infof("List stream complete")
	}()

	return iterChan
}

func (this *Client) CreateLogicalVolume(volumeConfig *config.LogicalVolumeConfig) error {
	_, err := this.etcdClient.Put(
		context.Background(),
		filepath.Join(DefaultEtcdPrefix, EtcdVolumesPrefix, volumeConfig.Id),
		proto.MarshalTextString(volumeConfig),
	)
	if err != nil {
		return err
	}

	return nil
}

func (this *Client) DeleteLogicalVolume(volumeId string) (bool, error) {
	resp, err := this.etcdClient.Delete(
		context.Background(),
		filepath.Join(DefaultEtcdPrefix, EtcdVolumesPrefix, volumeId),
	)

	return resp.Deleted == 1, err
}

func (this *Client) ListVolumes() ([]*config.LogicalVolumeConfig, error) {
	getResp, err := this.etcdClient.Get(
		context.Background(),
		filepath.Join(DefaultEtcdPrefix, EtcdVolumesPrefix),
		clientv3.WithPrefix(),
	)
	if err != nil {
		return nil, err
	}

	lvConfigs := make([]*config.LogicalVolumeConfig, len(getResp.Kvs))

	for i, kv := range getResp.Kvs {
		lvConfig := &config.LogicalVolumeConfig{}
		if err := proto.UnmarshalText(string(kv.Value), lvConfig); err != nil {
			return nil, err
		}

		lvConfigs[i] = lvConfig
	}

	return lvConfigs, nil
}

func (this *Client) Stats() uintptr {
	var byteSize uintptr = 0
	byteSize += unsafe.Sizeof(config.HostConfig{}) * uintptr(len(this.clusterState.HostConfigs()))
	byteSize += unsafe.Sizeof(config.LogicalVolumeConfig{}) * uintptr(len(this.clusterState.LogicalVolumeConfigs()))
	return byteSize
}

func (this *Client) Close() error {
	if this.clientLRU != nil {
		this.clientLRU.Purge()
	}

	if this.volumeWatcher != nil {
		this.volumeWatcher.Stop()
	}

	if this.hostWatcher != nil {
		this.hostWatcher.Stop()
	}

	if this.etcdClient != nil {
		if err := this.etcdClient.Close(); err != nil {
			return err
		}
	}

	return nil
}

// A name shard visitor callback.
//
// This visitor is invoked for each name service in the configured cluster. Visitors can return true if they wish to
// continue being called for more shards or false to terminate early. If the visitor returns an error, no additional
// shards will be visited; the bool argument is ignored when an error is present.
type ShardVisitor func(name string, conn nameservice.NameServiceClient) (bool, error)

// Visit each name shard with the given visitor function.
//
// See ShardVisitor for more information.
func (this *Client) VisitNameShards(visitor ShardVisitor) error {
	for _, hostConfig := range this.clusterState.HostConfigs() {
		glog.V(logging.LogLevelTrace).Infof("Visit shard %s with %v", hostConfig.Hostname, visitor)

		connectionAddress := fmt.Sprintf("%s:%d", hostConfig.NameServiceConfig.Hostname, hostConfig.NameServiceConfig.Port)

		o, err := this.clientLRU.Get(connectionAddress)
		if err != nil {
			return err
		}

		conn := o.(*util.ServiceCtx)

		keepGoing, err := visitor(hostConfig.Hostname, conn.NameServiceClient)
		if !keepGoing || err != nil {
			glog.V(logging.LogLevelTrace).Infof("Visit terminating early - continue: %t err: %v", keepGoing, err)
			return err
		}
	}

	return nil
}

func (this *Client) connectionForPath(path string) (*util.ServiceCtx, string, error) {
	hostId, err := this.hash.Get(path)
	if err != nil {
		return nil, "", err
	}

	nsc := this.clusterState.HostConfig(hostId).NameServiceConfig
	connectionAddress := fmt.Sprintf("%s:%d", nsc.Hostname, nsc.Port)
	obj, err := this.clientLRU.Get(connectionAddress)
	if err != nil {
		return nil, "", err
	}

	return obj.(*util.ServiceCtx), hostId, nil
}

func (this *Client) blockAcceptFunc(node *file.ValueNode) bool {
	// A host must be:
	//
	// 1. Known to the system.
	id := this.clusterState.HostId(node.LabelValue)
	glog.V(logging.LogLevelTrace).Infof("Found host id: %s for label: %s", id, node.LabelValue)
	if len(id) == 0 {
		glog.V(logging.LogLevelTrace).Infof("No configuration for host %s", node.LabelValue)
		return false
	}

	// 2. Alive and healthy.
	hostStatus := this.clusterState.HostStat(id)
	glog.V(logging.LogLevelTrace).Infof("Found host status: %v for id: %s", hostStatus, id)
	if hostStatus == nil {
		glog.V(logging.LogLevelTrace).Infof("No status for host %s", node.LabelValue)
		return false
	}

	for _, volumeStatus := range hostStatus.VolumeStatus {
		// 3. Have status info on the pvId in question.
		if volumeStatus.Id == node.Value.Id {
			glog.V(logging.LogLevelTrace).Infof("Found pv id: %s for id: %s", volumeStatus.Id, node.Value.Id)

			fsStats := volumeStatus.FileSystemStatus
			bytesAvailable := fsStats.BlocksAvailable * uint64(fsStats.BlockSize)

			// 4. Have enough space available.
			gbAvail := size.Bytes(float64(bytesAvailable)).ToGigabytes()
			if gbAvail >= 10 {
				glog.V(logging.LogLevelTrace).Infof("Found enough space %.03fGB on %s", gbAvail, volumeStatus.Id)
				return true
			}

			glog.V(logging.LogLevelTrace).Infof("Not enough space %.03f on PV %s on host %s", gbAvail, node.Value.Id, node.LabelValue)
		}
	}

	glog.V(logging.LogLevelTrace).Infof("No PV %s on host %s", node.Value.Id, node.LabelValue)
	return false
}
