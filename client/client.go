package client

import (
	"bfs/blockservice"
	"bfs/config"
	"bfs/file"
	"bfs/lru"
	"bfs/nameservice"
	"bfs/registryservice"
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
	"unsafe"
)

type Client struct {
	etcdClient *clientv3.Client
	clientLRU  *lru.LRUCache
	hash       *consistent.Consistent

	volumeConfigs         map[string]*config.LogicalVolumeConfig
	volumesWatchCancel    context.CancelFunc
	hostConfigs           map[string]*config.HostConfig
	hostsWatchCancel      context.CancelFunc
	hostStatus            map[string]*registryservice.HostStatus
	hostStatusWatchCancel context.CancelFunc
}

type serviceClient struct {
	conn        *grpc.ClientConn
	nameClient  nameservice.NameServiceClient
	blockClient blockservice.BlockServiceClient
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
		etcdClient:    etcdClient,
		hostConfigs:   make(map[string]*config.HostConfig, 64),
		hostStatus:    make(map[string]*registryservice.HostStatus, 64),
		volumeConfigs: make(map[string]*config.LogicalVolumeConfig, 4),
		hash:          consistent.New(),
	}

	client.hash.NumberOfReplicas = 10

	if err := client.startVolumeUpdater(); err != nil {
		return nil, err
	}

	if err := client.startHostUpdater(); err != nil {
		return nil, err
	}

	client.clientLRU = lru.NewCache(
		2,
		func(name string) (interface{}, error) {
			glog.V(2).Infof("Creating new connection for %s", name)

			conn, err := grpc.Dial(name, grpc.WithBlock(), grpc.WithInsecure())
			if err != nil {
				return nil, err
			}

			c := &serviceClient{
				conn:        conn,
				nameClient:  nameservice.NewNameServiceClient(conn),
				blockClient: blockservice.NewBlockServiceClient(conn),
			}

			return c, nil
		},
		func(name string, value interface{}) error {
			glog.V(2).Infof("Destroying connection for %s", name)

			return value.(*serviceClient).conn.Close()
		},
	)

	return client, nil
}

func (this *Client) startVolumeUpdater() error {
	volumesResp, err := this.etcdClient.Get(
		context.Background(),
		filepath.Join(DefaultEtcdPrefix, EtcdVolumesPrefix),
		clientv3.WithPrefix(),
	)
	if err != nil {
		return err
	}

	for _, kv := range volumesResp.Kvs {
		glog.V(2).Infof("Found volume: %s", string(kv.Key))

		lvConfig := &config.LogicalVolumeConfig{}

		if err := proto.UnmarshalText(string(kv.Value), lvConfig); err != nil {
			glog.Warningf("Unable to deserialize host config from %s - %v", string(kv.Key), err)
			continue
		} else {
			var mount string
			for _, label := range lvConfig.Labels {
				if label.Key == "mount" {
					mount = label.Value
				}
			}

			if mount == "" {
				glog.Warningf("Volume %s has no mount label", lvConfig.Id)
				continue
			}

			this.volumeConfigs[mount] = lvConfig
		}
	}

	ctx, volumesWatchCancel := context.WithCancel(context.Background())
	this.volumesWatchCancel = volumesWatchCancel

	go func() {
		glog.V(2).Infof("Volume watcher process starting")

		volumesWatchChan := this.etcdClient.Watch(
			ctx,
			filepath.Join(DefaultEtcdPrefix, EtcdVolumesPrefix),
			clientv3.WithPrefix(),
			clientv3.WithRev(volumesResp.Header.Revision),
		)

		for watchEvent := range volumesWatchChan {
			for _, event := range watchEvent.Events {
				glog.V(2).Infof("Update to volume %s - %v", string(event.Kv.Key), event.Type)

				lvConfig := &config.LogicalVolumeConfig{}
				if err := proto.UnmarshalText(string(event.Kv.Value), lvConfig); err != nil {
					glog.Warningf("Unable to deserialize volume config from %s - %v", string(event.Kv.Key), err)
					continue
				}

				var mount string
				for _, label := range lvConfig.Labels {
					if label.Key == "mount" {
						mount = label.Value
						break
					}
				}

				if mount == "" {
					glog.Warningf("Volume %s has no mount label", lvConfig.Id)
					continue
				}

				switch event.Type {
				case mvccpb.PUT:
					this.volumeConfigs[mount] = lvConfig
				case mvccpb.DELETE:
					delete(this.volumeConfigs, mount)
				default:
					glog.Warningf("Unknown event type %v received in volume watcher", event.Type)
				}
			}
		}

		glog.V(2).Infof("Volume watcher process complete")
	}()

	return nil
}

func (this *Client) startHostUpdater() error {
	hostResp, err := this.etcdClient.Get(
		context.Background(),
		filepath.Join(DefaultEtcdPrefix, EtcdHostsPrefix),
		clientv3.WithPrefix(),
	)
	if err != nil {
		return err
	}

	for _, kv := range hostResp.Kvs {
		glog.V(2).Infof("Found host %s", string(kv.Key))

		pathComponents := strings.Split(string(kv.Key), string(filepath.Separator))
		if len(pathComponents) < 4 {
			glog.Warningf("Host key entry %s (components: %v) is of the wrong format - skipping", string(kv.Key), pathComponents)
			continue
		}

		entryType := pathComponents[len(pathComponents)-2]

		if entryType == "config" {
			hostConfig := &config.HostConfig{}

			if err := proto.UnmarshalText(string(kv.Value), hostConfig); err != nil {
				glog.Warningf("Unable to deserialize host config from %s - %v", string(kv.Key), err)
				continue
			} else {
				this.hostConfigs[hostConfig.Id] = hostConfig
				this.hash.Add(hostConfig.Id)
			}
		} else if entryType == "status" {
			status := &registryservice.HostStatus{}

			if err := proto.UnmarshalText(string(kv.Value), status); err != nil {
				glog.Warningf("Unable to deserialize host status from %s - %v", string(kv.Key), err)
				continue
			} else {
				this.hostStatus[status.Id] = status
			}
		}
	}

	ctx, hostWatchCancel := context.WithCancel(context.Background())
	this.hostsWatchCancel = hostWatchCancel

	go func() {
		glog.V(2).Infof("Hosts watcher process starting")

		hostsWatchChan := this.etcdClient.Watch(
			ctx,
			filepath.Join(DefaultEtcdPrefix, EtcdHostsPrefix, EtcdHostsConfigPrefix),
			clientv3.WithPrefix(),
			clientv3.WithRev(hostResp.Header.Revision),
		)

		for watchEvent := range hostsWatchChan {
			for _, event := range watchEvent.Events {
				glog.V(2).Infof("Update to host %s - %v", string(event.Kv.Key), event.Type)

				pathComponents := filepath.SplitList(string(event.Kv.Key))
				if len(pathComponents) < 4 {
					glog.Warning("Host key entry %s is of the wrong format - skipping", string(event.Kv.Key))
					continue
				}

				entryType := pathComponents[len(pathComponents)-2]

				if entryType == "config" {
					hostConfig := &config.HostConfig{}

					if err := proto.UnmarshalText(string(event.Kv.Value), hostConfig); err != nil {
						glog.Warningf("Unable to deserialize host config from %s - %v", string(event.Kv.Key), err)
						continue
					} else {
						switch event.Type {
						case mvccpb.PUT:
							this.hostConfigs[hostConfig.Id] = hostConfig
							this.hash.Add(hostConfig.Id)
						case mvccpb.DELETE:
							delete(this.hostConfigs, hostConfig.Id)
							this.hash.Remove(hostConfig.Id)
						default:
							glog.Warningf("Unknown event type %v received in host watcher", event.Type)
						}
					}
				} else if entryType == "status" {
					status := &registryservice.HostStatus{}

					if err := proto.UnmarshalText(string(event.Kv.Value), status); err != nil {
						glog.Warningf("Unable to deserialize host status from %s - %v", string(event.Kv.Key), err)
						continue
					} else {
						switch event.Type {
						case mvccpb.PUT:
							this.hostStatus[status.Id] = status
						case mvccpb.DELETE:
							delete(this.hostStatus, status.Id)
						default:
							glog.Warningf("Unknown event type %v received in host watcher", event.Type)
						}
					}
				}
			}
		}

		glog.V(2).Infof("Hosts watcher process complete")
	}()

	return nil
}

func (this *Client) Hosts() []*config.HostConfig {
	hostConfigs := make([]*config.HostConfig, 0, len(this.hostConfigs))
	for _, v := range this.hostConfigs {
		hostConfigs = append(hostConfigs, v)
	}

	return hostConfigs
}

func (this *Client) HostStatus() []*registryservice.HostStatus {
	hostStatus := make([]*registryservice.HostStatus, 0, len(this.hostStatus))
	for _, v := range this.hostStatus {
		hostStatus = append(hostStatus, v)
	}

	return hostStatus
}

func (this *Client) Create(path string, blockSize int) (file.Writer, error) {
	var pvIds []string

	for mount, lvConfig := range this.volumeConfigs {
		glog.V(2).Infof("Checking volume mount %s for file %s", mount, path)
		if strings.HasPrefix(path, mount) {
			pvIds = lvConfig.PvIds
			break
		}
	}

	if len(pvIds) == 0 {
		return nil, fmt.Errorf("unable to find volume for file %s", path)
	}

	conn, _, err := this.connectionForPath(path)
	if err != nil {
		return nil, err
	}

	return file.NewWriter(conn.nameClient, conn.blockClient, pvIds, path, blockSize)
}

func (this *Client) Open(path string) (file.Reader, error) {
	conn, _, err := this.connectionForPath(path)
	if err != nil {
		return nil, err
	}

	reader := file.NewReader(conn.nameClient, conn.blockClient, path)
	return reader, reader.Open()
}

func (this *Client) Stat(path string) (*nameservice.Entry, error) {
	conn, _, err := this.connectionForPath(path)

	getResp, err := conn.nameClient.Get(context.Background(), &nameservice.GetRequest{Path: path})
	if err != nil {
		return nil, err
	}

	return getResp.Entry, nil
}

func (this *Client) Remove(path string) error {
	conn, _, err := this.connectionForPath(path)
	if err != nil {
		return err
	}

	_, err = conn.nameClient.Delete(context.Background(), &nameservice.DeleteRequest{Path: path})
	if err != nil {
		return err
	}

	return nil
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
		getResp, err := sourceConn.nameClient.Get(context.Background(), &nameservice.GetRequest{Path: sourcePath})
		if err != nil {
			return err
		}

		_, err = destConn.nameClient.Add(context.Background(), &nameservice.AddRequest{
			Entry: getResp.Entry,
		})
		if err != nil {
			return err
		}

		_, err = sourceConn.nameClient.Delete(context.Background(), &nameservice.DeleteRequest{Path: sourcePath})
		if err != nil {
			return err
		}
	} else {
		// Rename is on the same host.
		_, err := sourceConn.nameClient.Rename(
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
		for _, hostConfig := range this.hostConfigs {
			glog.V(2).Infof("List on %s", hostConfig.Hostname)

			o, err := this.clientLRU.Get(hostConfig.NameServiceConfig.AdvertiseAddress)
			if err != nil {
				close(iterChan)
				return
			}
			conn := o.(*serviceClient)

			listStream, err := conn.nameClient.List(context.Background(), &nameservice.ListRequest{StartKey: startKey, EndKey: endKey})
			if err != nil {
				glog.V(2).Infof("Closing list stream due to %v", err)
				close(iterChan)
				return
			}

			for {
				resp, err := listStream.Recv()
				if err == io.EOF {
					glog.V(2).Infof("Finished list receive chunk")
					break
				} else if err != nil {
					glog.V(2).Infof("Closing list stream due to %v", err)
					close(iterChan)
					break
				}

				for _, entry := range resp.Entries {
					iterChan <- entry
				}
			}
		}

		close(iterChan)
		glog.V(2).Infof("List stream complete")
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
	var size uintptr = 0
	size += unsafe.Sizeof(config.HostConfig{}) * uintptr(len(this.hostConfigs))
	size += unsafe.Sizeof(config.LogicalVolumeConfig{}) * uintptr(len(this.volumeConfigs))
	return size
}

func (this *Client) Close() error {
	if this.clientLRU != nil {
		this.clientLRU.Purge()
	}

	if this.volumesWatchCancel != nil {
		this.volumesWatchCancel()
	}

	if this.hostsWatchCancel != nil {
		this.hostsWatchCancel()
	}

	if this.etcdClient != nil {
		if err := this.etcdClient.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (this *Client) connectionForPath(path string) (*serviceClient, string, error) {
	hostId, err := this.hash.Get(path)
	if err != nil {
		return nil, "", err
	}

	obj, err := this.clientLRU.Get(this.hostConfigs[hostId].NameServiceConfig.AdvertiseAddress)
	if err != nil {
		return nil, "", err
	}

	return obj.(*serviceClient), hostId, nil
}
