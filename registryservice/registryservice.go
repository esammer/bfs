package registryservice

import (
	"bfs/config"
	"bfs/selector"
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"sync"
	"time"
)

type RegistryService struct {
	registeredHosts map[string]*config.HostConfig
	hostStatus      map[string]*HostStatus

	hostLabels map[string][]*config.Label

	etcdClient      *clientv3.Client
	volumeConfigs   map[string]*config.LogicalVolumeConfig
	volumeConfigMut *sync.Mutex
}

func New(etcdClient *clientv3.Client) *RegistryService {
	registryService := &RegistryService{
		hostStatus:      make(map[string]*HostStatus, 64),
		registeredHosts: make(map[string]*config.HostConfig, 64),
		hostLabels:      make(map[string][]*config.Label, 64),
		etcdClient:      etcdClient,
		volumeConfigs:   make(map[string]*config.LogicalVolumeConfig),
		volumeConfigMut: &sync.Mutex{},
	}

	go func() {
		glog.V(1).Infof("Volumes watch process started")

		volumesWatchChan := etcdClient.Watch(context.Background(), "/bfs/volumes", clientv3.WithPrefix())

		for volumesWatchEvent := range volumesWatchChan {
			for _, event := range volumesWatchEvent.Events {
				glog.V(2).Infof("Received watch event %s = %s", event.Kv.Key, event.Kv.Value)
				lvc := &config.LogicalVolumeConfig{}
				if err := proto.UnmarshalText(string(event.Kv.Value), lvc); err != nil {
					glog.V(2).Infof("Failed to parse protobuf: %v", err)
					continue
				}

				registryService.volumeConfigMut.Lock()
				glog.V(2).Infof("Adding logical volume config: %v id: %s", lvc, lvc.Id)
				registryService.volumeConfigs[lvc.Id] = lvc
				registryService.volumeConfigMut.Unlock()
			}
		}

		glog.V(1).Infof("Volumes watch process complete")
	}()

	return registryService
}

func (this *RegistryService) RegisterHost(ctx context.Context, request *RegisterHostRequest) (*RegisterHostResponse, error) {
	hostConfig := request.HostConfig

	glog.V(1).Infof("Register host id: %s hostname: %s", hostConfig.Id, hostConfig.Hostname)

	if val, ok := this.registeredHosts[hostConfig.Id]; ok {
		return nil, fmt.Errorf("host %s already registered from hostname %s", val.Id, val.Hostname)
	}

	this.registeredHosts[hostConfig.Id] = hostConfig
	this.hostLabels[hostConfig.Id] = hostConfig.Labels

	return &RegisterHostResponse{}, nil
}

func (this *RegistryService) Hosts(ctx context.Context, request *HostsRequest) (*HostsResponse, error) {
	glog.V(2).Infof("Received host status query - selector: %s", request.Selector)

	hosts := make([]*HostStatus, 0, 40)
	hostConfigs := make([]*config.HostConfig, 0, 40)

	var sel *selector.Selector
	if request.Selector != "" {
		var err error

		sel, err = selector.ParseSelector(request.Selector)
		if err != nil {
			return nil, err
		}
	}

	for key, entry := range this.registeredHosts {
		if sel != nil && !sel.Evaluate(entry.Labels) {
			continue
		}

		glog.V(2).Infof("Returning host: %s", entry.Hostname)

		hosts = append(hosts, this.hostStatus[key])
		hostConfigs = append(hostConfigs, entry)
	}

	return &HostsResponse{Hosts: hosts, HostConfigs: hostConfigs}, nil
}

func (this *RegistryService) HostStatus(ctx context.Context, request *HostStatusRequest) (*HostStatusResponse, error) {
	if _, ok := this.registeredHosts[request.Id]; !ok {
		glog.Warningf("Received status report from unknown host %s", request.Id)
	}

	hostStatus := &HostStatus{
		Id:          request.Id,
		FirstSeen:   time.Now().UnixNano(),
		LastSeen:    time.Now().UnixNano(),
		VolumeStats: request.VolumeStats,
	}

	var distance time.Duration

	if entry, ok := this.hostStatus[request.Id]; ok {
		distance = time.Since(time.Unix(0, entry.LastSeen))
		hostStatus.FirstSeen = entry.FirstSeen
	}

	glog.V(2).Infof("Host report - %s last seen: %s distance from previous: %s",
		request.Id,
		time.Unix(0, hostStatus.LastSeen).String(),
		distance.String(),
	)

	this.hostStatus[request.Id] = hostStatus

	return &HostStatusResponse{}, nil
}

func (this *RegistryService) LogicalVolumeInfo(ctx context.Context, request *LogicalVolumeInfoRequest) (*LogicalVolumeInfoResponse, error) {
	glog.V(2).Infof("Received logical volume info request for %s", request.Id)

	this.volumeConfigMut.Lock()
	defer this.volumeConfigMut.Unlock()

	if val, ok := this.volumeConfigs[request.Id]; ok {
		return &LogicalVolumeInfoResponse{Config: val}, nil
	} else {
		return nil, fmt.Errorf("no logical volume with id: %s", request.Id)
	}
}
