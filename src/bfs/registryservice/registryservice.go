package registryservice

import (
	"bfs/config"
	"bfs/selector"
	"context"
	"fmt"
	"github.com/golang/glog"
	"time"
)

type RegistryService struct {
	registeredHosts map[string]*config.HostConfig
	hostStatus      map[string]*HostStatus

	hostLabels map[string][]*config.Label
}

func New() *RegistryService {
	return &RegistryService{
		hostStatus:      make(map[string]*HostStatus, 64),
		registeredHosts: make(map[string]*config.HostConfig, 64),
		hostLabels:      make(map[string][]*config.Label, 64),
	}
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
