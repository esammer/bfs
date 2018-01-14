package registryservice

import (
	"context"
	"github.com/golang/glog"
	"time"
)

type RegistryService struct {
	hostRegistry map[string]*HostStatus
}

func New() *RegistryService {
	return &RegistryService{
		hostRegistry: make(map[string]*HostStatus, 64),
	}
}

func (this *RegistryService) Hosts(ctx context.Context, request *HostsRequest) (*HostsResponse, error) {
	hosts := make([]*HostStatus, 0, 40)

	for _, entry := range this.hostRegistry {
		hosts = append(hosts, entry)
	}

	return &HostsResponse{Hosts: hosts}, nil
}

func (this *RegistryService) HostStatus(ctx context.Context, request *HostStatusRequest) (*HostStatusResponse, error) {
	hostStatus := &HostStatus{
		Id:          request.Id,
		Hostname:    request.Hostname,
		Port:        request.Port,
		FirstSeen:   time.Now().UnixNano(),
		LastSeen:    time.Now().UnixNano(),
		VolumeStats: request.VolumeStats,
	}

	var distance time.Duration

	if entry, ok := this.hostRegistry[request.Id]; ok {
		distance = time.Since(time.Unix(0, entry.LastSeen))
		hostStatus.FirstSeen = entry.FirstSeen
	}

	glog.V(2).Infof("Host report - %s (%s) last seen: %s distance from previous: %s",
		request.Hostname,
		request.Id,
		time.Unix(0, hostStatus.LastSeen).String(),
		distance.String(),
	)

	this.hostRegistry[request.Id] = hostStatus

	return &HostStatusResponse{}, nil
}
