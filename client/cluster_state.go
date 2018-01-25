package client

import (
	"bfs/config"
	"sync"
)

type ClusterState struct {
	hostConfigs     map[string]*config.HostConfig
	hostNameIdIndex map[string]string
	hostConfigsMut  sync.RWMutex
	hostStatus      map[string]*config.HostStatus
	hostStatusMut   sync.RWMutex
	pvConfigs       map[string]*config.PhysicalVolumeConfig
	pvConfigsMut    sync.RWMutex
	pvStatus        map[string]*config.PhysicalVolumeStatus
	pvStatusMut     sync.RWMutex
	lvConfigs       map[string]*config.LogicalVolumeConfig
	lvConfigsMut    sync.RWMutex
}

func NewClusterState() *ClusterState {
	return &ClusterState{
		hostNameIdIndex: make(map[string]string),
		hostConfigs:     make(map[string]*config.HostConfig),
		hostConfigsMut:  sync.RWMutex{},
		hostStatus:      make(map[string]*config.HostStatus),
		hostStatusMut:   sync.RWMutex{},
		pvConfigs:       make(map[string]*config.PhysicalVolumeConfig),
		pvConfigsMut:    sync.RWMutex{},
		pvStatus:        make(map[string]*config.PhysicalVolumeStatus),
		pvStatusMut:     sync.RWMutex{},
		lvConfigs:       make(map[string]*config.LogicalVolumeConfig),
		lvConfigsMut:    sync.RWMutex{},
	}
}

func (this *ClusterState) HostId(hostname string) string {
	this.hostConfigsMut.RLock()
	defer this.hostConfigsMut.RUnlock()

	return this.hostNameIdIndex[hostname]
}

func (this *ClusterState) Hosts() ([]*config.HostConfig, []*config.HostStatus) {
	this.hostConfigsMut.RLock()
	defer this.hostConfigsMut.RUnlock()
	this.hostStatusMut.RLock()
	defer this.hostStatusMut.RUnlock()

	hostConfigs := make([]*config.HostConfig, 0, len(this.hostConfigs))
	// Purposefully allocate on configs, not status. We need to return the same number of results in both slices.
	hostStatus := make([]*config.HostStatus, 0, len(this.hostConfigs))

	for id, hostConfig := range this.hostConfigs {
		hostConfigs = append(hostConfigs, hostConfig)
		hostStatus = append(hostStatus, this.hostStatus[id])
	}

	return hostConfigs, hostStatus
}

func (this *ClusterState) Host(hostId string) (*config.HostConfig, *config.HostStatus) {
	this.hostConfigsMut.RLock()
	defer this.hostConfigsMut.RUnlock()
	this.hostStatusMut.RLock()
	defer this.hostStatusMut.RUnlock()

	return this.hostConfigs[hostId], this.hostStatus[hostId]
}

func (this *ClusterState) HostStat(hostId string) *config.HostStatus {
	this.hostStatusMut.RLock()
	defer this.hostStatusMut.RUnlock()

	return this.hostStatus[hostId]
}

func (this *ClusterState) HostStatus() []*config.HostStatus {
	this.hostStatusMut.RLock()
	defer this.hostStatusMut.RUnlock()

	hostStatus := make([]*config.HostStatus, 0, len(this.hostStatus))
	for _, hostStat := range this.hostStatus {
		hostStatus = append(hostStatus, hostStat)
	}

	return hostStatus
}

func (this *ClusterState) HostConfigs() []*config.HostConfig {
	this.hostConfigsMut.RLock()
	defer this.hostConfigsMut.RUnlock()

	hostConfigs := make([]*config.HostConfig, 0, len(this.hostConfigs))

	for _, hostConfig := range this.hostConfigs {
		hostConfigs = append(hostConfigs, hostConfig)
	}

	return hostConfigs
}

func (this *ClusterState) HostConfig(hostId string) *config.HostConfig {
	this.hostConfigsMut.RLock()
	defer this.hostConfigsMut.RUnlock()

	return this.hostConfigs[hostId]
}

func (this *ClusterState) PhysicalVolumesForHost(hostId string) (map[string]*config.PhysicalVolumeConfig, map[string]*config.PhysicalVolumeStatus) {
	this.hostConfigsMut.RLock()
	defer this.hostConfigsMut.RUnlock()
	this.hostStatusMut.RLock()
	defer this.hostStatusMut.RUnlock()

	pvConfigs := make(map[string]*config.PhysicalVolumeConfig, len(this.hostConfigs[hostId].BlockServiceConfig.VolumeConfigs))
	pvStatus := make(map[string]*config.PhysicalVolumeStatus, len(this.hostConfigs[hostId].BlockServiceConfig.VolumeConfigs))

	for _, pvConfig := range this.hostConfigs[hostId].BlockServiceConfig.VolumeConfigs {
		pvConfigs[pvConfig.Id] = pvConfig
		pvStatus[pvConfig.Id] = this.pvStatus[pvConfig.Id]
	}

	return pvConfigs, pvStatus
}

func (this *ClusterState) PhysicalVolumesForLogicalVolume(lvId string) ([]*config.PhysicalVolumeConfig) {
	this.hostConfigsMut.RLock()
	defer this.hostConfigsMut.RUnlock()
	this.hostStatusMut.RLock()
	defer this.hostStatusMut.RUnlock()

	pvConfigs := make([]*config.PhysicalVolumeConfig, 0, len(this.lvConfigs[lvId].PvIds))

	for _, pvId := range this.lvConfigs[lvId].PvIds {
		pvConfigs = append(pvConfigs, this.pvConfigs[pvId])
	}

	return pvConfigs
}

func (this *ClusterState) PhysicalVolume(pvId string) (*config.PhysicalVolumeConfig, *config.PhysicalVolumeStatus) {
	this.pvConfigsMut.RLock()
	defer this.pvConfigsMut.RUnlock()
	this.pvStatusMut.RLock()
	defer this.pvStatusMut.RUnlock()

	return this.pvConfigs[pvId], this.pvStatus[pvId]
}

func (this *ClusterState) PhysicalVolumeStatus(pvId string) *config.PhysicalVolumeStatus {
	this.pvStatusMut.RLock()
	defer this.pvStatusMut.RUnlock()

	return this.pvStatus[pvId]
}

func (this *ClusterState) PhysicalVolumeConfig(pvId string) *config.PhysicalVolumeConfig {
	this.pvConfigsMut.RLock()
	defer this.pvConfigsMut.RUnlock()

	return this.pvConfigs[pvId]
}

func (this *ClusterState) LogicalVolumeConfigs() []*config.LogicalVolumeConfig {
	this.lvConfigsMut.RLock()
	defer this.lvConfigsMut.RUnlock()

	lvConfigs := make([]*config.LogicalVolumeConfig, 0, len(this.lvConfigs))

	for _, lvConfig := range this.lvConfigs {
		lvConfigs = append(lvConfigs, lvConfig)
	}

	return lvConfigs
}

func (this *ClusterState) LogicalVolumeConfig(lvId string) *config.LogicalVolumeConfig {
	this.lvConfigsMut.RLock()
	defer this.lvConfigsMut.RUnlock()

	return this.lvConfigs[lvId]
}

func (this *ClusterState) AddLogicalVolumeConfig(lvConfig *config.LogicalVolumeConfig) {
	this.lvConfigsMut.Lock()
	defer this.lvConfigsMut.Unlock()

	this.lvConfigs[lvConfig.Id] = lvConfig
}

func (this *ClusterState) RemoveLogicalVolumeConfig(id string) {
	this.lvConfigsMut.Lock()
	defer this.lvConfigsMut.Unlock()

	delete(this.lvConfigs, id)
}

func (this *ClusterState) AddHostConfig(hostConfig *config.HostConfig) {
	this.hostConfigsMut.Lock()
	defer this.hostConfigsMut.Unlock()
	this.pvConfigsMut.Lock()
	defer this.pvConfigsMut.Unlock()

	this.hostConfigs[hostConfig.Id] = hostConfig
	this.hostNameIdIndex[hostConfig.Hostname] = hostConfig.Id

	for _, pvConfig := range hostConfig.BlockServiceConfig.VolumeConfigs {
		this.pvConfigs[pvConfig.Id] = pvConfig
	}
}

func (this *ClusterState) RemoveHostConfig(id string) {
	this.hostConfigsMut.Lock()
	defer this.hostConfigsMut.Unlock()

	delete(this.hostConfigs, id)
}

func (this *ClusterState) AddHostStatus(hostStatus *config.HostStatus) {
	this.hostStatusMut.Lock()
	defer this.hostStatusMut.Unlock()
	this.pvStatusMut.Lock()
	defer this.pvStatusMut.Unlock()

	this.hostStatus[hostStatus.Id] = hostStatus

	for _, pvStat := range hostStatus.VolumeStatus {
		this.pvStatus[pvStat.Id] = pvStat
	}
}

func (this *ClusterState) RemoveHostStatus(id string) {
	this.hostStatusMut.Lock()
	defer this.hostStatusMut.Unlock()

	delete(this.hostStatus, id)
}

func (this *ClusterState) SetHostConfigs(hostConfigs map[string]*config.HostConfig) {
	this.hostConfigsMut.Lock()
	defer this.hostConfigsMut.Unlock()

	this.hostConfigs = hostConfigs
	this.hostNameIdIndex = make(map[string]string, len(this.hostConfigs))
	this.pvConfigs = make(map[string]*config.PhysicalVolumeConfig)

	for _, hostConfig := range this.hostConfigs {
		this.hostNameIdIndex[hostConfig.Hostname] = hostConfig.Id

		for _, pvConfig := range hostConfig.BlockServiceConfig.VolumeConfigs {
			this.pvConfigs[pvConfig.Id] = pvConfig
		}
	}
}

func (this *ClusterState) SetHostStatus(hostStatus map[string]*config.HostStatus) {
	this.hostStatusMut.Lock()
	defer this.hostStatusMut.Unlock()

	this.hostStatus = hostStatus
	this.pvStatus = make(map[string]*config.PhysicalVolumeStatus)

	for _, hostStat := range this.hostStatus {
		for _, pvStat := range hostStat.VolumeStatus {
			this.pvStatus[pvStat.Id] = pvStat
		}
	}
}

func (this *ClusterState) SetPhysicalVolumeConfigs(pvConfigs map[string]*config.PhysicalVolumeConfig) {
	this.pvConfigsMut.Lock()
	defer this.pvConfigsMut.Unlock()

	this.pvConfigs = pvConfigs
}

func (this *ClusterState) SetLogicalVolumeConfigs(lvConfigs map[string]*config.LogicalVolumeConfig) {
	this.lvConfigsMut.Lock()
	defer this.lvConfigsMut.Unlock()

	this.lvConfigs = lvConfigs
}
