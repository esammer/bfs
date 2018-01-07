package host

import (
	"bfs/volume"
	"fmt"
	"github.com/golang/glog"
	"github.com/pborman/uuid"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
)

type Host struct {
	Config *HostConfig
	Id     uuid.UUID

	Hostname      string
	BindAddresses []net.Addr
	Volumes       []*volume.PhysicalVolume
}

type HostConfig struct {
	RootPath      string
	BindInterface string
}

func New(config *HostConfig) (*Host, error) {
	host := &Host{
		Config: config,
	}

	err := host.discoverIdentity()
	if err != nil {
		return nil, err
	}

	err = host.discoverAddresses()
	if err != nil {
		return nil, err
	}

	err = host.discoverPhysicalVolumes()
	if err != nil {
		return nil, err
	}

	return host, nil
}

func (this *Host) discoverIdentity() error {
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	this.Hostname = hostname

	idPath := filepath.Join(this.Config.RootPath, "id")

	_, err = os.Stat(idPath)
	if err != nil {
		if os.IsNotExist(err) {
			id := uuid.NewRandom()

			err := ioutil.WriteFile(filepath.Join(idPath), []byte(id.String()), 0644)
			if err != nil {
				return err
			}

			glog.V(1).Infof("Generated host id %s", id.String())
		} else {
			return err
		}
	}

	id, err := ioutil.ReadFile(idPath)
	if err != nil {
		return err
	}

	this.Id = uuid.Parse(string(id))
	if this.Id == nil {
		return fmt.Errorf("unable to parse '%s' as a uuid", string(id))
	}

	glog.V(1).Infof("Loaded host id %s", this.Id.String())

	return nil
}

func (this *Host) discoverAddresses() error {
	interfaces, err := net.Interfaces()
	if err != nil {
		return err
	}

	addresses := make([]net.Addr, 0, 16)

	for ifaceIdx, iface := range interfaces {
		glog.V(2).Infof("Interface %d: %s", ifaceIdx, iface.Name)

		ifaceAddrs, err := iface.Addrs()
		if err != nil {
			return err
		}

		for addrIdx, addr := range ifaceAddrs {
			glog.V(2).Infof("Interface %d: %s Address %d: %v (%v)", ifaceIdx, iface.Name, addrIdx, addr.String(),
				addr.Network())
			addresses = append(addresses, addr)
		}
	}

	this.BindAddresses = addresses

	return nil
}

func (this *Host) discoverPhysicalVolumes() error {
	glog.V(1).Infof("Discovering physical volumes on %s at %s", this.Hostname, this.Config.RootPath)

	entries, err := ioutil.ReadDir(this.Config.RootPath)
	if err != nil {
		return err
	}

	pvs := make([]*volume.PhysicalVolume, 0, 16)

	for _, entry := range entries {
		path := filepath.Join(this.Config.RootPath, entry.Name())

		glog.V(2).Infof("Checking %s", path)

		if entry.IsDir() {
			pvIdPath := filepath.Join(path, "id")
			glog.V(2).Infof("Checking for %s", pvIdPath)

			info, err := os.Stat(pvIdPath)
			if os.IsNotExist(err) || info.IsDir() {
				continue
			} else if err != nil {
				return err
			}

			glog.V(1).Infof("Found volume %s", path)

			pv := volume.NewPhysicalVolume(path, nil)
			pvs = append(pvs, pv)
		}
	}

	this.Volumes = pvs

	return nil
}