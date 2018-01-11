package nameservice

import (
	"bfs/ns"
	"context"
	"github.com/golang/glog"
	"io"
	"time"
)

const (
	DefaultListBatchSize = 512
)

type NameService struct {
	Namespace *ns.Namespace

	hostRegistry map[string]*Host

	NameServiceServer
}

type Host struct {
	id          string
	hostname    string
	port        uint32
	pvIds       []string
	firstSeen   time.Time
	lastSeen    time.Time
	volumeStats []*PhysicalVolumeStatus
}

func New(namespace *ns.Namespace) *NameService {
	return &NameService{
		Namespace:    namespace,
		hostRegistry: make(map[string]*Host, 40),
	}
}

func (this *NameService) Get(ctx context.Context, request *GetRequest) (*GetResponse, error) {
	entry, err := this.Namespace.Get(request.Path)
	if err != nil {
		return nil, err
	}

	blocks := make([]*BlockMetadata, 0, len(entry.Blocks))

	for _, block := range entry.Blocks {
		pBlock := &BlockMetadata{
			BlockId: block.Block,
			PvId:    block.PVID,
		}

		blocks = append(blocks, pBlock)
	}

	return &GetResponse{
		Entry: &Entry{
			Path:             entry.Path,
			LvId:             entry.VolumeName,
			Blocks:           blocks,
			Permissions:      uint32(entry.Permissions),
			BlockSize:        entry.BlockSize,
			ReplicationLevel: entry.ReplicationLevel,
			Size:             entry.Size,
		},
	}, nil
}

func (this *NameService) Add(ctx context.Context, request *AddRequest) (*AddResponse, error) {
	blocks := make([]*ns.BlockMetadata, 0, len(request.Entry.Blocks))

	for _, pBlock := range request.Entry.Blocks {
		blocks = append(blocks, &ns.BlockMetadata{
			Block:  pBlock.BlockId,
			LVName: request.Entry.LvId,
			PVID:   pBlock.PvId,
		})
	}

	entry := &ns.Entry{
		Path:             request.Entry.Path,
		VolumeName:       request.Entry.LvId,
		Blocks:           blocks,
		Permissions:      uint8(request.Entry.Permissions),
		Status:           ns.FileStatus_OK,
		BlockSize:        request.Entry.BlockSize,
		Size:             request.Entry.Size,
		ReplicationLevel: request.Entry.ReplicationLevel,
	}

	if err := this.Namespace.Add(entry); err != nil {
		return nil, err
	}

	return &AddResponse{}, nil
}

func (this *NameService) Delete(ctx context.Context, request *DeleteRequest) (*DeleteResponse, error) {
	return &DeleteResponse{}, this.Namespace.Delete(request.Path)
}

func (this *NameService) Rename(ctx context.Context, request *RenameRequest) (*RenameResponse, error) {
	ok, err := this.Namespace.Rename(request.SourcePath, request.DestinationPath)
	if err != nil {
		return nil, err
	}

	return &RenameResponse{
		Success: ok,
	}, nil
}

func (this *NameService) List(request *ListRequest, stream NameService_ListServer) error {
	var pEntries []*Entry

	err := this.Namespace.List(request.StartKey, request.EndKey, func(entry *ns.Entry, err error) (bool, error) {
		if err == io.EOF {
			err := stream.Send(&ListResponse{
				Entries: pEntries,
			})
			if err != nil {
				return false, err
			}

			pEntries = nil

			return false, io.EOF
		} else if err != nil {
			return false, err
		}

		if len(pEntries) == DefaultListBatchSize {
			err := stream.Send(&ListResponse{
				Entries: pEntries,
			})
			if err != nil {
				return false, err
			}

			pEntries = nil
		}

		if pEntries == nil {
			pEntries = make([]*Entry, 0, DefaultListBatchSize)
		}

		blocks := make([]*BlockMetadata, 0, len(entry.Blocks))

		for _, block := range entry.Blocks {
			pBlock := &BlockMetadata{
				BlockId: block.Block,
				PvId:    block.PVID,
			}

			blocks = append(blocks, pBlock)
		}

		pEntries = append(pEntries, &Entry{
			Path:             entry.Path,
			Size:             entry.Size,
			ReplicationLevel: entry.ReplicationLevel,
			BlockSize:        entry.BlockSize,
			LvId:             entry.VolumeName,
			Permissions:      uint32(entry.Permissions),
			Blocks:           blocks,
		})

		return true, nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (this *NameService) VolumeInfo(ctx context.Context, request *VolumeInfoRequest) (*VolumeInfoResponse, error) {
	pvIds, err := this.Namespace.Volume(request.VolumeId)
	if err != nil {
		return nil, err
	}

	return &VolumeInfoResponse{PvIds: pvIds}, nil
}

func (this *NameService) AddVolume(ctx context.Context, request *AddVolumeRequest) (*AddVolumeResponse, error) {
	err := this.Namespace.AddVolume(request.VolumeId, request.PvIds)
	if err != nil {
		return nil, err
	}

	return &AddVolumeResponse{}, nil
}

func (this *NameService) HostReport(ctx context.Context, request *HostReportRequest) (*HostReportResponse, error) {
	host := &Host{
		id:          request.Id,
		hostname:    request.Hostname,
		port:        request.Port,
		pvIds:       request.PvIds,
		firstSeen:   time.Now(),
		lastSeen:    time.Now(),
		volumeStats: request.VolumeStats,
	}

	var distance time.Duration

	if entry, ok := this.hostRegistry[request.Id]; ok {
		host.firstSeen = entry.firstSeen
		distance = host.lastSeen.Sub(entry.lastSeen)
	}

	glog.V(2).Infof("Host report - %s (%s) last seen: %s distance from previous: %s",
		request.Hostname,
		request.Id,
		host.lastSeen,
		distance.String(),
	)

	this.hostRegistry[request.Id] = host

	return &HostReportResponse{}, nil
}

func (this *NameService) Hosts(ctx context.Context, request *HostsRequest) (*HostsResponse, error) {
	hosts := make([]*KnownHost, 0, 40)

	for _, entry := range this.hostRegistry {
		hosts = append(hosts, &KnownHost{
			Id:          entry.id,
			PvIds:       entry.pvIds,
			Hostname:    entry.hostname,
			Port:        entry.port,
			FirstSeen:   entry.firstSeen.Unix(),
			LastSeen:    entry.lastSeen.Unix(),
			VolumeStats: entry.volumeStats,
		})
	}

	return &HostsResponse{Hosts: hosts}, nil
}
