package nameservice

import (
	"bfs/ns"
	"context"
	"io"
	"time"
)

const (
	DefaultListBatchSize = 512
)

type NameService struct {
	Namespace *ns.Namespace
}

func New(namespace *ns.Namespace) *NameService {
	return &NameService{
		Namespace: namespace,
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
			Ctime:            &Time{entry.Ctime.Unix(), int64(entry.Ctime.Nanosecond())},
			Mtime:            &Time{entry.Mtime.Unix(), int64(entry.Mtime.Nanosecond())},
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
		Ctime:            time.Unix(request.Entry.Ctime.Seconds, request.Entry.Ctime.Nanos).UTC(),
		Mtime:            time.Unix(request.Entry.Mtime.Seconds, request.Entry.Mtime.Nanos).UTC(),
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
			Ctime:            &Time{Seconds: entry.Ctime.Unix(), Nanos: int64(entry.Ctime.Nanosecond())},
			Mtime:            &Time{Seconds: entry.Mtime.Unix(), Nanos: int64(entry.Mtime.Nanosecond())},
		})

		return true, nil
	})

	if err != nil {
		return err
	}

	return nil
}
