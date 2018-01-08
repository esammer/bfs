package nameservice

import (
	"bfs/ns"
	"context"
)

type NameService struct {
	Namespace *ns.Namespace

	NameServiceServer
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
			Path:        entry.Path,
			LvId:        entry.VolumeName,
			Blocks:      blocks,
			Permissions: uint32(entry.Permissions),
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
		Path:        request.Entry.Path,
		VolumeName:  request.Entry.LvId,
		Blocks:      blocks,
		Permissions: uint8(request.Entry.Permissions),
		Status:      ns.FileStatus_OK,
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
