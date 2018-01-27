package file

import (
	"bfs/blockservice"
	"bfs/nameservice"
	"bfs/util/logging"
	"bfs/util/size"
	"context"
	"github.com/golang/glog"
	"io"
)

type Reader interface {
	io.Reader
	io.Closer

	Open() error
}

type LocalFileReader struct {
	// Configuration
	nameClient  nameservice.NameServiceClient
	blockClient blockservice.BlockServiceClient
	filename    string

	// Reader state
	entry    *nameservice.Entry
	blockIdx int

	// Current block state
	blockReader blockservice.BlockService_ReadClient
	blockBuf    []byte
	blockPos    int

	// Metrics
	readCalls     int
	receiveCalls  int
	minReadSize   int
	readLoopIters int
}

func NewReader(nameClient nameservice.NameServiceClient, blockClient blockservice.BlockServiceClient, path string) *LocalFileReader {
	return &LocalFileReader{
		nameClient:  nameClient,
		blockClient: blockClient,
		filename:    path,
	}
}

func (this *LocalFileReader) Open() error {
	glog.V(logging.LogLevelDebug).Infof("Opening reader for %s", this.filename)

	resp, err := this.nameClient.Get(context.Background(), &nameservice.GetRequest{
		Path: this.filename,
	})
	if err != nil {
		return err
	}

	// Set up reader state.
	glog.V(logging.LogLevelTrace).Infof("Entry: %v", resp.Entry)
	this.entry = resp.Entry
	this.blockIdx = 0

	// Set up initial block state.
	this.blockReader = nil
	this.blockPos = 0
	this.blockBuf = nil

	return nil
}

func (this *LocalFileReader) Read(buffer []byte) (int, error) {
	glog.V(logging.LogLevelTrace).Infof("Read up to %d bytes from %s", len(buffer), this.filename)

	totalRead := 0
	this.readCalls++

	for {
		this.readLoopIters++

		if this.blockReader == nil {
			glog.V(logging.LogLevelTrace).Infof("Opening block reader %d for block %v", this.blockIdx, this.entry.Blocks[this.blockIdx])

			blockEntry := this.entry.Blocks[this.blockIdx]

			readStream, err := this.blockClient.Read(context.Background(), &blockservice.ReadRequest{
				VolumeId:  blockEntry.PvId,
				BlockId:   blockEntry.BlockId,
				ChunkSize: size.MB,
				Position:  0,
			})
			if err != nil {
				return 0, err
			}

			this.blockReader = readStream
			this.blockPos = 0
			this.blockBuf = nil
		}

		if this.blockBuf == nil {
			this.blockPos = 0
			this.receiveCalls++

			glog.V(logging.LogLevelTrace).Info("Receiving new chunk from block service")

			readResp, err := this.blockReader.Recv()

			if err != nil {
				if err != io.EOF {
					return totalRead, err
				}

				glog.V(logging.LogLevelTrace).Info("Detected block EOF")
				if err := this.blockReader.CloseSend(); err != nil {
					return totalRead, err
				}

				this.blockIdx++
				this.blockReader = nil

				if this.blockIdx >= len(this.entry.Blocks) {
					glog.V(logging.LogLevelTrace).Info("Detected file EOF")
					return totalRead, io.EOF
				} else {
					continue
				}
			}

			this.blockBuf = readResp.Buffer
		}

		glog.V(logging.LogLevelTrace).Infof("Read buffer: %d/%d, Block buffer %d/%d",
			totalRead, len(buffer), this.blockPos, len(this.blockBuf))

		readMax := len(buffer[totalRead:])

		if readMax > len(this.blockBuf[this.blockPos:]) {
			readMax = len(this.blockBuf[this.blockPos:])
		}

		if readMax > 0 {
			if readMax < this.minReadSize || this.readCalls == 0 {
				this.minReadSize = readMax
			}

			amountRead := copy(buffer[totalRead:totalRead+readMax], this.blockBuf[this.blockPos:this.blockPos+readMax])
			this.blockPos += amountRead
			totalRead += amountRead
		} else {
			if this.blockPos >= len(this.blockBuf) {
				glog.V(logging.LogLevelTrace).Info("Exhausted block buffer")
				this.blockBuf = nil
			} else {
				glog.V(logging.LogLevelTrace).Info("Exhausted read buffer")
				break
			}
		}
	}

	return totalRead, nil
}

func (this *LocalFileReader) Close() error {
	glog.V(logging.LogLevelDebug).Infof("Closing reader for %s", this.filename)

	if glog.V(logging.LogLevelTrace) {
		blockCount := len(this.entry.Blocks)

		glog.Infof(
			"Performance for %s: %f reads/block, %f receives/block - readCalls: %d receiveCalls: %d blocks: %d minReadSize: %d readLoopIters: %d",
			this.filename,
			float64(this.readCalls)/float64(blockCount),
			float64(this.receiveCalls)/float64(blockCount),
			this.readCalls,
			this.receiveCalls,
			blockCount,
			this.minReadSize,
			this.readLoopIters,
		)
	}

	if this.blockReader != nil {
		return this.blockReader.CloseSend()
	}

	return nil
}
