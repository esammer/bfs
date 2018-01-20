package volumeutil

type VolumeState int

const (
	VolumeState_Initial VolumeState = iota
	VolumeState_Closed
	VolumeState_Open
)

var volumeStateStr = []string{
	"INITIAL",
	"CLOSED",
	"OPEN",
}

func (this *VolumeState) String() string {
	return volumeStateStr[*this]
}
