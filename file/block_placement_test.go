package file

import (
	"bfs/config"
	"fmt"
	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLabelAwarePlacementPolicy(t *testing.T) {
	defer glog.Flush()

	pvParams := map[string]int{
		"a": 1,
		"b": 2,
		"c": 3,
		"d": 4,
		"e": 5,
		"f": 6,
		"g": 7,
		"h": 8,
		"i": 9,
	}

	pvConfigs := make([]*config.PhysicalVolumeConfig, 0, 45)

	for hostname, disks := range pvParams {
		for disk := 1; disk <= disks; disk++ {
			pvConfigs = append(pvConfigs, &config.PhysicalVolumeConfig{
				Id:     fmt.Sprintf("%s%d", hostname, disk),
				Labels: []*config.Label{{Key: "hostname", Value: hostname}},
			})
		}
	}

	pp := NewLabelAwarePlacementPolicy(
		pvConfigs,
		"hostname",
		false,
		5,
		3,
		func(node *ValueNode) bool {
			return !(node.Value.Id == "c2" || node.Value.Id == "a1")
		},
	)

	require.NotNil(t, pp)
	require.Equal(t, len(pvParams), pp.ring.Len())

	for i := 0; i < 100; i++ {
		selectedPVs, err := pp.Next()

		require.Len(t, selectedPVs, 5)

		pvs := make([]string, len(selectedPVs))
		for j := 0; j < len(selectedPVs); j++ {
			pvs[j] = selectedPVs[j].Id
		}

		glog.V(2).Infof("Request: %d - Selected: %v - Error: %v", i, pvs, err)
	}
}
