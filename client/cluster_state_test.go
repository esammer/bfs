package client

import (
	"bfs/config"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestClusterState(t *testing.T) {
	clusterState := NewClusterState()

	clusterState.SetHostConfigs(
		map[string]*config.HostConfig{
			"host1": {
				Id:       "host1",
				Hostname: "hostname1",
				BlockServiceConfig: &config.BlockServiceConfig{
					BindAddress: "localhost:60000",
					VolumeConfigs: []*config.PhysicalVolumeConfig{
						{
							Id:   "pv1",
							Path: "data/host1/pv1",
						},
						{
							Id:   "pv2",
							Path: "data/host1/pv2",
						},
					},
				},
			},
			"host2": {
				Id:       "host2",
				Hostname: "hostname2",
				BlockServiceConfig: &config.BlockServiceConfig{
					BindAddress: "localhost:60000",
					VolumeConfigs: []*config.PhysicalVolumeConfig{
						{
							Id:   "pv3",
							Path: "data/host2/pv3",
						},
						{
							Id:   "pv4",
							Path: "data/host2/pv4",
						},
					},
				},
			},
		},
	)

	clusterState.SetHostStatus(
		map[string]*config.HostStatus{
			"host1": {
				Id:        "host1",
				FirstSeen: 0,
				LastSeen:  1,
				VolumeStatus: map[string]*config.PhysicalVolumeStatus{
					"pv1": {Id: "pv1", Path: "data/host1/pv1"},
					"pv2": {Id: "pv2", Path: "data/host1/pv2"},
				},
			},
			"host2": {
				Id:        "host2",
				FirstSeen: 3,
				LastSeen:  4,
				VolumeStatus: map[string]*config.PhysicalVolumeStatus{
					"pv3": {Id: "pv3", Path: "data/host2/pv3"},
					"pv4": {Id: "pv4", Path: "data/host2/pv4"},
				},
			},
		},
	)

	t.Run("HostId", func(t *testing.T) {
		require.Equal(t, "host1", clusterState.HostId("hostname1"))
		require.Equal(t, "host2", clusterState.HostId("hostname2"))
	})

	t.Run("Host", func(t *testing.T) {
		hostConfig, hostStatus := clusterState.Host("host1")

		require.Equal(t, &config.HostConfig{
			Id:       "host1",
			Hostname: "hostname1",
			BlockServiceConfig: &config.BlockServiceConfig{
				BindAddress: "localhost:60000",
				VolumeConfigs: []*config.PhysicalVolumeConfig{
					{
						Id:   "pv1",
						Path: "data/host1/pv1",
					},
					{
						Id:   "pv2",
						Path: "data/host1/pv2",
					},
				},
			},
		}, hostConfig)
		require.Equal(t, &config.HostStatus{
			Id:        "host1",
			FirstSeen: 0,
			LastSeen:  1,
			VolumeStatus: map[string]*config.PhysicalVolumeStatus{
				"pv1": {Id: "pv1", Path: "data/host1/pv1"},
				"pv2": {Id: "pv2", Path: "data/host1/pv2"},
			},
		}, hostStatus)
	})

	t.Run("PhysicalVolume", func(t *testing.T) {
		pvConfig, pvStatus := clusterState.PhysicalVolume("pv1")
		require.Equal(t, &config.PhysicalVolumeConfig{Id: "pv1", Path: "data/host1/pv1",}, pvConfig)
		require.Equal(t, &config.PhysicalVolumeStatus{Id: "pv1", Path: "data/host1/pv1",}, pvStatus)
		pvConfig, pvStatus = clusterState.PhysicalVolume("pv2")
		require.Equal(t, &config.PhysicalVolumeConfig{Id: "pv2", Path: "data/host1/pv2",}, pvConfig)
		require.Equal(t, &config.PhysicalVolumeStatus{Id: "pv2", Path: "data/host1/pv2",}, pvStatus)
		pvConfig, pvStatus = clusterState.PhysicalVolume("pv3")
		require.Equal(t, &config.PhysicalVolumeConfig{Id: "pv3", Path: "data/host2/pv3",}, pvConfig)
		require.Equal(t, &config.PhysicalVolumeStatus{Id: "pv3", Path: "data/host2/pv3",}, pvStatus)
		pvConfig, pvStatus = clusterState.PhysicalVolume("pv4")
		require.Equal(t, &config.PhysicalVolumeConfig{Id: "pv4", Path: "data/host2/pv4",}, pvConfig)
		require.Equal(t, &config.PhysicalVolumeStatus{Id: "pv4", Path: "data/host2/pv4",}, pvStatus)
	})
}

func BenchmarkClusterState(b *testing.B) {
	clusterState := NewClusterState()

	clusterState.SetHostConfigs(
		map[string]*config.HostConfig{
			"host1": {
				Id:       "host1",
				Hostname: "hostname1",
				BlockServiceConfig: &config.BlockServiceConfig{
					BindAddress: "localhost:60000",
					VolumeConfigs: []*config.PhysicalVolumeConfig{
						{
							Id:   "pv1",
							Path: "data/host1/pv1",
						},
						{
							Id:   "pv2",
							Path: "data/host1/pv2",
						},
					},
				},
			},
			"host2": {
				Id:       "host2",
				Hostname: "hostname2",
				BlockServiceConfig: &config.BlockServiceConfig{
					BindAddress: "localhost:60000",
					VolumeConfigs: []*config.PhysicalVolumeConfig{
						{
							Id:   "pv3",
							Path: "data/host2/pv3",
						},
						{
							Id:   "pv4",
							Path: "data/host2/pv4",
						},
					},
				},
			},
		},
	)

	clusterState.SetHostStatus(
		map[string]*config.HostStatus{
			"host1": {
				Id:        "host1",
				FirstSeen: 0,
				LastSeen:  1,
				VolumeStatus: map[string]*config.PhysicalVolumeStatus{
					"pv1": {Id: "pv1", Path: "data/host1/pv1"},
					"pv2": {Id: "pv2", Path: "data/host1/pv2"},
				},
			},
			"host2": {
				Id:        "host2",
				FirstSeen: 3,
				LastSeen:  4,
				VolumeStatus: map[string]*config.PhysicalVolumeStatus{
					"pv3": {Id: "pv3", Path: "data/host2/pv3"},
					"pv4": {Id: "pv4", Path: "data/host2/pv4"},
				},
			},
		},
	)

	b.Run("PhysicalVolume", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			clusterState.PhysicalVolume("pv1")
		}
	})

	b.Run("PhysicalVolumeForHost", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			clusterState.PhysicalVolumesForHost("host1")
		}
	})
}
