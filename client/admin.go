package client

import (
	"bfs/config"
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
	"path/filepath"
)

func (this *Client) CreateLogicalVolume(volumeConfig *config.LogicalVolumeConfig) error {
	_, err := this.etcdClient.Put(
		context.Background(),
		filepath.Join(DefaultEtcdPrefix, EtcdVolumesPrefix, volumeConfig.Id),
		proto.MarshalTextString(volumeConfig),
	)
	if err != nil {
		return err
	}

	return nil
}

func (this *Client) DeleteLogicalVolume(volumeId string) (bool, error) {
	resp, err := this.etcdClient.Delete(
		context.Background(),
		filepath.Join(DefaultEtcdPrefix, EtcdVolumesPrefix, volumeId),
	)

	return resp.Deleted == 1, err
}

func (this *Client) ListVolumes() ([]*config.LogicalVolumeConfig, error) {
	getResp, err := this.etcdClient.Get(
		context.Background(),
		filepath.Join(DefaultEtcdPrefix, EtcdVolumesPrefix),
		clientv3.WithPrefix(),
	)
	if err != nil {
		return nil, err
	}

	lvConfigs := make([]*config.LogicalVolumeConfig, len(getResp.Kvs))

	for i, kv := range getResp.Kvs {
		lvConfig := &config.LogicalVolumeConfig{}
		if err := proto.UnmarshalText(string(kv.Value), lvConfig); err != nil {
			return nil, err
		}

		lvConfigs[i] = lvConfig
	}

	return lvConfigs, nil
}
