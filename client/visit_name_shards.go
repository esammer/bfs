package client

import (
	"bfs/nameservice"
	"bfs/util"
	"bfs/util/logging"
	"fmt"
	"github.com/golang/glog"
)

// A name shard visitor callback.
//
// This visitor is invoked for each name service in the configured cluster. Visitors can return true if they wish to
// continue being called for more shards or false to terminate early. If the visitor returns an error, no additional
// shards will be visited; the bool argument is ignored when an error is present.
type ShardVisitor func(name string, conn nameservice.NameServiceClient) (bool, error)

// Visit each name shard with the given visitor function.
//
// See ShardVisitor for more information.
func (this *Client) VisitNameShards(visitor ShardVisitor) error {
	for _, hostConfig := range this.clusterState.HostConfigs() {
		glog.V(logging.LogLevelTrace).Infof("Visit shard %s with %v", hostConfig.Hostname, visitor)

		connectionAddress := fmt.Sprintf("%s:%d", hostConfig.NameServiceConfig.Hostname, hostConfig.NameServiceConfig.Port)

		o, err := this.clientLRU.Get(connectionAddress)
		if err != nil {
			return err
		}

		conn := o.(*util.ServiceCtx)

		keepGoing, err := visitor(hostConfig.Hostname, conn.NameServiceClient)
		if !keepGoing || err != nil {
			glog.V(logging.LogLevelTrace).Infof("Visit terminating early - continue: %t err: %v", keepGoing, err)
			return err
		}
	}

	return nil
}
