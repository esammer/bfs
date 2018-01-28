// An etcd-based Namespace
package etcd

import (
	"bfs/ns"
	"bfs/util/fsm"
	"bfs/util/logging"
	"context"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/golang/glog"
	"io"
	"net/url"
	"time"
)

const (
	StateInitial = "INITIAL"
	StateOpen    = "OPEN"
	StateClosed  = "CLOSED"
	StateError   = "ERROR"
)

var stateFSM = fsm.New(StateInitial).
	Allow(StateInitial, StateOpen).
	Allow(StateInitial, StateClosed).
	Allow(StateInitial, StateError).
	Allow(StateOpen, StateClosed).
	Allow(StateOpen, StateError).
	Allow(StateError, StateClosed)

type EtcdNamespace struct {
	config *Config

	fsm        *fsm.FSMInstance
	etcdConfig *embed.Config
	etcd       *embed.Etcd
	client     *clientv3.Client
}

// A namespace node.
type NsNode struct {
	Id         string
	Hostname   string
	ClientPort int
	PeerPort   int
}

// Returns the etcd client endpoint for this node.
func (this *NsNode) ClientEndpoint() string {
	return fmt.Sprintf("http://%s:%d", this.Hostname, this.ClientPort)
}

// Returns the etcd peer endpoint for this node.
func (this *NsNode) PeerEndpoint() string {
	return fmt.Sprintf("http://%s:%d", this.Hostname, this.PeerPort)
}

// Returns the etcd bootstrap entry. (e.g. id1=http://localhost:2380)
func (this *NsNode) BootstrapEndpoint() string {
	return fmt.Sprintf("%s=%s", this.Id, this.PeerEndpoint())
}

// An EtcdNamespace configuration.
type Config struct {
	Path    string
	GroupId string
	Self    int
	Nodes   []*NsNode
}

// Returns the etcd bootstrap entries. (e.g. id1=url1,id2=url2)
func (this *Config) BootstrapCluster() string {
	var s string

	for i, node := range this.Nodes {
		if i > 0 {
			s += ","
		}

		s += node.BootstrapEndpoint()
	}

	return s
}

// Returns the etcd client endpoint list.
func (this *Config) ClientEndpoints() []string {
	endpoints := make([]string, len(this.Nodes))
	for i, node := range this.Nodes {
		endpoints[i] = node.ClientEndpoint()
	}

	return endpoints
}

func New(config *Config) *EtcdNamespace {

	this := &EtcdNamespace{
		config: config,
		fsm:    stateFSM.NewInstance(),
	}

	selfNode := config.Nodes[config.Self]

	etcdConfig := embed.NewConfig()
	etcdConfig.Dir = config.Path
	etcdConfig.Name = selfNode.Id
	etcdConfig.InitialClusterToken = config.GroupId
	etcdConfig.InitialCluster = config.BootstrapCluster()

	clientUrl, err := url.Parse(selfNode.ClientEndpoint())
	if err != nil {
		return nil
	}

	peerUrl, err := url.Parse(selfNode.PeerEndpoint())
	if err != nil {
		return nil
	}

	etcdConfig.LCUrls = []url.URL{*clientUrl}
	etcdConfig.ACUrls = []url.URL{*clientUrl}
	etcdConfig.LPUrls = []url.URL{*peerUrl}
	etcdConfig.APUrls = []url.URL{*peerUrl}

	this.etcdConfig = etcdConfig

	return this
}

func (this *EtcdNamespace) Open() error {
	glog.V(logging.LogLevelDebug).Infof("Opening namespace at %s config:%+v", this.config.Path, this.config)

	if err := this.fsm.Is(StateInitial); err != nil {
		return err
	}

	if err := this.etcdConfig.Validate(); err != nil {
		this.fsm.To(StateError)
		return err
	}

	etcd, err := embed.StartEtcd(this.etcdConfig)
	if err != nil {
		this.fsm.To(StateError)
		return err
	}

	<-etcd.Server.ReadyNotify()

	this.etcd = etcd

	endpoints := this.config.ClientEndpoints()

	glog.V(logging.LogLevelDebug).Infof("Creating namespace self-client with endpoint: %v", endpoints)
	this.client, err = clientv3.New(clientv3.Config{Endpoints: endpoints})
	if err != nil {
		this.fsm.To(StateError)
		return err
	}
	glog.V(logging.LogLevelDebug).Info("Created namespace self-client")

	glog.V(logging.LogLevelDebug).Infof("Opened namespace at %s", this.config.Path)

	return this.fsm.To(StateOpen)
}

func (this *EtcdNamespace) Add(entry *ns.Entry) error {
	glog.V(logging.LogLevelTrace).Infof("Add entry for path: %s", entry.Path)

	if err := this.fsm.Is(StateOpen); err != nil {
		return err
	}

	jsonEntry, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	this.client.Put(context.Background(), entry.Path, string(jsonEntry))

	return nil
}

func (this *EtcdNamespace) Get(path string) (*ns.Entry, error) {
	glog.V(logging.LogLevelTrace).Infof("Get entry for path: %s", path)

	if err := this.fsm.Is(StateOpen); err != nil {
		return nil, err
	}

	resp, err := this.client.Get(context.Background(), path)
	if err != nil {
		return nil, err
	}

	if resp.Count == 0 {
		return nil, fmt.Errorf("no such entry %s", path)
	}

	entry := &ns.Entry{}
	err = json.Unmarshal(resp.Kvs[0].Value, entry)
	if err != nil {
		return nil, err
	}

	return entry, nil
}

func (this *EtcdNamespace) List(prefix string, visitor func(*ns.Entry, error) (bool, error)) error {
	if err := this.fsm.Is(StateOpen); err != nil {
		return err
	}

	getKey := prefix

	for {
		glog.V(logging.LogLevelDebug).Infof("List from key %s", getKey)

		getResp, err := this.client.Get(context.Background(), getKey, clientv3.WithPrefix())
		if err != nil {
			return err
		}

		for _, kv := range getResp.Kvs {
			entry := &ns.Entry{}
			if err := json.Unmarshal(kv.Value, entry); err != nil {
				if ok, err := visitor(nil, err); !ok {
					return err
				}
			}

			visitor(entry, nil)
			getKey = entry.Path
		}

		if !getResp.More {
			glog.V(logging.LogLevelDebug).Info("No more entries to get")
			break
		}
	}

	visitor(nil, io.EOF)

	return nil
}

func (this *EtcdNamespace) Remove(path string, recursive bool) (int, error) {
	glog.V(logging.LogLevelTrace).Infof("Deleting path: %s recursive: %t", path, recursive)

	var ops []clientv3.OpOption
	if recursive {
		ops = []clientv3.OpOption{clientv3.WithPrefix()}
	}

	glog.V(logging.LogLevelTrace).Infof("Ops: %v", ops)

	resp, err := this.client.Delete(context.Background(), path, ops...)
	if err != nil {
		return 0, err
	}

	return int(resp.Deleted), nil
}

func (this *EtcdNamespace) Rename(source string, dest string) error {
	glog.V(logging.LogLevelTrace).Infof("Rename source: %s to dest: %s", source, dest)

	// This purposefully doesn't use this.Get() because we need access to the raw bytes in the response.
	getResp, err := this.client.Get(context.Background(), source)
	if err != nil {
		return err
	}

	if getResp.Count == 0 {
		return fmt.Errorf("unable to rename %s to %s - %s does not exist", source, dest, source)
	} else if getResp.Count > 1 {
		return fmt.Errorf("unable to rename %s to %s - %s produced %d results", source, dest, source, getResp.Count)
	}

	kv := getResp.Kvs[0]
	entry := &ns.Entry{}
	err = json.Unmarshal(kv.Value, entry)
	if err != nil {
		return err
	}

	// Update the path and mtime. We preserve ctime.
	entry.Path = dest
	entry.Mtime = time.Now()

	jsonEntry, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	// Atomically put the new entry and delete the old entry.
	tx := this.client.Txn(context.Background())
	txResp, err := tx.If(
		// There's no etcd compare function for just the key so we compare the full value.
		clientv3.Compare(clientv3.Value(source), "=", string(kv.Value)),
	).Then(
		clientv3.OpPut(dest, string(jsonEntry)),
		clientv3.OpDelete(source),
	).Commit()
	if err != nil {
		return err
	}

	// If txResp.Succeeded is false, the tx.If() condition failed to match.
	if !txResp.Succeeded {
		return fmt.Errorf("unable to rename %s to %s - %s does not exist", source, dest, source)
	}

	return nil
}

func (this *EtcdNamespace) Close() error {
	glog.V(logging.LogLevelDebug).Infof("Closing namespace at %s", this.config.Path)

	if err := this.fsm.IsOneOf(StateOpen, StateError); err != nil {
		return err
	}

	if this.client != nil {
		glog.V(logging.LogLevelDebug).Info("Closing namespace self-client")
		// Warn and continue if we can't close the client. This might leak resources.
		if err := this.client.Close(); err != nil {
			glog.Warningf("Unable to close namespace client - %v", err)
			this.client = nil
		}
		glog.V(logging.LogLevelDebug).Info("Closed namespace self-client")
	}

	if this.etcd != nil {
		this.etcd.Close()
	}

	glog.V(logging.LogLevelDebug).Infof("Closed namespace at %s", this.config.Path)

	return this.fsm.To(StateClosed)
}
