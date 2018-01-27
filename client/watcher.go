package client

import (
	"bfs/util/logging"
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/golang/glog"
	"sync/atomic"
	"time"
)

const retryDelay = time.Second

// A callback function for puts or delete events.
//
// Callbacks may return errors in which case the watcher completes. If keep alive is enabled, a new watcher starts.
type EventFunc func(kv *mvccpb.KeyValue) error

// A callback function to invoke upon watcher completion.
//
// This function is called when a watcher completes for any reason. If there's an error, it is provided to the
// callback. If keep alive is enabled, the watcher is restarted so it is possible that the done function is called
// multiple times.
type DoneFunc func(err error);

// A callback-based etcd watcher support service.
//
// Watchers implement a low level watcher service that monitors a particular etcd key, invoking provided functions upon
// interesting events. Beyond the basic etcd watch support, Watchers re-watch should they become disconnected, and
// optionally perform an initial key get upon start up. Callbacks may be supplied for put, delete, and done operations.
// The put callback is also invoked for the initial get. Prefix watches are fully supported; simply pass a
// clientv3.WithPrefix() option.
//
// If an initial get is requested, the watch begins with revision N+1 where N is the revision from the get response. As
// a result, the callbacks will see the current state followed by any changes thereto. That is, clients will receive
// exactly once delivery of changes making Watchers appropriate for implementing client-side caches of etcd state.
type Watcher struct {
	client     *clientv3.Client
	key        string
	initialGet bool
	putFunc    EventFunc
	deleteFunc EventFunc
	doneFunc   DoneFunc
	keepAlive  bool
	watchOps   []clientv3.OpOption

	shouldRun       atomic.Value
	doneChan        chan bool
	watchCancelFunc context.CancelFunc
}

func NewWatcher(client *clientv3.Client, key string, initialGet bool, putFunc EventFunc,
	deleteFunc EventFunc, doneFunc DoneFunc, keepAlive bool, watchOps ...clientv3.OpOption) *Watcher {

	shouldRun := atomic.Value{}
	shouldRun.Store(true)

	return &Watcher{
		client:     client,
		key:        key,
		initialGet: initialGet,
		putFunc:    putFunc,
		deleteFunc: deleteFunc,
		doneFunc:   doneFunc,
		keepAlive:  keepAlive,
		watchOps:   watchOps,

		shouldRun: shouldRun,
		doneChan:  make(chan bool),
	}
}

func (this *Watcher) Start() error {
	glog.V(logging.LogLevelTrace).Infof("Starting watcher for %s", this.key)

	var rev int64

	if this.initialGet {
		glog.V(logging.LogLevelTrace).Infof("Doing initial get of %s", this.key)

		resp, err := this.client.Get(context.Background(), this.key, this.watchOps...)
		if err != nil {
			return err
		}

		for _, kv := range resp.Kvs {
			if err := this.putFunc(kv); err != nil {
				return err
			}
		}

		rev = resp.Header.Revision
		glog.V(logging.LogLevelTrace).Infof("Finished initial get of %s @ %d", this.key, rev)

		rev++
	}

	go func() {
		for this.shouldRun.Load() == true {
			glog.V(logging.LogLevelTrace).Infof("Starting watcher process for %s @ %d", this.key, rev)

			ctx, cancelFunc := context.WithCancel(context.Background())
			this.watchCancelFunc = cancelFunc

			ops := make([]clientv3.OpOption, 0, len(this.watchOps)+1)
			for _, op := range this.watchOps {
				ops = append(ops, op)
			}

			if this.initialGet {
				ops = append(ops, clientv3.WithRev(rev))
			}

			watchChan := this.client.Watch(ctx, this.key, ops...)

			for watchEvent := range watchChan {
				rev = watchEvent.Header.Revision

				if watchEvent.Canceled {
					if this.doneFunc != nil {
						this.doneFunc(watchEvent.Err())
					}
					time.Sleep(retryDelay)
					break
				}

				var err error

			eventLoop:
				for _, event := range watchEvent.Events {
					switch event.Type {
					case mvccpb.PUT:
						if this.putFunc != nil {
							if err = this.putFunc(event.Kv); err != nil {
								break eventLoop
							}
						}
					case mvccpb.DELETE:
						if this.deleteFunc != nil {
							if err = this.deleteFunc(event.Kv); err != nil {
								break eventLoop
							}
						}
					}
				}

				if err != nil {
					if this.doneFunc != nil {
						this.doneFunc(err)
					}
					time.Sleep(retryDelay)
					break
				}
			}

			if !this.keepAlive {
				break
			}
		}

		glog.V(logging.LogLevelTrace).Infof("Finished watcher process for %s", this.key)
		this.doneChan <- true
	}()

	return nil
}

func (this *Watcher) Stop() {
	glog.V(logging.LogLevelTrace).Infof("Stopping watcher for %s", this.key)

	this.shouldRun.Store(false)

	if this.watchCancelFunc != nil {
		this.watchCancelFunc()
	}

	<-this.doneChan

	glog.V(logging.LogLevelTrace).Infof("Stopped watcher for %s", this.key)
}
