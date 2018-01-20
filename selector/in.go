package selector

import (
	"bfs/config"
	"github.com/golang/glog"
)

type InPredicate struct {
	Key    string
	Values []string
}

func (this *InPredicate) Evaluate(label *config.Label) bool {
	glog.V(2).Infof("Evaluate in expression: %s %#v", this.Key, this.Values)

	if this.Key == label.Key {
		for _, value := range this.Values {
			if value == label.Value {
				return true
			}
		}
	}

	return false
}
