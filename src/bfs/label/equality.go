package label

import (
	"bfs/config"
	"github.com/golang/glog"
)

type EqualsPredicate struct {
	Key   string
	Value string
}

func (this *EqualsPredicate) Evaluate(label *config.Label) bool {
	glog.V(2).Infof("Evaluate equality expression: %s %#v against label: %v", this.Key, this.Value, label)

	if this.Key == label.Key {
		if this.Value == "" || this.Value == label.Value {
			return true
		}
	}

	return false
}
