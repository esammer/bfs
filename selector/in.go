package selector

import (
	"github.com/golang/glog"
)

type InPredicate struct {
	Key    string
	Values []string
}

func (this *InPredicate) Evaluate(key string, value string) bool {
	glog.V(2).Infof("Evaluate in expression: %s %#v", this.Key, this.Values)

	if this.Key == key {
		for _, v := range this.Values {
			if value == v {
				return true
			}
		}
	}

	return false
}
