package selector

import (
	"bfs/util/logging"
	"github.com/golang/glog"
)

type AndPredicate struct {
	Predicates []Predicate
}

func (this *AndPredicate) Evaluate(key string, value string) bool {
	glog.V(logging.LogLevelTrace).Infof("Evaluate and expression: %#v", this.Predicates)

	for _, predicate := range this.Predicates {
		if !predicate.Evaluate(key, value) {
			glog.V(logging.LogLevelTrace).Infof("Label %s = %s doesn't match", key, value)
			return false
		}
	}

	glog.V(logging.LogLevelTrace).Infof("Label %s = %s matches", key, value)
	return true
}
