package label

import (
	"bfs/config"
	"github.com/golang/glog"
)

type AndPredicate struct {
	Predicates []Predicate
}

func (this *AndPredicate) Evaluate(label *config.Label) bool {
	glog.V(2).Infof("Evaluate and expression: %#v", this.Predicates)

	for _, predicate := range this.Predicates {
		if !predicate.Evaluate(label) {
			glog.V(2).Infof("Label %#v doesn't match", label)
			return false
		}
	}

	glog.V(2).Infof("Label %#v matches", label)
	return true
}
