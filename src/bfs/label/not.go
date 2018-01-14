package label

import (
	"bfs/config"
	"github.com/golang/glog"
)

type NotPredicate struct {
	Predicate Predicate
}

func (this *NotPredicate) Evaluate(label *config.Label) bool {
	glog.V(2).Infof("Evaluate not expression: %#v", this.Predicate)
	return !this.Predicate.Evaluate(label)
}
