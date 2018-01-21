package selector

import (
	"github.com/golang/glog"
)

type NotPredicate struct {
	Predicate Predicate
}

func (this *NotPredicate) Evaluate(key string, value string) bool {
	glog.V(2).Infof("Evaluate not expression: %#v", this.Predicate)
	return !this.Predicate.Evaluate(key, value)
}
