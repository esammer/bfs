package file

import (
	"bfs/config"
	"container/ring"
	"fmt"
	"github.com/golang/glog"
)

// A policy plugin dictating the placement of blocks.
//
// Implementations of this interface dictate how blocks are mapped to physical volumes. Callers invoke this method as
// block locations are required, and implementations return the next best locations for those blocks.
type BlockPlacementPolicy interface {
	// Returns the next set of ValueNodes that should be used for block placement. Each entry in the returned slice is
	// for a replica. An error is returned if the required number of replicas can not be satisfied. Implementations may
	// specify additional conditions and constraints on the returned values.
	Next() ([]*config.PhysicalVolumeConfig, error)
}

// Distribute replicas across PVs with different values for a given label.
//
// This policy returns PVs that are guaranteed to NOT share a value for a
// given label. The intention is that administrators label PVs such that
// replicas are, for example, never placed on the same host, rack, or zone.
type LabelAwarePlacementPolicy struct {
	// The label name or key by which to distribute.
	LabelName string

	// If true, PVs missing the specified label will be treated as a different
	// label value. Set to false to force all selected PVs to have the label
	// in addition to having a distinct value.
	AllowMissing bool

	// The desired number of replicas.
	Replicas int

	// The minimum number of required replicas.
	MinimumReplicas int

	// Optional function used to approve a selection.
	AcceptanceFunc func(node *ValueNode) bool

	ring *ring.Ring
}

type ValueNode struct {
	Value      *config.PhysicalVolumeConfig
	LabelValue string
}

func NewLabelAwarePlacementPolicy(volumeConfigs []*config.PhysicalVolumeConfig, labelName string, allowMissing bool,
	replicas int, minimumReplicas int, acceptFunc func(*ValueNode) bool) *LabelAwarePlacementPolicy {

	this := &LabelAwarePlacementPolicy{
		LabelName:       labelName,
		AllowMissing:    allowMissing,
		Replicas:        replicas,
		MinimumReplicas: minimumReplicas,
		AcceptanceFunc:  acceptFunc,
	}

	/*
	 * Build a ring of rings. Each inner ring (a "value" ring) contains a *ValueNode holding both a label value and a
	 * physical volume config that contains the given value. The outer ring (the "root" ring) is a ring of the value
	 * rings. Ultimately, the Next() method iterates through each entry column-wise, then row-wise.
	 *
	 * Given:
	 * [
	 *   [ {L1, C1}, {L1, C2}, {L1,C3} ],
	 *   [ {L2, C4}, {L2, C5}, {L1,C6} ],
	 *   [ {L3, C7}, {L3, C8} ],
	 * ]
	 *
	 * Where Lx is label value x and Cy is PV config y, and a Replica value of 2, Next() will produce:
	 *
	 * Next() call 1: [ {L1, C1}, {L2,C4} ]
	 * Next() call 2: [ {L3, C7}, {L1,C2} ]
	 * Next() call 3: [ {L2, C5}, {L3,C8} ]
	 * Next() call 4: [ {L1, C3}, {L2,C6} ]
	 * Next() call 5: [ {L3, C7}, {L1,C1} ] <-- Note L3,C7 and L1,C1 loop.
	 * Next() call 6: [ {L2, C4}, {L3,C8} ] <-- Note L2,C4 loop.
	 */
	ringIndex := make(map[string]*ring.Ring, 16)

	for _, pvConfig := range volumeConfigs {
		labelValue := ""
		for _, label := range pvConfig.Labels {
			if label.Key == this.LabelName {
				labelValue = label.Value
				break
			}
		}

		if labelValue == "" && !this.AllowMissing {
			continue
		}

		newNode := &ring.Ring{Value: &ValueNode{LabelValue: labelValue, Value: pvConfig}}

		if val, ok := ringIndex[labelValue]; !ok {
			ringIndex[labelValue] = newNode

			if this.ring == nil {
				glog.V(2).Infof("Add root ring - %v", pvConfig)
				this.ring = &ring.Ring{Value: newNode}
			} else {
				glog.V(2).Infof("Append root ring current %v - %v", this.ring.Value.(*ring.Ring).Value, pvConfig)
				oldRoot := this.ring
				this.ring.Link(&ring.Ring{Value: newNode})
				this.ring = oldRoot.Next()
			}
		} else {
			glog.V(2).Infof("Append entry current %v - %v", val.Value, pvConfig)
			val.Link(newNode).Next()
			ringIndex[labelValue] = newNode
		}
	}

	this.ring = this.ring.Next()

	return this
}

func (this *LabelAwarePlacementPolicy) Next() ([]*config.PhysicalVolumeConfig, error) {
	glog.V(3).Infof("Next replica set for %s (desired replicas: %d, min replicas: %d)",
		this.LabelName, this.Replicas, this.MinimumReplicas)

	selectedPVs := make([]*config.PhysicalVolumeConfig, 0, this.MinimumReplicas)
	selectedValues := make(map[string]bool, this.MinimumReplicas)

	firstPV := ""

	for i := 1; i <= this.Replicas; {
		rootNode := this.ring
		valueNode := rootNode.Value.(*ring.Ring)

		value := valueNode.Value.(*ValueNode)

		glog.V(3).Infof("Replica %d/%d potential selection: %s pv: %s",
			i, this.Replicas, value.LabelValue, value.Value.Id)

		if firstPV == "" {
			firstPV = value.Value.Id
		} else if firstPV == value.Value.Id {
			// Looped the root ring. Exhausted options.
			glog.V(3).Infof("Seen %s before - exhausted everything", firstPV)
			break
		}

		if this.AcceptanceFunc != nil && !this.AcceptanceFunc(value) {
			glog.V(3).Infof("Replica %d/%d selection disallowed by accept func", i, this.Replicas)

			if valueNode == valueNode.Next() {
				// Special case: If the label value only has one element in the ring, it will loop forever. Advance root.
				if this.ring == this.ring.Next() {
					// Special case: If the root value only has one element in the ring, it will loop forever. Give up.
					break
				}

				this.ring = rootNode.Next()
			} else {
				rootNode.Value = valueNode.Next()
			}
		} else {
			if _, ok := selectedValues[value.LabelValue]; !ok {
				glog.V(3).Infof("Replica %d/%d selection: %s pv: %s",
					i, this.Replicas, value.LabelValue, value.Value.Id)

				selectedPVs = append(selectedPVs, value.Value)
				selectedValues[value.LabelValue] = true
				i++
			}

			rootNode.Value = valueNode.Next()
			this.ring = rootNode.Next()
		}
	}

	var err error
	if len(selectedPVs) < this.MinimumReplicas {
		err = fmt.Errorf("too few replicas - %d found, wanted %d, minimum %d",
			len(selectedPVs), this.Replicas, this.MinimumReplicas)
	}

	glog.V(3).Infof("Selected %d/%d replicas (needed at least %d)",
		len(selectedPVs), this.Replicas, this.MinimumReplicas)

	return selectedPVs, err
}
