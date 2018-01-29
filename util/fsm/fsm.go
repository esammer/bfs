package fsm

import "fmt"

// A finite state machine.
type FSM struct {
	transitions map[interface{}]map[interface{}]bool
	initial     interface{}
}

func New(initial interface{}) *FSM {
	this := &FSM{
		transitions: make(map[interface{}]map[interface{}]bool),
		initial:     initial,
	}

	return this
}

// Returns true if the transition from -> to is valid.
func (this *FSM) Can(from interface{}, to interface{}) bool {
	if toStates, fromExists := this.transitions[from]; fromExists {
		if _, toExists := toStates[to]; toExists {
			return toExists
		}
	}

	return false
}

// Returns a list of the valid transitions from the given from state.
func (this *FSM) ValidTransitions(from interface{}) []interface{} {
	valid := make([]interface{}, 0)

	if toStates, ok := this.transitions[from]; ok {
		for state := range toStates {
			valid = append(valid, state)
		}
	}

	return valid
}

// Add a new legal transition.
func (this *FSM) Allow(from interface{}, to interface{}) *FSM {
	if _, ok := this.transitions[from]; !ok {
		this.transitions[from] = make(map[interface{}]bool)
	}

	this.transitions[from][to] = true
	return this
}

// Create an instance of this FSM starting from the configured initial state.
func (this *FSM) NewInstance() *FSMInstance {
	if _, ok := this.transitions[this.initial]; !ok {
		panic("fsm: improperly programmed FSM - initial state has no transitions")
	}

	return &FSMInstance{
		fsm:   this,
		state: this.initial,
	}
}

// The error produced when the current state is not the expected state.
type StateErr struct {
	Current  interface{}
	Expected []interface{}
}

func (this StateErr) Error() string {
	return fmt.Sprintf("fsm: the current state: %v is not the expected state: %v", this.Current, this.Expected)
}

// The error produced when an illegal transition is attempted.
type TransitionErr struct {
	From             interface{}
	To               interface{}
	ValidTransitions []interface{}
}

func (this TransitionErr) Error() string {
	return fmt.Sprintf("fsm: illegal state transition from %v to %v", this.From, this.To)
}

// An instance of a finite state machine.
type FSMInstance struct {
	fsm   *FSM
	state interface{}
}

// Returns true if it is legal to transition to the given state from the current state.
func (this *FSMInstance) Can(to interface{}) bool {
	return this.fsm.Can(this.state, to)
}

// Transition to the given state.
//
// Returns a TransitionErr if the transition is not allowed.
func (this *FSMInstance) To(to interface{}) error {
	if !this.fsm.Can(this.state, to) {
		return &TransitionErr{
			To:               to,
			From:             this.state,
			ValidTransitions: this.fsm.ValidTransitions(this.state),
		}
	}

	this.state = to
	return nil
}

// Transition to the given state.
//
// Returns a TransitionErr if the transition is not allowed. If the transition is successful, return the
// provided error. This form of transition is a convenience for transitioning in the face of errors where
// one needs to return no matter what.
//
// Ex:
// 	err := SomeFunc()
//
// 	if err != nil {
// 		return fsmInst.ToWithErr("ERROR", err)
// 	}
//
func (this *FSMInstance) ToWithErr(to interface{}, err error) error {
	if err := this.To(to); err != nil {
		return err
	}

	return err
}

// Determines whether the current state is equal to the given state.
//
// Returns a StateErr if the current state is not what is expected.
func (this *FSMInstance) Is(expected interface{}) error {
	if this.state != expected {
		return &StateErr{
			Current:  this.state,
			Expected: []interface{}{expected},
		}
	}

	return nil
}

func (this *FSMInstance) IsOneOf(expected ...interface{}) error {
	for _, possible := range expected {
		if this.state == possible {
			return nil
		}
	}

	return &StateErr{
		Current:  this.state,
		Expected: expected,
	}
}

// Clone the current instance.
func (this *FSMInstance) Clone() *FSMInstance {
	return &FSMInstance{
		fsm:   this.fsm,
		state: this.state,
	}
}

// Returns the FSM behind this instance.
func (this *FSMInstance) FSM() *FSM {
	return this.fsm
}

func (this *FSMInstance) String() string {
	return fmt.Sprintf("{ state: %v, fsm: %+v }", this.state, this.fsm)
}
