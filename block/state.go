package block

import "bfs/util/fsm"

const (
	StateInitial = "INITIAL"
	StateOpen    = "OPEN"
	StateClosed  = "CLOSED"
	StateError   = "ERROR"
)

var readerWriterFSM = fsm.New(StateInitial).
	Allow(StateInitial, StateOpen).
	Allow(StateOpen, StateClosed).
	Allow(StateOpen, StateError).
	Allow(StateError, StateClosed).
	Allow(StateError, StateError)
