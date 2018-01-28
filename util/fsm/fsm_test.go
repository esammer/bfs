package fsm

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFSM_Linear(t *testing.T) {
	fsm := New("a")
	fsm.Allow("a", "b")
	fsm.Allow("b", "c")

	require.Equal(t, "a", fsm.initial)
	require.Equal(t, map[interface{}]map[interface{}]bool{
		"a": {"b": true},
		"b": {"c": true},
	}, fsm.transitions)

	t.Run("Instance", func(t *testing.T) {
		inst := fsm.NewInstance()
		require.NotNil(t, inst)

		require.Equal(t, "a", inst.state)
		require.NoError(t, inst.Is("a"))
		require.True(t, inst.Can("b"))

		require.NoError(t, inst.To("b"))
		require.NoError(t, inst.To("c"))

		err := inst.To("d")
		require.Error(t, err)
		transErr, ok := err.(*TransitionErr)
		require.True(t, ok)
		require.Equal(t, "c", transErr.From)
		require.Equal(t, "d", transErr.To)
		require.Equal(t, []interface{}{}, transErr.ValidTransitions)
	})
}

func TestFSM_Branch(t *testing.T) {
	fsm := New("a")
	fsm.Allow("a", "b")
	fsm.Allow("a", "c")
	fsm.Allow("b", "c")
	fsm.Allow("b", "d")
	fsm.Allow("b", "d")
	fsm.Allow("c", "d")

	require.Equal(t, "a", fsm.initial)
	require.Equal(t, map[interface{}]map[interface{}]bool{
		"a": {"b": true, "c": true},
		"b": {"c": true, "d": true},
		"c": {"d": true},
	}, fsm.transitions)

	t.Run("Instance", func(t *testing.T) {
		instA := fsm.NewInstance()
		require.NotNil(t, instA)

		require.Equal(t, "a", instA.state)
		require.NoError(t, instA.Is("a"))
		require.True(t, instA.Can("b"))

		instB := instA.Clone()

		// Check that instA goes from a -> b, b -> c, c -> d.
		require.NoError(t, instA.To("b"))
		require.NoError(t, instA.To("c"))
		require.NoError(t, instA.To("d"))

		// Check that instB is a proper clone and also goes from a -> c, c -> d,
		require.NoError(t, instB.To("c"))
		require.NoError(t, instB.To("d"))

		// instA, now at d, fails because d !> d
		err := instA.To("d")
		require.Error(t, err)
		transErr, ok := err.(*TransitionErr)
		require.True(t, ok)
		require.Equal(t, "d", transErr.From)
		require.Equal(t, "d", transErr.To)
		require.Equal(t, []interface{}{}, transErr.ValidTransitions)

		// instB, now at d, fails because d !> d
		err = instB.To("d")
		require.Error(t, err)
		transErr, ok = err.(*TransitionErr)
		require.True(t, ok)
		require.Equal(t, "d", transErr.From)
		require.Equal(t, "d", transErr.To)
		require.Equal(t, []interface{}{}, transErr.ValidTransitions)
	})
}

func TestFSM_LinearCycle(t *testing.T) {
	fsm := New("a")
	fsm.Allow("a", "b").
		Allow("b", "c").
		Allow("c", "a").
		Allow("c", "d")

	require.Equal(t, "a", fsm.initial)
	require.Equal(t, map[interface{}]map[interface{}]bool{
		"a": {"b": true},
		"b": {"c": true},
		"c": {"a": true, "d": true},
	}, fsm.transitions)

	t.Run("Instance", func(t *testing.T) {
		inst := fsm.NewInstance()
		require.True(t, inst.Can("b"))
		require.NoError(t, inst.To("b"))
		require.True(t, inst.Can("c"))
		require.NoError(t, inst.To("c"))
		require.True(t, inst.Can("a"))
		require.NoError(t, inst.To("a"))
		require.True(t, inst.Can("b"))
		require.NoError(t, inst.To("b"))
		require.True(t, inst.Can("c"))
		require.NoError(t, inst.To("c"))
		require.True(t, inst.Can("d"))
		require.NoError(t, inst.To("d"))
		require.False(t, inst.Can("a"))
	})
}

func TestFSMNew_InvalidInitialState(t *testing.T) {
	defer func() {
		err := recover()

		assert.NotNil(t, err)
	}()

	fsm := New("a")

	// This should panic because `a` isn't a valid state.
	fsm.NewInstance()
}
