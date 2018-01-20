package selector

import (
	"bfs/config"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEqualsPredicate(t *testing.T) {
	labelA := &config.Label{
		Key:   "a",
		Value: "1",
	}
	labelB := &config.Label{
		Key: "b",
	}

	t.Run("NoMatch", func(t *testing.T) {
		predicate := &EqualsPredicate{Key: "no match", Value: "no match"}
		require.False(t, predicate.Evaluate(labelA))
		require.False(t, predicate.Evaluate(labelB))
	})

	t.Run("KeyOnlyMatch", func(t *testing.T) {
		predicate := &EqualsPredicate{Key: "a", Value: "no match"}
		require.False(t, predicate.Evaluate(labelA))
		require.False(t, predicate.Evaluate(labelB))
		predicate = &EqualsPredicate{Key: "b"}
		require.False(t, predicate.Evaluate(labelA))
		require.True(t, predicate.Evaluate(labelB))
	})

	t.Run("ValueOnlyMatch", func(t *testing.T) {
		predicate := &EqualsPredicate{Key: "no match", Value: "1"}
		require.False(t, predicate.Evaluate(labelA))
		require.False(t, predicate.Evaluate(labelB))
	})

	t.Run("KeyValueMatch", func(t *testing.T) {
		predicate := &EqualsPredicate{Key: "a", Value: "1"}
		require.True(t, predicate.Evaluate(labelA))
		require.False(t, predicate.Evaluate(labelB))
	})
}
