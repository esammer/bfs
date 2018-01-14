package label

import (
	"bfs/config"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestInPredicate(t *testing.T) {
	labelA := &config.Label{
		Key:   "a",
		Value: "1",
	}
	labelB := &config.Label{
		Key: "b",
	}

	t.Run("NoMatch", func(t *testing.T) {
		predicate := &InPredicate{Key: "no match", Values: []string{"no", "match"}}
		require.False(t, predicate.Evaluate(labelA))
		require.False(t, predicate.Evaluate(labelB))
	})

	t.Run("KeyOnlyMatch", func(t *testing.T) {
		predicate := &InPredicate{Key: "a", Values: []string{"no", "match"}}
		require.False(t, predicate.Evaluate(labelA))
		require.False(t, predicate.Evaluate(labelB))
	})

	t.Run("KeyValueMatch", func(t *testing.T) {
		predicate := &InPredicate{Key: "a", Values: []string{"2", "1"}}
		require.True(t, predicate.Evaluate(labelA))
		require.False(t, predicate.Evaluate(labelB))
	})
}
