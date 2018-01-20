package selector

import (
	"bfs/config"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAndPredicate(t *testing.T) {
	labelA := &config.Label{
		Key:   "a",
		Value: "1",
	}
	labelB := &config.Label{
		Key: "b",
	}

	t.Run("EmptyPredicates", func(t *testing.T) {
		selector := &AndPredicate{}
		require.True(t, selector.Evaluate(labelA))
		require.True(t, selector.Evaluate(labelB))
	})

	t.Run("NoMatch", func(t *testing.T) {
		selector := &AndPredicate{
			Predicates: []Predicate{
				&EqualsPredicate{Key: "no match"},
			},
		}

		require.False(t, selector.Evaluate(labelA))
		require.False(t, selector.Evaluate(labelB))
	})

	t.Run("OneMatch", func(t *testing.T) {
		predicate := &AndPredicate{
			Predicates: []Predicate{
				&EqualsPredicate{Key: "a", Value: "1"},
				&EqualsPredicate{Key: "no match"},
			},
		}

		require.False(t, predicate.Evaluate(labelA))
		require.False(t, predicate.Evaluate(labelB))
	})

	t.Run("Match", func(t *testing.T) {
		predicate := &AndPredicate{
			Predicates: []Predicate{
				&EqualsPredicate{Key: "a", Value: "1"},
				&NotPredicate{Predicate: &EqualsPredicate{Key: "b"}},
			},
		}

		require.True(t, predicate.Evaluate(labelA))
		require.False(t, predicate.Evaluate(labelB))
	})
}
