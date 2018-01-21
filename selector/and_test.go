package selector

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAndPredicate(t *testing.T) {
	labels := map[string]string{
		"a": "1",
		"b": "",
	}

	t.Run("EmptyPredicates", func(t *testing.T) {
		selector := &AndPredicate{}
		require.True(t, selector.Evaluate("a", labels["a"]))
		require.True(t, selector.Evaluate("b", labels["b"]))
	})

	t.Run("NoMatch", func(t *testing.T) {
		selector := &AndPredicate{
			Predicates: []Predicate{
				&EqualsPredicate{Key: "no match"},
			},
		}

		require.False(t, selector.Evaluate("a", labels["a"]))
		require.False(t, selector.Evaluate("b", labels["b"]))
	})

	t.Run("OneMatch", func(t *testing.T) {
		predicate := &AndPredicate{
			Predicates: []Predicate{
				&EqualsPredicate{Key: "a", Value: "1"},
				&EqualsPredicate{Key: "no match"},
			},
		}

		require.False(t, predicate.Evaluate("a", labels["a"]))
		require.False(t, predicate.Evaluate("b", labels["b"]))
	})

	t.Run("Match", func(t *testing.T) {
		predicate := &AndPredicate{
			Predicates: []Predicate{
				&EqualsPredicate{Key: "a", Value: "1"},
				&NotPredicate{Predicate: &EqualsPredicate{Key: "b"}},
			},
		}

		require.True(t, predicate.Evaluate("a", labels["a"]))
		require.False(t, predicate.Evaluate("b", labels["b"]))
	})
}
