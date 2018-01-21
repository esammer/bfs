package selector

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEqualsPredicate(t *testing.T) {
	labels := map[string]string{
		"a": "1",
		"b": "",
	}

	t.Run("NoMatch", func(t *testing.T) {
		predicate := &EqualsPredicate{Key: "no match", Value: "no match"}
		require.False(t, predicate.Evaluate("a", labels["a"]))
		require.False(t, predicate.Evaluate("b", labels["b"]))
	})

	t.Run("KeyOnlyMatch", func(t *testing.T) {
		predicate := &EqualsPredicate{Key: "a", Value: "no match"}
		require.False(t, predicate.Evaluate("a", labels["a"]))
		require.False(t, predicate.Evaluate("b", labels["b"]))
		predicate = &EqualsPredicate{Key: "b"}
		require.False(t, predicate.Evaluate("a", labels["a"]))
		require.True(t, predicate.Evaluate("b", labels["b"]))
	})

	t.Run("ValueOnlyMatch", func(t *testing.T) {
		predicate := &EqualsPredicate{Key: "no match", Value: "1"}
		require.False(t, predicate.Evaluate("a", labels["a"]))
		require.False(t, predicate.Evaluate("b", labels["b"]))
	})

	t.Run("KeyValueMatch", func(t *testing.T) {
		predicate := &EqualsPredicate{Key: "a", Value: "1"}
		require.True(t, predicate.Evaluate("a", labels["a"]))
		require.False(t, predicate.Evaluate("b", labels["b"]))
	})
}
