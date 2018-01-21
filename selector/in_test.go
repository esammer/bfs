package selector

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestInPredicate(t *testing.T) {
	labels := map[string]string{
		"a": "1",
		"b": "",
	}

	t.Run("NoMatch", func(t *testing.T) {
		predicate := &InPredicate{Key: "no match", Values: []string{"no", "match"}}
		require.False(t, predicate.Evaluate("a", labels["a"]))
		require.False(t, predicate.Evaluate("b", labels["b"]))
	})

	t.Run("KeyOnlyMatch", func(t *testing.T) {
		predicate := &InPredicate{Key: "a", Values: []string{"no", "match"}}
		require.False(t, predicate.Evaluate("a", labels["a"]))
		require.False(t, predicate.Evaluate("b", labels["b"]))
	})

	t.Run("KeyValueMatch", func(t *testing.T) {
		predicate := &InPredicate{Key: "a", Values: []string{"2", "1"}}
		require.True(t, predicate.Evaluate("a", labels["a"]))
		require.False(t, predicate.Evaluate("b", labels["b"]))
	})
}
