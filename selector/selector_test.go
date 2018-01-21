package selector

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestSelector(t *testing.T) {
	selector := &Selector{
		Predicates: []Predicate{&EqualsPredicate{Key: "a", Value: "1"}},
	}

	require.True(t,
		selector.Evaluate(map[string]string{
			"c": "3",
			"b": "",
			"a": "1",
		}),
	)
}

func TestSelector_Parse(t *testing.T) {
	testExpression(t, "a = 1", map[string]string{"a": "1"})
	testExpression(t, "!a = 1", map[string]string{"a": "2"})
	testExpression(t, "a != 1", map[string]string{"a": "2"})
	testExpression(t, "a == 1", map[string]string{"a": "1"})
	testExpression(t, "a", map[string]string{"a": ""})
	testExpression(t, "a", map[string]string{"a": "1"})
	testExpression(t, "!a", map[string]string{"b": ""})
	testExpression(t, "!!a", map[string]string{"a": ""})
	testExpression(t, "a in (1, 2, 3)", map[string]string{"a": "3"})
	testExpression(t, "!a in (1, 2, 3)", map[string]string{"b": "2"})
	testExpression(t, "a notin (1, 2, 3)", map[string]string{"b": "2"})
	testExpression(t, "a = 1, b = 2", map[string]string{"a": "1", "b": "2"})
	testExpression(t, "a = 1, !b", map[string]string{"a": "1", "b": "2"})
	testExpression(t, "a = 1, !b in (2, 3", map[string]string{"a": "1", "b": "2"})
}

func testExpression(t *testing.T, expression string, labels map[string]string) {
	t.Run(strings.Join([]string{expression, fmt.Sprint(labels)}, "_"), func(t *testing.T) {
		defer glog.Flush()
		selector, err := ParseSelector(expression)
		require.NoError(t, err)
		require.True(t, selector.Evaluate(labels))
	})
}
