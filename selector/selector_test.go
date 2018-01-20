package selector

import (
	"bfs/config"
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
		selector.Evaluate([]*config.Label{
			{Key: "c", Value: "3"},
			{Key: "b"},
			{Key: "a", Value: "1"},
		}),
	)
}

func TestSelector_Parse(t *testing.T) {
	testExpression(t, "a = 1", []*config.Label{{Key: "a", Value: "1"}})
	testExpression(t, "!a = 1", []*config.Label{{Key: "a", Value: "2"}})
	testExpression(t, "a != 1", []*config.Label{{Key: "a", Value: "2"}})
	testExpression(t, "a == 1", []*config.Label{{Key: "a", Value: "1"}})
	testExpression(t, "a", []*config.Label{{Key: "a"}})
	testExpression(t, "a", []*config.Label{{Key: "a", Value: "1"}})
	testExpression(t, "!a", []*config.Label{{Key: "b"}})
	testExpression(t, "!!a", []*config.Label{{Key: "a"}})
	testExpression(t, "a in (1, 2, 3)", []*config.Label{{Key: "a", Value: "3"}})
	testExpression(t, "!a in (1, 2, 3)", []*config.Label{{Key: "b", Value: "2"}})
	testExpression(t, "a notin (1, 2, 3)", []*config.Label{{Key: "b", Value: "2"}})
	testExpression(t, "a = 1, b = 2", []*config.Label{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}})
	testExpression(t, "a = 1, !b", []*config.Label{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}})
	testExpression(t, "a = 1, !b in (2, 3", []*config.Label{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}})
}

func testExpression(t *testing.T, expression string, labels []*config.Label) {
	t.Run(strings.Join([]string{expression, fmt.Sprint(labels)}, "_"), func(t *testing.T) {
		defer glog.Flush()
		selector, err := ParseSelector(expression)
		require.NoError(t, err)
		require.True(t, selector.Evaluate(labels))
	})
}
