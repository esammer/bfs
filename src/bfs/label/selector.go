package label

import (
	"bfs/config"
	"container/list"
	"fmt"
	"github.com/golang/glog"
	"strings"
	"text/scanner"
)

type Predicate interface {
	Evaluate(label *config.Label) bool
}

type Selector struct {
	Expression string
	Predicates []Predicate
}

func ParseSelector(expression string) (*Selector, error) {
	s := &scanner.Scanner{}
	s.Init(strings.NewReader(expression))

	tokens := make([]string, 0, 16)
	for token := s.Scan(); token != scanner.EOF; token = s.Scan() {
		tokens = append(tokens, s.TokenText())
	}

	stateStack := list.New()
	stateStack.PushFront(0)
	predicates := make([]Predicate, 0, 3)
	i := 0

	glog.V(2).Infof("Tokens: %+v", tokens)

	for stateStack.Len() > 0 {
		state := stateStack.Remove(stateStack.Front())

		glog.V(2).Infof("position: %d state: %d predicates: %#v", i, state, predicates)

		switch state {
		case 0:
			// expression
			if len(tokens[i:]) == 1 {
				// exists expression
				stateStack.PushFront(2)
			} else if len(tokens[i:]) > 1 {
				switch {
				case tokens[i] == "!":
					// not expression
					i++
					stateStack.PushFront(4) // Evaluate not after expression.
					stateStack.PushFront(0) // Evaluate expression.
				case tokens[i] == ",":
					// and
					i++
					stateStack.PushFront(0)
				case tokens[i+1] == "!" && tokens[i+2] == "=":
					// inequality
					stateStack.PushFront(6)
				case tokens[i+1] == "=":
					// equality expression
					stateStack.PushFront(3)
				case tokens[i+1] == "in":
					// in expression
					stateStack.PushFront(5)
				case tokens[i+1] == "notin":
					// not in expression
					stateStack.PushFront(7)
				default:
					// unknown expression
					return nil, fmt.Errorf("unable to determine operator in expression '%s' - token: %s position: %d state: %d", expression, tokens[i], i, state)
				}
			}
		case 2:
			// exists
			predicates = append(predicates, &EqualsPredicate{Key: tokens[i]})
			i++
		case 3:
			// equality
			key := tokens[i]
			i += 2
			value := tokens[i]
			if value == "=" {
				i++
				value = tokens[i]
			}
			i++
			predicates = append(predicates, &EqualsPredicate{Key: key, Value: value})
			stateStack.PushFront(0)
		case 4:
			// not
			if len(predicates) > 0 {
				lastPred := predicates[len(predicates)-1]
				predicates = append(predicates[:len(predicates)-1], &NotPredicate{Predicate: lastPred})
			} else {
				return nil, fmt.Errorf("no existing predicate to negate in expression '%s' - position: %d state: %d", expression, i, state)
			}
			stateStack.PushFront(0)
		case 7:
			// not in expression
			stateStack.PushFront(4)
			stateStack.PushFront(5)
		case 5:
			// in expression
			key := tokens[i]
			i += 2

			if tokens[i] == "(" {
				i++
			}

			values := make([]string, 0, len(tokens[i:])/2)

			for _, token := range tokens[i:] {
				if token == ")" {
					i++
					break
				} else if token == "," {
					i++
				} else {
					values = append(values, token)
					i++
				}
			}

			predicates = append(predicates, &InPredicate{Key: key, Values: values})
			stateStack.PushFront(0)
		case 6:
			// inequality
			key := tokens[i]
			i += 3
			value := tokens[i]
			i++
			predicates = append(predicates, &NotPredicate{Predicate: &EqualsPredicate{Key: key, Value: value}})
		default:
			return nil, fmt.Errorf("internal error - unknown state %d - position: %d tokens: %v", state, i, tokens)
		}
	}

	glog.V(2).Infof("Predicates: %#v", predicates)

	return &Selector{Expression: expression, Predicates: predicates}, nil
}

func (this *Selector) Evaluate(labels []*config.Label) bool {
	glog.V(2).Infof("Evaluate expression: %s against labels: %v", this.Expression, labels)

	for _, predicate := range this.Predicates {
		matches := 0

		for _, label := range labels {
			if predicate.Evaluate(label) {
				glog.V(2).Infof("Matched label: %v", label)
				matches++
			} else {
				glog.V(2).Infof("No match for label: %v", label)
			}
		}

		if matches == 0 {
			return false
		}
	}

	glog.V(2).Info("Selector matches")

	return true
}
