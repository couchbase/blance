/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package blance

import (
	"reflect"
	"strings"
	"testing"
)

func TestFindStateChanges(t *testing.T) {
	tests := []struct {
		begStateIdx     int
		endStateIdx     int
		state           string
		states          []string
		begNodesByState map[string][]string
		endNodesByState map[string][]string
		expected        []string
	}{
		{0, 0, "primary",
			[]string{"primary", "replica"},
			map[string][]string{
				"primary": {"a"},
				"replica": {"b", "c"},
			},
			map[string][]string{
				"primary": {"a"},
				"replica": {"b", "c"},
			},
			nil,
		},
		{1, 2, "primary",
			[]string{"primary", "replica"},
			map[string][]string{
				"primary": {"a"},
				"replica": {"b", "c"},
			},
			map[string][]string{
				"primary": {"a"},
				"replica": {"b", "c"},
			},
			nil,
		},
		{0, 0, "primary",
			[]string{"primary", "replica"},
			map[string][]string{
				"primary": {},
				"replica": {"a"},
			},
			map[string][]string{
				"primary": {"a"},
				"replica": {},
			},
			nil,
		},
		{1, 2, "primary",
			[]string{"primary", "replica"},
			map[string][]string{
				"primary": {},
				"replica": {"a"},
			},
			map[string][]string{
				"primary": {"a"},
				"replica": {},
			},
			[]string{"a"},
		},
		{0, 1, "replica",
			[]string{"primary", "replica"},
			map[string][]string{
				"primary": {"a"},
				"replica": {},
			},
			map[string][]string{
				"primary": {},
				"replica": {"a"},
			},
			[]string{"a"},
		},
		{1, 2, "replica",
			[]string{"primary", "replica"},
			map[string][]string{
				"primary": {"a"},
				"replica": {},
			},
			map[string][]string{
				"primary": {},
				"replica": {"a"},
			},
			nil,
		},
		{1, 2, "replica",
			[]string{"primary", "replica"},
			map[string][]string{
				"primary": {},
				"replica": {"a"},
			},
			map[string][]string{
				"primary": {},
				"replica": {},
			},
			nil,
		},
		{1, 2, "primary",
			[]string{"primary", "replica"},
			map[string][]string{
				"primary": {"a"},
				"replica": {"b", "c", "d"},
			},
			map[string][]string{
				"primary": {"b"},
				"replica": {"a", "c", "d"},
			},
			[]string{"b"},
		},
		{1, 2, "primary",
			[]string{"primary", "replica"},
			map[string][]string{
				"primary": {"a"},
				"replica": {"b", "c", "d"},
			},
			map[string][]string{
				"primary": {"x"},
				"replica": {"a", "c", "d"},
			},
			nil,
		},
	}

	for i, test := range tests {
		got := findStateChanges(test.begStateIdx, test.endStateIdx,
			test.state, test.states,
			test.begNodesByState,
			test.endNodesByState)
		if !reflect.DeepEqual(got, test.expected) {
			t.Errorf("i: %d, got: %#v, expected: %#v, test: %#v",
				i, got, test.expected, test)
		}
	}
}

func TestCalcPartitionMoves(t *testing.T) {
	states := []string{"primary", "replica"}

	tests := []struct {
		before string
		moves  string
		after  string

		favorMinNodes bool
	}{
		//  primary | replica
		//  -------|--------
		{ // Test #0.
			" a",
			"",
			" a",
			false,
		},
		{
			" a",
			"",
			" a",
			true,
		},
		{
			"      | a",
			"",
			"      | a",
			false,
		},
		{
			"      | a",
			"",
			"      | a",
			true,
		},
		{
			" a    | b",
			"",
			" a    | b",
			false,
		},
		{ // Test #5.
			" a    | b",
			"",
			" a    | b",
			true,
		},
		{
			"",
			"+a",
			" a",
			false,
		},
		{
			"",
			"+a",
			" a",
			true,
		},
		{
			" a",
			"-a",
			"",
			false,
		},
		{
			" a",
			"-a",
			"",
			true,
		},
		{ // Test #10.
			"",
			`+a    |
			  a    |+b`,
			" a    | b",
			false,
		},
		{
			"",
			`      |+b
			 +a    | b`,
			" a    | b",
			true,
		},
		{
			" a    | b",
			` a    |-b`,
			" a",
			false,
		},
		{
			" a    | b",
			` a    |-b`,
			" a",
			true,
		},
		{
			" a    | b",
			`-a    | b`,
			"      | b",
			false,
		},
		{ // Test #15.
			" a    | b",
			`-a    | b`,
			"      | b",
			true,
		},
		{
			" a    | b",
			`-a    | b
			       |-b`, // NOTE: Some may say we should remove replica first.
			"",
			false,
		},
		{
			" a    | b",
			` a    |-b
			 -a    |`,
			"",
			true,
		},
		{
			" a",
			` a +b |
			 -a  b |`,
			"    b",
			false,
		},
		{
			" a",
			`-a    |
			    +b |`,
			"    b",
			true,
		},
		{ // Test #20.
			" a    | b  c",
			` a +b |-b  c
			 -a  b |    c
			     b |    c +d`,
			"    b |    c  d",
			false,
		},
		{ // Test #21.
			" a    | b  c",
			` a    | b  c +d
			 -a    | b  c  d
			    +b |-b  c  d`,
			"    b |    c  d",
			true,
		},
		{
			" a    |    b",
			` a +b |   -b
			 -a  b |+a`,
			"    b | a",
			false,
		},
		{
			" a    |    b",
			`-a    |+a  b
			    +b | a -b`,
			"    b | a",
			true,
		},
		{
			" a    |    b",
			` a +c |    b
			 -a  c |+a  b
			     c | a -b`,
			"    c | a",
			false,
		},
		{ // Test #25.
			" a    |    b",
			` a    |   -b
			 -a    |+a
			    +c | a`,
			"    c | a",
			true,
		},
		{
			" a    | b",
			` a +c | b
			 -a  c | b
			     c | b +d
			     c |-b  d`,
			"    c |    d",
			false,
		},
		{
			" a    | b",
			` a    |-b
			  a    |   +d
			 -a    |    d
			    +c |    d`,
			"    c |    d",
			true,
		},
		{
			" a    |    b",
			`-a    |+a  b
			       | a  b +c`,
			"      | a  b  c",
			false,
		},
	}

	negate := map[string]string{
		"+": "-",
		"-": "+",
	}

	ops := map[string]string{
		"+": "add",
		"-": "del",
	}

	for testi, test := range tests {
		before := convertLineToNodesByState(test.before, states)
		after := convertLineToNodesByState(test.after, states)

		var movesExp []map[string][]string

		if test.moves != "" {
			moveLines := strings.Split(test.moves, "\n")
			for _, moveLine := range moveLines {
				moveExp := convertLineToNodesByState(moveLine, states)
				movesExp = append(movesExp, moveExp)
			}
		}

		movesGot := CalcPartitionMoves(states, before, after, test.favorMinNodes)

		if len(movesGot) != len(movesExp) {
			t.Errorf("testi: %d, mismatch lengths,"+
				" before: %#v, after: %#v,"+
				" movesExp: %#v, movesGot: %#v, test: %#v",
				testi, before, after, movesExp, movesGot, test)

			continue
		}

		for moveExpi, moveExp := range movesExp {
			moveGot := movesGot[moveExpi]

			found := false

			for statei, state := range states {
				if found {
					continue
				}

				for _, move := range moveExp[state] {
					if found {
						continue
					}

					op := move[0:1]
					if op == "+" || op == "-" {
						found = true

						if moveGot.Node != move[1:] {
							t.Errorf("testi: %d, wrong node,"+
								" before: %#v, after: %#v,"+
								" movesExp: %#v, movesGot: %#v,"+
								" test: %#v",
								testi, before, after,
								movesExp, movesGot, test)
						}

						flipSideFound := ""
						flipSideState := ""
						flipSide := negate[op] + move[1:]
						for j := statei + 1; j < len(states); j++ {
							for _, x := range moveExp[states[j]] {
								if x == flipSide {
									flipSideFound = flipSide
									flipSideState = states[j]
								}
							}
						}

						stateExp := state
						if flipSideFound != "" {
							if op == "-" {
								stateExp = flipSideState
							}
						} else {
							if op == "-" {
								stateExp = ""
							}
						}

						if moveGot.State != stateExp {
							t.Errorf("testi: %d, not stateExp: %q,"+
								" before: %#v, after: %#v,"+
								" movesExp: %#v, movesGot: %#v,"+
								" test: %#v, move: %s,"+
								" flipSideFound: %q, flipSideState: %q",
								testi, stateExp, before, after,
								movesExp, movesGot, test, move,
								flipSideFound, flipSideState)
						}

						if flipSideFound != "" {
							if moveGot.Op != "promote" &&
								moveGot.Op != "demote" {
								t.Errorf("testi: %d, wanted pro/demote,"+
									" before: %#v, after: %#v,"+
									" movesExp: %#v, movesGot: %#v,"+
									" test: %#v, move: %s,"+
									" flipSideFound: %q, flipSideState: %q",
									testi, before, after,
									movesExp, movesGot, test, move,
									flipSideFound, flipSideState)
							}
						} else if moveGot.Op != ops[op] {
							t.Errorf("testi: %d, wanted op: %q,"+
								" before: %#v, after: %#v,"+
								" movesExp: %#v, movesGot: %#v,"+
								" test: %#v, move: %s,"+
								" flipSideFound: %q, flipSideState: %q",
								testi, ops[op], before, after,
								movesExp, movesGot, test, move,
								flipSideFound, flipSideState)
						}
					}
				}
			}
		}
	}
}

// Converts an input line string like " a b | +c -d", with input
// states of ["primary", "replica"] to something like {"primary": ["a",
// "b"], "replica": ["+c", "-d"]}.
func convertLineToNodesByState(
	line string, states []string) map[string][]string {
	nodesByState := map[string][]string{}

	line = strings.Trim(line, " ")
	for {
		linex := strings.Replace(line, "  ", " ", -1)
		if linex == line {
			break
		}
		line = linex
	}

	parts := strings.Split(line, "|")
	for i, state := range states {
		if i >= len(parts) {
			break
		}
		part := strings.Trim(parts[i], " ")
		if part != "" {
			nodes := strings.Split(part, " ")
			nodesByState[state] = append(nodesByState[state], nodes...)
		}
	}

	return nodesByState
}
