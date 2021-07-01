//  Copyright 2015-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package blance

// A NodeStateOp associates a node with a state change and operation.
// An array of NodeStateOp's could be interpreted as a series of
// node-by-node state transitions for a partition.  For example, for
// partition X, the NodeState transitions might be: first add node A
// to "primary", then demote node B to "replica", then remove (or del)
// partition X from node C.
type NodeStateOp struct {
	Node  string
	State string
	Op    string // Ex: "add", "del", "promote", "demote".
}

// CalcPartitionMoves computes the step-by-step moves to transition a
// partition from begNodesByState to endNodesByState.
//
// The states is an array of state names, like ["primary",
// "hotStandBy", "coldStandBy"], and should be ordered by more
// superior or important states coming earlier.  For example, "primary"
// should come before "replica".
//
// The begNodesByState and endNodesByState are keyed by stateName,
// where the values are an array of node names.  For example,
// {"primary": ["a"], "replica": ["b", "c"]}.
//
// The favorMinNodes should be true if the moves should be computed to
// have the partition assigned to the least number of nodes at any
// time (i.e., favoring max of single primary, if even there are
// temporarily no primaries for a time); if false, then the algorithm
// will instead try to assign the partition to 1 or more nodes,
// favoring partition availability across multiple nodes during moves.
func CalcPartitionMoves(
	states []string,
	begNodesByState map[string][]string,
	endNodesByState map[string][]string,
	favorMinNodes bool,
) []NodeStateOp {
	var moves []NodeStateOp

	seen := map[string]bool{}

	addMoves := func(nodes []string, state, op string) {
		for _, node := range nodes {
			if !seen[node] {
				seen[node] = true
				moves = append(moves, NodeStateOp{node, state, op})
			}
		}
	}

	begNodes := flattenNodesByState(begNodesByState)
	endNodes := flattenNodesByState(endNodesByState)

	adds := StringsRemoveStrings(endNodes, begNodes)
	dels := StringsRemoveStrings(begNodes, endNodes)

	if !favorMinNodes {
		for statei, state := range states {
			// Handle promotions of inferiorTo(state) to state.
			addMoves(findStateChanges(statei+1, len(states),
				state, states, begNodesByState, endNodesByState),
				state, "promote")

			// Handle demotions of superiorTo(state) to state.
			addMoves(findStateChanges(0, statei,
				state, states, begNodesByState, endNodesByState),
				state, "demote")

			// Handle clean additions of state.
			addMoves(StringsIntersectStrings(StringsRemoveStrings(
				endNodesByState[state], begNodesByState[state]),
				adds),
				state, "add")

			// Handle clean deletions of state.
			addMoves(StringsIntersectStrings(StringsRemoveStrings(
				begNodesByState[state], endNodesByState[state]),
				dels),
				"", "del")
		}
	} else {
		for statei := len(states) - 1; statei >= 0; statei-- {
			state := states[statei]

			// Handle clean deletions of state.
			addMoves(StringsIntersectStrings(StringsRemoveStrings(
				begNodesByState[state], endNodesByState[state]),
				dels),
				"", "del")

			// Handle demotions of superiorTo(state) to state.
			addMoves(findStateChanges(0, statei,
				state, states, begNodesByState, endNodesByState),
				state, "demote")

			// Handle promotions of inferiorTo(state) to state.
			addMoves(findStateChanges(statei+1, len(states),
				state, states, begNodesByState, endNodesByState),
				state, "promote")

			// Handle clean additions of state.
			addMoves(StringsIntersectStrings(StringsRemoveStrings(
				endNodesByState[state], begNodesByState[state]),
				adds),
				state, "add")
		}
	}

	return moves
}

func findStateChanges(begStateIdx, endStateIdx int,
	state string, states []string,
	begNodesByState map[string][]string,
	endNodesByState map[string][]string) (rv []string) {
	for _, node := range endNodesByState[state] {
		for i := begStateIdx; i < endStateIdx; i++ {
			for _, n := range begNodesByState[states[i]] {
				if n == node {
					rv = append(rv, node)
				}
			}
		}
	}

	return rv
}
