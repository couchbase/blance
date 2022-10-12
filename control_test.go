//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package blance

import (
	"fmt"
	"reflect"
	"testing"
)

// Force partition's active on "c" and replica on "b"
func TestControlCase1(t *testing.T) {
	NodeScoreBooster = func(w int, s float64) float64 {
		// As defined in couchbase/cbgt
		score := float64(-w)
		if score < s {
			score = s
		}
		return score
	}
	defer func() {
		NodeScoreBooster = nil
	}()

	partitions := PartitionMap{
		"X": &Partition{
			Name:         "X",
			NodesByState: map[string][]string{},
		},
	}

	nodes := []string{"a", "b", "c", "d", "e"}

	model := PartitionModel{
		"primary": &PartitionModelState{
			Priority:    0,
			Constraints: 1,
		},
		"replica": &PartitionModelState{
			Priority:    1,
			Constraints: 1,
		},
	}

	r, warnings := PlanNextMapEx(
		partitions,
		nodes,
		nil,
		nil,
		model,
		PlanNextMapOptions{
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights: map[string]int{
				"a": -2,
				"b": -1,
				"d": -2,
				"e": -2,
			},
			NodeHierarchy:  nil,
			HierarchyRules: nil,
		})

	if len(warnings) > 0 {
		t.Errorf("WARNINGS: %v", warnings)
	}

	expect := PartitionMap{
		"X": &Partition{
			Name: "X",
			NodesByState: map[string][]string{
				"primary": {"c"},
				"replica": {"b"},
			},
		},
	}

	if !reflect.DeepEqual(r, expect) {
		var rStr string
		for k, v := range r {
			rStr += fmt.Sprintf("%+v: %+v", k, v)
		}

		t.Fatalf("Mismatch: %+v", rStr)
	}
}

// Do not normalize node weights (meaning "enablePartitionNodeStickiness" in
// cbgt) so single partitioned indexes (even with 1 replica) do not relocate
// on node additions.
func TestControlCase2(t *testing.T) {
	NodeScoreBooster = func(w int, s float64) float64 {
		// As defined in couchbase/cbgt
		score := float64(-w)
		if score < s {
			score = s
		}
		return score
	}
	defer func() {
		NodeScoreBooster = nil
	}()

	partitions := PartitionMap{
		"X": &Partition{
			Name: "X",
			NodesByState: map[string][]string{
				"primary": {"a"},
				"replica": {"b"},
			},
		},
		"Y": &Partition{
			Name: "Y",
			NodesByState: map[string][]string{
				"primary": {"b"},
				"replica": {"a"},
			},
		},
		"Z": &Partition{
			Name: "Z",
			NodesByState: map[string][]string{
				"primary": {"a"},
				"replica": {"b"},
			},
		},
	}

	nodes := []string{"a", "b"}
	nodesToAdd := []string{"c"}

	model := PartitionModel{
		"primary": &PartitionModelState{
			Priority:    0,
			Constraints: 1,
		},
		"replica": &PartitionModelState{
			Priority:    1,
			Constraints: 1,
		},
	}

	r, warnings := PlanNextMapEx(
		partitions,
		nodes,
		nil,
		nodesToAdd,
		model,
		PlanNextMapOptions{
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			NodeHierarchy:         nil,
			HierarchyRules:        nil,
		})

	if len(warnings) > 0 {
		t.Errorf("WARNINGS: %v", warnings)
	}

	expect := PartitionMap{
		"X": &Partition{
			Name: "X",
			NodesByState: map[string][]string{
				"primary": {"a"},
				"replica": {"b"},
			},
		},
		"Y": &Partition{
			Name: "Y",
			NodesByState: map[string][]string{
				"primary": {"b"},
				"replica": {"a"},
			},
		},
		"Z": &Partition{
			Name: "Z",
			NodesByState: map[string][]string{
				"primary": {"a"},
				"replica": {"b"},
			},
		},
	}

	if !reflect.DeepEqual(r, expect) {
		var rStr string
		for k, v := range r {
			rStr += fmt.Sprintf("%+v: %+v", k, v)
		}

		t.Fatalf("Mismatch: %+v", rStr)
	}
}

// If multiple nodes available for a single partitioned
// index with 1 replica, control the new index to reside on
// replica:"a" and primary:"b"
func TestControlCase3(t *testing.T) {
	NodeScoreBooster = func(w int, s float64) float64 {
		// As defined in couchbase/cbgt
		score := float64(-w)
		if score < s {
			score = s
		}
		return score
	}
	defer func() {
		NodeScoreBooster = nil
	}()

	partitions := PartitionMap{
		"X": &Partition{
			Name: "X",
			NodesByState: map[string][]string{
				"primary": {"a"},
				"replica": {"b"},
			},
		},
		"Y": &Partition{
			Name: "Y",
			NodesByState: map[string][]string{
				"primary": {"b"},
				"replica": {"a"},
			},
		},
		"Z": &Partition{
			Name:         "Z",
			NodesByState: map[string][]string{},
		},
	}

	nodes := []string{"a", "b", "c"}

	model := PartitionModel{
		"primary": &PartitionModelState{
			Priority:    0,
			Constraints: 1,
		},
		"replica": &PartitionModelState{
			Priority:    1,
			Constraints: 1,
		},
	}

	r, warnings := PlanNextMapEx(
		partitions,
		nodes,
		nil,
		nil,
		model,
		PlanNextMapOptions{
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights: map[string]int{
				"c": -3,
				"a": -1,
			},
			NodeHierarchy:  nil,
			HierarchyRules: nil,
		})

	if len(warnings) > 0 {
		t.Errorf("WARNINGS: %v", warnings)
	}

	expect := PartitionMap{
		"X": &Partition{
			Name: "X",
			NodesByState: map[string][]string{
				"primary": {"a"},
				"replica": {"b"},
			},
		},
		"Y": &Partition{
			Name: "Y",
			NodesByState: map[string][]string{
				"primary": {"b"},
				"replica": {"a"},
			},
		},
		"Z": &Partition{
			Name: "Z",
			NodesByState: map[string][]string{
				"primary": {"b"},
				"replica": {"a"},
			},
		},
	}

	if !reflect.DeepEqual(r, expect) {
		var rStr string
		for k, v := range r {
			rStr += fmt.Sprintf("%+v: %+v", k, v)
		}

		t.Fatalf("Mismatch: %+v", rStr)
	}
}

// Expect even distribution of actives and replicas
func TestControlCase4(t *testing.T) {
	NodeScoreBooster = func(w int, s float64) float64 {
		// As defined in couchbase/cbgt
		score := float64(-w)
		if score < s {
			score = s
		}
		return score
	}
	defer func() {
		NodeScoreBooster = nil
	}()

	partitions := PartitionMap{
		"X": &Partition{
			Name: "X",
			NodesByState: map[string][]string{
				"primary": {"a"},
				"replica": {"b"},
			},
		},
		"Y": &Partition{
			Name:         "Y",
			NodesByState: map[string][]string{},
		},
	}

	nodes := []string{"a", "b"}

	model := PartitionModel{
		"primary": &PartitionModelState{
			Priority:    0,
			Constraints: 1,
		},
		"replica": &PartitionModelState{
			Priority:    1,
			Constraints: 1,
		},
	}

	r, warnings := PlanNextMapEx(
		partitions,
		nodes,
		nil,
		nil,
		model,
		PlanNextMapOptions{
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights: map[string]int{
				"a": -1,
				"b": -1,
			},
			NodeHierarchy: map[string]string{
				"a": "Group 1",
				"b": "Group 2",
			},
			HierarchyRules: HierarchyRules{
				"replica": []*HierarchyRule{
					{
						IncludeLevel: 2,
						ExcludeLevel: 1,
					},
				},
			},
		})

	if len(warnings) > 0 {
		t.Errorf("WARNINGS: %v", warnings)
	}

	expect := PartitionMap{
		"X": &Partition{
			Name: "X",
			NodesByState: map[string][]string{
				"primary": {"a"},
				"replica": {"b"},
			},
		},
		"Y": &Partition{
			Name: "Y",
			NodesByState: map[string][]string{
				"primary": {"b"},
				"replica": {"a"},
			},
		},
	}

	if !reflect.DeepEqual(r, expect) {
		var rStr string
		for k, v := range r {
			rStr += fmt.Sprintf("%+v: %+v", k, v)
		}

		t.Fatalf("Mismatch: %+v", rStr)
	}
}
