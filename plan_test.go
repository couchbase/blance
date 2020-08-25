package blance

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"
)

func TestFlattenNodesByState(t *testing.T) {
	tests := []struct {
		a   map[string][]string
		exp []string
	}{
		{map[string][]string{},
			[]string{}},
		{map[string][]string{"primary": {}},
			[]string{}},
		{map[string][]string{"primary": {"a"}},
			[]string{"a"}},
		{map[string][]string{"primary": {"a", "b"}},
			[]string{"a", "b"}},
		{map[string][]string{
			"primary": {"a", "b"},
			"replica": {"c"},
		}, []string{"a", "b", "c"}},
		{map[string][]string{
			"primary": {"a", "b"},
			"replica": {},
		}, []string{"a", "b"}},
	}
	for i, c := range tests {
		r := flattenNodesByState(c.a)
		if !reflect.DeepEqual(r, c.exp) {
			t.Errorf("i: %d, a: %#v, exp: %#v, got: %#v",
				i, c.a, c.exp, r)
		}
	}
}

func TestRemoveNodesFromNodesByState(t *testing.T) {
	tests := []struct {
		nodesByState map[string][]string
		removeNodes  []string
		exp          map[string][]string
	}{
		{map[string][]string{"primary": {"a", "b"}},
			[]string{"a", "b"},
			map[string][]string{"primary": {}},
		},
		{map[string][]string{"primary": {"a", "b"}},
			[]string{"b", "c"},
			map[string][]string{"primary": {"a"}},
		},
		{map[string][]string{"primary": {"a", "b"}},
			[]string{"a", "c"},
			map[string][]string{"primary": {"b"}},
		},
		{map[string][]string{"primary": {"a", "b"}},
			[]string{},
			map[string][]string{"primary": {"a", "b"}},
		},
		{
			map[string][]string{
				"primary": {"a", "b"},
				"replica": {"c"},
			},
			[]string{},
			map[string][]string{
				"primary": {"a", "b"},
				"replica": {"c"},
			},
		},
		{
			map[string][]string{
				"primary": {"a", "b"},
				"replica": {"c"},
			},
			[]string{"a"},
			map[string][]string{
				"primary": {"b"},
				"replica": {"c"},
			},
		},
		{
			map[string][]string{
				"primary": {"a", "b"},
				"replica": {"c"},
			},
			[]string{"a", "c"},
			map[string][]string{
				"primary": {"b"},
				"replica": {},
			},
		},
	}
	for i, c := range tests {
		r := removeNodesFromNodesByState(c.nodesByState, c.removeNodes, nil)
		if !reflect.DeepEqual(r, c.exp) {
			t.Errorf("i: %d, nodesByState: %#v,"+
				" removeNodes: %#v, exp: %#v, got: %#v",
				i, c.nodesByState, c.removeNodes, c.exp, r)
		}
	}
}

func TestStateNameSorter(t *testing.T) {
	tests := []struct {
		m   PartitionModel
		s   []string
		exp []string
	}{
		{
			PartitionModel{
				"primary": &PartitionModelState{Priority: 0},
				"replica": &PartitionModelState{Priority: 1},
			},
			[]string{},
			[]string{},
		},
		{
			PartitionModel{
				"primary": &PartitionModelState{Priority: 0},
				"replica": &PartitionModelState{Priority: 1},
			},
			[]string{"primary", "replica"},
			[]string{"primary", "replica"},
		},
		{
			PartitionModel{
				"primary": &PartitionModelState{Priority: 0},
				"replica": &PartitionModelState{Priority: 1},
			},
			[]string{"replica", "primary"},
			[]string{"primary", "replica"},
		},
		{
			PartitionModel{
				"primary": &PartitionModelState{Priority: 0},
				"replica": &PartitionModelState{Priority: 1},
			},
			[]string{"a", "b"},
			[]string{"a", "b"},
		},
		{
			PartitionModel{
				"primary": &PartitionModelState{Priority: 0},
				"replica": &PartitionModelState{Priority: 1},
			},
			[]string{"a", "primary"},
			[]string{"a", "primary"},
		},
		{
			PartitionModel{
				"primary": &PartitionModelState{Priority: 0},
				"replica": &PartitionModelState{Priority: 1},
			},
			[]string{"primary", "a"},
			[]string{"a", "primary"},
		},
	}
	for i, c := range tests {
		sort.Sort(&stateNameSorter{m: c.m, s: c.s})
		if !reflect.DeepEqual(c.s, c.exp) {
			t.Errorf("i: %d, m: %#v, s: %#v, exp: %#v",
				i, c.m, c.s, c.exp)
		}
	}
}

func TestCountStateNodes(t *testing.T) {
	tests := []struct {
		m   PartitionMap
		w   map[string]int
		exp map[string]map[string]int
	}{
		{
			PartitionMap{
				"0": &Partition{NodesByState: map[string][]string{
					"primary": {"a"},
					"replica": {"b", "c"},
				}},
				"1": &Partition{NodesByState: map[string][]string{
					"primary": {"b"},
					"replica": {"c"},
				}},
			},
			nil,
			map[string]map[string]int{
				"primary": {
					"a": 1,
					"b": 1,
				},
				"replica": {
					"b": 1,
					"c": 2,
				},
			},
		},
		{
			PartitionMap{
				"0": &Partition{NodesByState: map[string][]string{
					"replica": {"b", "c"},
				}},
				"1": &Partition{NodesByState: map[string][]string{
					"primary": {"b"},
					"replica": {"c"},
				}},
			},
			nil,
			map[string]map[string]int{
				"primary": {
					"b": 1,
				},
				"replica": {
					"b": 1,
					"c": 2,
				},
			},
		},
	}
	for i, c := range tests {
		r := countStateNodes(c.m, c.w)
		if !reflect.DeepEqual(r, c.exp) {
			t.Errorf("i: %d, m: %#v, w: %#v, exp: %#v",
				i, c.m, c.w, c.exp)
		}
	}
}

func TestPartitionMapToArrayCopy(t *testing.T) {
	tests := []struct {
		m   PartitionMap
		exp []*Partition
	}{
		{
			PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b", "c"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"c"},
					},
				},
			},
			[]*Partition{
				{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b", "c"},
					},
				},
				{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"c"},
					},
				},
			},
		},
	}
	for _, c := range tests {
		r := c.m.toArrayCopy()
		testSubset := func(a, b []*Partition) {
			if len(a) != len(b) {
				t.Errorf("expected same lengths")
			}
			for _, ap := range a {
				found := false
				for _, bp := range b {
					if reflect.DeepEqual(ap, bp) {
						found = true
					}
				}
				if !found {
					t.Errorf("couldn't find a entry in b")
				}
			}
		}
		testSubset(r, c.exp)
		testSubset(c.exp, r)
	}
}

func TestFindAncestor(t *testing.T) {
	tests := []struct {
		level      int
		mapParents map[string]string
		exp        string
	}{
		{0, map[string]string{}, "a"},
		{1, map[string]string{}, ""},
		{2, map[string]string{}, ""},
		{0, map[string]string{"a": "r"}, "a"},
		{1, map[string]string{"a": "r"}, "r"},
		{2, map[string]string{"a": "r"}, ""},
		{3, map[string]string{"a": "r"}, ""},
		{0, map[string]string{"a": "r", "r": "g"}, "a"},
		{1, map[string]string{"a": "r", "r": "g"}, "r"},
		{2, map[string]string{"a": "r", "r": "g"}, "g"},
		{3, map[string]string{"a": "r", "r": "g"}, ""},
	}
	for i, c := range tests {
		r := findAncestor("a", c.mapParents, c.level)
		if !reflect.DeepEqual(r, c.exp) {
			t.Errorf("i: %d, level: %d, mapParents: %#v,"+
				" RESULT: %#v, EXPECTED: %#v",
				i, c.level, c.mapParents, r, c.exp)
		}
	}
}

func TestFindLeaves(t *testing.T) {
	tests := []struct {
		mapChildren map[string][]string
		exp         []string
	}{
		{map[string][]string{}, []string{"a"}},
		{map[string][]string{"x": {"xx"}}, []string{"a"}},
		{map[string][]string{"a": {}}, []string{"a"}},
		{map[string][]string{"a": {"b"}}, []string{"b"}},
		{map[string][]string{"a": {"b", "c"}}, []string{"b", "c"}},
	}
	for i, c := range tests {
		r := findLeaves("a", c.mapChildren)
		if !reflect.DeepEqual(r, c.exp) {
			t.Errorf("i: %d, mapChildren: %#v, RESULT: %#v, EXPECTED: %#v",
				i, c.mapChildren, r, c.exp)
		}
	}
}

func TestMapParentsToMapChildren(t *testing.T) {
	tests := []struct {
		in  map[string]string
		exp map[string][]string
	}{
		{map[string]string{},
			map[string][]string{}},
		{map[string]string{"a": "r"},
			map[string][]string{"r": {"a"}}},
		{map[string]string{"a": "r", "b": "r2"},
			map[string][]string{
				"r":  {"a"},
				"r2": {"b"},
			}},
		{map[string]string{"a": "r", "a1": "a"},
			map[string][]string{
				"r": {"a"},
				"a": {"a1"},
			}},
		{map[string]string{"a": "r", "a1": "a", "a2": "a"},
			map[string][]string{
				"r": {"a"},
				"a": {"a1", "a2"},
			}},
		{map[string]string{"a": "r", "a1": "a", "a2": "a", "a0": "a"},
			map[string][]string{
				"r": {"a"},
				"a": {"a0", "a1", "a2"},
			}},
	}
	for i, c := range tests {
		r := mapParentsToMapChildren(c.in)
		if !reflect.DeepEqual(r, c.exp) {
			t.Errorf("i: %d, in: %#v, RESULT: %#v, EXPECTED: %#v",
				i, c.in, r, c.exp)
		}
	}
}

func TestPlanNextMap(t *testing.T) {
	tests := []struct {
		About                 string
		PrevMap               PartitionMap
		Nodes                 []string
		NodesToRemove         []string
		NodesToAdd            []string
		Model                 PartitionModel
		ModelStateConstraints map[string]int
		PartitionWeights      map[string]int
		StateStickiness       map[string]int
		NodeWeights           map[string]int
		NodeHierarchy         map[string]string
		HierarchyRules        HierarchyRules
		exp                   PartitionMap
		expNumWarnings        int
	}{
		{
			About: "single node, simple assignment of primary",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a"},
			Model: PartitionModel{
				"primary": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"replica": &PartitionModelState{
					Priority: 1, Constraints: 0,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"a"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"a"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "single node, not enough to assign replicas",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a"},
			Model: PartitionModel{
				"primary": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"replica": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
			},
			expNumWarnings: 2,
		},
		{
			About:         "no partitions case",
			PrevMap:       PartitionMap{},
			Nodes:         []string{"a"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a"},
			Model: PartitionModel{
				"primary": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"replica": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp:                   PartitionMap{},
			expNumWarnings:        0,
		},
		{
			About: "no model states case",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:                 []string{"a"},
			NodesToRemove:         []string{},
			NodesToAdd:            []string{"a"},
			Model:                 PartitionModel{},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "2 nodes, enough for clean primary & replica",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a", "b"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a", "b"},
			Model: PartitionModel{
				"primary": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"replica": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"a"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "2 nodes, remove 1",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"a"},
					},
				},
			},
			Nodes:         []string{"a", "b"},
			NodesToRemove: []string{"b"},
			NodesToAdd:    []string{},
			Model: PartitionModel{
				"primary": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"replica": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
			},
			expNumWarnings: 2,
		},
		{
			About: "2 nodes, remove 2",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"a"},
					},
				},
			},
			Nodes:         []string{"a", "b"},
			NodesToRemove: []string{"b", "a"},
			NodesToAdd:    []string{},
			Model: PartitionModel{
				"primary": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"replica": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {},
						"replica": {},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {},
						"replica": {},
					},
				},
			},
			expNumWarnings: 4,
		},
		{
			About: "2 nodes, remove 3",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"a"},
					},
				},
			},
			Nodes:         []string{"a", "b", "c"},
			NodesToRemove: []string{"c", "b", "a"},
			NodesToAdd:    []string{},
			Model: PartitionModel{
				"primary": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"replica": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {},
						"replica": {},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {},
						"replica": {},
					},
				},
			},
			expNumWarnings: 4,
		},
		{
			About: "2 nodes, nothing to add or remove",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"a"},
					},
				},
			},
			Nodes:         []string{"a", "b", "c"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{},
			Model: PartitionModel{
				"primary": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"replica": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"a"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "2 nodes, swap node a",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"a"},
					},
				},
			},
			Nodes:         []string{"a", "b", "c"},
			NodesToRemove: []string{"a"},
			NodesToAdd:    []string{"c"},
			Model: PartitionModel{
				"primary": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"replica": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"c"},
						"replica": {"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"c"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "2 nodes, swap node b",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"a"},
					},
				},
			},
			Nodes:         []string{"a", "b", "c"},
			NodesToRemove: []string{"b"},
			NodesToAdd:    []string{"c"},
			Model: PartitionModel{
				"primary": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"replica": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"c"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"c"},
						"replica": {"a"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "2 nodes, swap nodes a & b for c & d",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"a"},
					},
				},
			},
			Nodes:         []string{"a", "b", "c", "d"},
			NodesToRemove: []string{"a", "b"},
			NodesToAdd:    []string{"c", "d"},
			Model: PartitionModel{
				"primary": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"replica": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"c"},
						"replica": {"d"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"d"},
						"replica": {"c"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "add 2 nodes, 2 primaries, 1 replica",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a", "b"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a", "b"},
			Model: PartitionModel{
				"primary": &PartitionModelState{
					Priority: 0, Constraints: 2,
				},
				"replica": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"a", "b"},
						"replica": {},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"a", "b"},
						"replica": {},
					},
				},
			},
			expNumWarnings: 2,
		},
		{
			About: "add 3 nodes, 2 primaries, 1 replica",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a", "b", "c"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a", "b", "c"},
			Model: PartitionModel{
				"primary": &PartitionModelState{
					Priority: 0, Constraints: 2,
				},
				"replica": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"b", "a"},
						"replica": {"c"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"c", "a"},
						"replica": {"b"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "model state constraint override",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a", "b"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a", "b"},
			Model: PartitionModel{
				"primary": &PartitionModelState{
					Priority: 0, Constraints: 0,
				},
				"replica": &PartitionModelState{
					Priority: 1, Constraints: 0,
				},
			},
			ModelStateConstraints: map[string]int{
				"primary": 1,
				"replica": 1,
			},
			PartitionWeights: nil,
			StateStickiness:  nil,
			NodeWeights:      nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"a"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "partition weight of 3 for partition 0",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
				"2": &Partition{
					Name:         "2",
					NodesByState: map[string][]string{},
				},
				"3": &Partition{
					Name:         "3",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a", "b"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a", "b"},
			Model: PartitionModel{
				"primary": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"replica": &PartitionModelState{
					Priority: 1, Constraints: 0,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights: map[string]int{
				"0": 3,
			},
			StateStickiness: nil,
			NodeWeights:     nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"a"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"b"},
					},
				},
				"2": &Partition{
					Name: "2",
					NodesByState: map[string][]string{
						"primary": {"b"},
					},
				},
				"3": &Partition{
					Name: "3",
					NodesByState: map[string][]string{
						"primary": {"b"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "partition weight of 3 for partition 0, with 4 partitions",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
				"2": &Partition{
					Name:         "2",
					NodesByState: map[string][]string{},
				},
				"3": &Partition{
					Name:         "3",
					NodesByState: map[string][]string{},
				},
				"4": &Partition{
					Name:         "4",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a", "b"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a", "b"},
			Model: PartitionModel{
				"primary": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"replica": &PartitionModelState{
					Priority: 1, Constraints: 0,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights: map[string]int{
				"0": 3,
			},
			StateStickiness: nil,
			NodeWeights:     nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"a"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"b"},
					},
				},
				"2": &Partition{
					Name: "2",
					NodesByState: map[string][]string{
						"primary": {"b"},
					},
				},
				"3": &Partition{
					Name: "3",
					NodesByState: map[string][]string{
						"primary": {"b"},
					},
				},
				"4": &Partition{
					Name: "4",
					NodesByState: map[string][]string{
						"primary": {"a"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "partition weight of 3 for partition 1, with 5 partitions",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
				"2": &Partition{
					Name:         "2",
					NodesByState: map[string][]string{},
				},
				"3": &Partition{
					Name:         "3",
					NodesByState: map[string][]string{},
				},
				"4": &Partition{
					Name:         "4",
					NodesByState: map[string][]string{},
				},
				"5": &Partition{
					Name:         "5",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a", "b"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a", "b"},
			Model: PartitionModel{
				"primary": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"replica": &PartitionModelState{
					Priority: 1, Constraints: 0,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights: map[string]int{
				"1": 3,
			},
			StateStickiness: nil,
			NodeWeights:     nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"a"},
					},
				},
				"2": &Partition{
					Name: "2",
					NodesByState: map[string][]string{
						"primary": {"b"},
					},
				},
				"3": &Partition{
					Name: "3",
					NodesByState: map[string][]string{
						"primary": {"b"},
					},
				},
				"4": &Partition{
					Name: "4",
					NodesByState: map[string][]string{
						"primary": {"a"},
					},
				},
				"5": &Partition{
					Name: "5",
					NodesByState: map[string][]string{
						"primary": {"b"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "node weight of 3 for node a",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
				"2": &Partition{
					Name:         "2",
					NodesByState: map[string][]string{},
				},
				"3": &Partition{
					Name:         "3",
					NodesByState: map[string][]string{},
				},
				"4": &Partition{
					Name:         "4",
					NodesByState: map[string][]string{},
				},
				"5": &Partition{
					Name:         "5",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a", "b"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a", "b"},
			Model: PartitionModel{
				"primary": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"replica": &PartitionModelState{
					Priority: 1, Constraints: 0,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights: map[string]int{
				"a": 3,
			},
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"a"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"b"},
					},
				},
				"2": &Partition{
					Name: "2",
					NodesByState: map[string][]string{
						"primary": {"a"},
					},
				},
				"3": &Partition{
					Name: "3",
					NodesByState: map[string][]string{
						"primary": {"a"},
					},
				},
				"4": &Partition{
					Name: "4",
					NodesByState: map[string][]string{
						"primary": {"a"},
					},
				},
				"5": &Partition{
					Name: "5",
					NodesByState: map[string][]string{
						"primary": {"b"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "node weight of 3 for node b",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
				"2": &Partition{
					Name:         "2",
					NodesByState: map[string][]string{},
				},
				"3": &Partition{
					Name:         "3",
					NodesByState: map[string][]string{},
				},
				"4": &Partition{
					Name:         "4",
					NodesByState: map[string][]string{},
				},
				"5": &Partition{
					Name:         "5",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a", "b"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a", "b"},
			Model: PartitionModel{
				"primary": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"replica": &PartitionModelState{
					Priority: 1, Constraints: 0,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights: map[string]int{
				"b": 3,
			},
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"primary": {"a"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"primary": {"b"},
					},
				},
				"2": &Partition{
					Name: "2",
					NodesByState: map[string][]string{
						"primary": {"b"},
					},
				},
				"3": &Partition{
					Name: "3",
					NodesByState: map[string][]string{
						"primary": {"b"},
					},
				},
				"4": &Partition{
					Name: "4",
					NodesByState: map[string][]string{
						"primary": {"a"},
					},
				},
				"5": &Partition{
					Name: "5",
					NodesByState: map[string][]string{
						"primary": {"b"},
					},
				},
			},
			expNumWarnings: 0,
		},
	}
	for i, c := range tests {
		r, rWarnings := PlanNextMap(
			c.PrevMap,
			c.Nodes,
			c.NodesToRemove,
			c.NodesToAdd,
			c.Model,
			c.ModelStateConstraints,
			c.PartitionWeights,
			c.StateStickiness,
			c.NodeWeights,
			c.NodeHierarchy,
			c.HierarchyRules)
		if !reflect.DeepEqual(r, c.exp) {
			jc, _ := json.Marshal(c)
			jr, _ := json.Marshal(r)
			jexp, _ := json.Marshal(c.exp)
			t.Errorf("i: %d, planNextMap, c: %s,"+
				" [RESULT] r: %s, [EXPECTED] exp: %s",
				i, jc, jr, jexp)
		}
		if c.expNumWarnings != len(rWarnings) {
			t.Errorf("i: %d, planNextMap.warnings,"+
				" c: %#v, rWarnings: %v, expNumWarnings: %d",
				i, c, rWarnings, c.expNumWarnings)
		}
	}
}

type VisTestCase struct {
	Ignore                bool
	About                 string
	FromTo                [][]string
	FromToPriority        bool
	Nodes                 []string
	NodesToRemove         []string
	NodesToAdd            []string
	Model                 PartitionModel
	ModelStateConstraints map[string]int
	PartitionWeights      map[string]int
	StateStickiness       map[string]int
	NodeWeights           map[string]int
	NodeHierarchy         map[string]string
	HierarchyRules        HierarchyRules
	expNumWarnings        int
}

type fromToCell struct {
	entry    string
	nodeName string
}

type fromToCells []*fromToCell

func (pms fromToCells) Len() int {
	return len(pms)
}

func (pms fromToCells) Less(i, j int) bool {
	return pms[i].entry < pms[j].entry
}

func (pms fromToCells) Swap(i, j int) {
	pms[i], pms[j] = pms[j], pms[i]
}

func testVisTestCases(t *testing.T, tests []VisTestCase) {
	nodeNames := map[int]string{} // Maps 0 to "a", 1 to "b", etc.
	for i := 0; i < 26; i++ {
		nodeNames[i] = fmt.Sprintf("%c", i+97) // Start at ASCII 'a'.
	}
	stateNames := map[string]string{
		"m": "primary",
		"s": "replica",
	}
	for i, c := range tests {
		if c.Ignore {
			continue
		}
		prevMap := PartitionMap{}
		expMap := PartitionMap{}
		for i, partitionFromTo := range c.FromTo {
			partitionName := fmt.Sprintf("%03d", i)
			from := partitionFromTo[0]
			to := partitionFromTo[1]
			cellLength := 1
			if c.FromToPriority {
				cellLength = 2
			}

			partition := &Partition{
				Name:         partitionName,
				NodesByState: map[string][]string{},
			}
			prevMap[partitionName] = partition
			row := fromToCells{}
			for j := 0; j < len(from); j = j + cellLength {
				row = append(row, &fromToCell{
					entry:    from[j : j+cellLength],
					nodeName: nodeNames[j/cellLength],
				})
			}
			sort.Sort(row)
			for _, cell := range row {
				stateName := stateNames[cell.entry[0:1]]
				if stateName != "" {
					partition.NodesByState[stateName] =
						append(partition.NodesByState[stateName],
							cell.nodeName)
				}
			}

			partition = &Partition{
				Name:         partitionName,
				NodesByState: map[string][]string{},
			}
			expMap[partitionName] = partition
			row = fromToCells{}
			for j := 0; j < len(to); j = j + cellLength {
				row = append(row, &fromToCell{
					entry:    to[j : j+cellLength],
					nodeName: nodeNames[j/cellLength],
				})
			}
			sort.Sort(row)
			for _, cell := range row {
				stateName := stateNames[cell.entry[0:1]]
				if stateName != "" {
					partition.NodesByState[stateName] =
						append(partition.NodesByState[stateName],
							cell.nodeName)
				}
			}
		}
		r, rWarnings := PlanNextMap(
			prevMap,
			c.Nodes,
			c.NodesToRemove,
			c.NodesToAdd,
			c.Model,
			c.ModelStateConstraints,
			c.PartitionWeights,
			c.StateStickiness,
			c.NodeWeights,
			c.NodeHierarchy,
			c.HierarchyRules)
		if !reflect.DeepEqual(r, expMap) {
			jc, _ := json.Marshal(c)
			jp, _ := json.Marshal(prevMap)
			jr, _ := json.Marshal(r)
			jexp, _ := json.Marshal(expMap)
			t.Errorf("i: %d, planNextMapVis, c: %s,"+
				"\nINPUT jp: %s,\nRESULT r: %s,\nEXPECTED: %s",
				i, jc, jp, jr, jexp)
		}
		if c.expNumWarnings != len(rWarnings) {
			t.Errorf("i: %d, planNextMapVis.warnings, c: %#v,"+
				" rWarnings: %v, expNumWarnings: %d",
				i, c, rWarnings, c.expNumWarnings)
		}
	}
}

func TestPlanNextMapVis(t *testing.T) {
	partitionModel1Primary0Replica := PartitionModel{
		"primary": &PartitionModelState{
			Priority: 0, Constraints: 1,
		},
		"replica": &PartitionModelState{
			Priority: 1, Constraints: 0,
		},
	}
	partitionModel1Primary1Replica := PartitionModel{
		"primary": &PartitionModelState{
			Priority: 0, Constraints: 1,
		},
		"replica": &PartitionModelState{
			Priority: 1, Constraints: 1,
		},
	}
	tests := []VisTestCase{
		{
			About: "single node, simple assignment of primary",
			FromTo: [][]string{
				{"", "m"},
				{"", "m"},
			},
			Nodes:          []string{"a"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"a"},
			Model:          partitionModel1Primary0Replica,
			expNumWarnings: 0,
		},
		{
			About: "added nodes a & b",
			FromTo: [][]string{
				{"", "ms"},
				{"", "sm"},
			},
			Nodes:          []string{"a", "b"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"a", "b"},
			Model:          partitionModel1Primary1Replica,
			expNumWarnings: 0,
		},
		{
			About: "single node to 2 nodes",
			FromTo: [][]string{
				{"m", "sm"},
				{"m", "ms"},
			},
			Nodes:          []string{"a", "b"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"b"},
			Model:          partitionModel1Primary1Replica,
			expNumWarnings: 0,
		},
		{
			About: "single node to 3 nodes",
			FromTo: [][]string{
				{"m", "sm "},
				{"m", "m s"},
			},
			Nodes:          []string{"a", "b", "c"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"b", "c"},
			Model:          partitionModel1Primary1Replica,
			expNumWarnings: 0,
		},
		{
			About: "2 unbalanced nodes to balanced'ness",
			FromTo: [][]string{
				{"ms", "sm"},
				{"ms", "ms"},
			},
			Nodes:          []string{"a", "b"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{},
			Model:          partitionModel1Primary1Replica,
			expNumWarnings: 0,
		},
		{
			About: "2 unbalanced nodes to 3 balanced nodes",
			FromTo: [][]string{
				{"ms", " sm"},
				{"ms", "m s"},
			},
			Nodes:          []string{"a", "b", "c"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"c"},
			Model:          partitionModel1Primary1Replica,
			expNumWarnings: 0,
		},
		{
			About: "4 partitions, 1 to 4 nodes",
			FromTo: [][]string{
				{"m", "sm  "},
				{"m", "  ms"},
				{"m", "  sm"},
				{"m", "ms  "},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"b", "c", "d"},
			Model:          partitionModel1Primary1Replica,
			expNumWarnings: 0,
		},
		{
			About: "8 partitions, 1 to 4 nodes",
			FromTo: [][]string{
				//             abcd
				{"m", "sm  "},
				{"m", "  ms"},
				{"m", "s  m"},
				{"m", " ms "},
				{"m", "  ms"},
				{"m", " s m"},
				{"m", "ms  "},
				{"m", "m s "},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"b", "c", "d"},
			Model:          partitionModel1Primary1Replica,
			expNumWarnings: 0,
		},
		{
			About: "8 partitions, 4 nodes don't change, 1 replica moved",
			FromTo: [][]string{
				//        abcd    abcd
				{"sm  ", "sm  "},
				{"  ms", "  ms"},
				{"s  m", "s  m"},
				{" ms ", " ms "},
				{" sm ", "  ms"}, // Replica moved to d for more balanced'ness.
				{" s m", " s m"},
				{"ms  ", "ms  "},
				{"m s ", "m s "},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{},
			Model:          partitionModel1Primary1Replica,
			expNumWarnings: 0,
		},
		{
			// Take output from previous case and use as input to this
			// case, and see that it stabilized...
			About: "8 partitions, 4 nodes don't change, so no changes",
			FromTo: [][]string{
				//        abcd    abcd
				{"sm  ", "sm  "},
				{"  ms", "  ms"},
				{"s  m", "s  m"},
				{" ms ", " ms "},
				{" sm ", "  ms"},
				{" s m", " s m"},
				{"ms  ", "ms  "},
				{"m s ", "m s "},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{},
			Model:          partitionModel1Primary1Replica,
			expNumWarnings: 0,
		},
		{
			About: "single node swap, from node b to node e",
			FromTo: [][]string{
				//        abcd    abcde
				{" m s", "   sm"},
				{"  ms", "  ms "},
				{"s  m", "s  m "},
				{" ms ", "  s m"},
				{" sm ", "  m s"},
				{"s  m", "s  m "},
				{"ms  ", "m   s"},
				{"m s ", "m s  "},
			},
			Nodes:          []string{"a", "b", "c", "d", "e"},
			NodesToRemove:  []string{"b"},
			NodesToAdd:     []string{"e"},
			Model:          partitionModel1Primary1Replica,
			expNumWarnings: 0,
		},
		{
			// Primaries stayed nicely stable during node removal.
			// TODO: But, perhaps node a has too much load.
			About: "4 nodes to 3 nodes, remove node d",
			FromTo: [][]string{
				//        abcd    abc
				{" m s", "sm "},
				{"  ms", "s m"},
				{"s  m", "m s"},
				{" ms ", " ms"},
				{" sm ", " sm"},
				{"s  m", "sm "},
				{"ms  ", "ms "},
				{"m s ", "m s"},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{"d"},
			NodesToAdd:     []string{},
			Model:          partitionModel1Primary1Replica,
			expNumWarnings: 0,
		},
		{
			// TODO: ISSUE: the replicas aren't cleared when we change
			// the constraints from 1 replica down to 0 replicas, so
			// ignore this case for now.
			Ignore: true,
			About:  "change constraints from 1 replica to 0 replicas",
			FromTo: [][]string{
				//        abcd    abcd
				{" m s", " m  "},
				{"  ms", "  m "},
				{"s  m", "   m"},
				{" ms ", " m  "},
				{" sm ", "  m "},
				{"s  m", "   m"},
				{"ms  ", "m   "},
				{"m s ", "m   "},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{},
			Model:          partitionModel1Primary0Replica,
			expNumWarnings: 0,
		},
		{
			About: "8 partitions, 1 to 8 nodes",
			FromTo: [][]string{
				//             abcdefgh
				{"m", "sm      "},
				{"m", "  ms    "},
				{"m", "  sm    "},
				{"m", "    ms  "},
				{"m", "    sm  "},
				{"m", "      ms"},
				{"m", "      sm"},
				{"m", "ms      "},
			},
			Nodes:          []string{"a", "b", "c", "d", "e", "f", "g", "h"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"b", "c", "d", "e", "f", "g", "h"},
			Model:          partitionModel1Primary1Replica,
			expNumWarnings: 0,
		},
		{
			About: "8 partitions, 1 to 8 nodes, 0 replicas",
			FromTo: [][]string{
				//             abcdefgh
				{"m", " m      "},
				{"m", "  m     "},
				{"m", "   m    "},
				{"m", "    m   "},
				{"m", "     m  "},
				{"m", "      m "},
				{"m", "       m"},
				{"m", "m       "},
			},
			Nodes:          []string{"a", "b", "c", "d", "e", "f", "g", "h"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"b", "c", "d", "e", "f", "g", "h"},
			Model:          partitionModel1Primary0Replica,
			expNumWarnings: 0,
		},
		{
			About: "8 partitions, 4 nodes, increase partition 000 weight",
			FromTo: [][]string{
				//        abcd    abcd
				{"sm  ", " m s"},
				{"  ms", "s m "},
				{"s  m", "s  m"},
				{" ms ", "  sm"},
				{" sm ", " sm "},
				{" s m", " s m"},
				{"ms  ", "ms  "},
				{"m s ", "m s "},
			},
			Nodes:            []string{"a", "b", "c", "d"},
			NodesToRemove:    []string{},
			NodesToAdd:       []string{},
			PartitionWeights: map[string]int{"000": 100},
			Model:            partitionModel1Primary1Replica,
			expNumWarnings:   0,
		},
		{
			About: "8 partitions, 4 nodes, increase partition 004 weight",
			FromTo: [][]string{
				//        abcd    abcd
				{"sm  ", "sm  "},
				{"  ms", "s  m"},
				{"s  m", "s  m"},
				{" ms ", " ms "},
				{" sm ", "  ms"},
				{" s m", " s m"},
				{"ms  ", "ms  "},
				{"m s ", "m s "},
			},
			Nodes:            []string{"a", "b", "c", "d"},
			NodesToRemove:    []string{},
			NodesToAdd:       []string{},
			PartitionWeights: map[string]int{"004": 100},
			Model:            partitionModel1Primary1Replica,
			expNumWarnings:   0,
		},
		{
			About: "8 partitions, 4 nodes, increase partition 000, 004 weight",
			FromTo: [][]string{
				//        abcd    abcd
				{"sm  ", " m s"}, // partition 000.
				{"  ms", " s m"},
				{"s  m", "  sm"},
				{" ms ", "m s "},
				{" sm ", "s m "}, // partition 004.
				{" s m", " s m"},
				{"ms  ", "ms  "},
				{"m s ", "m s "},
			},
			Nodes:            []string{"a", "b", "c", "d"},
			NodesToRemove:    []string{},
			NodesToAdd:       []string{},
			PartitionWeights: map[string]int{"000": 100, "004": 100},
			Model:            partitionModel1Primary1Replica,
			expNumWarnings:   0,
		},
		{
			// Primaries stayed nicely stable during node removal.
			// TODO: But, perhaps node a has too much load.
			About: "4 nodes to 3 nodes, remove node d, high stickiness",
			FromTo: [][]string{
				//        abcd    abc
				{" m s", "sm "},
				{"  ms", "s m"},
				{"s  m", "m s"},
				{" ms ", " ms"},
				{" sm ", " sm"},
				{"s  m", "sm "},
				{"ms  ", "ms "},
				{"m s ", "m s"},
			},
			Nodes:           []string{"a", "b", "c", "d"},
			NodesToRemove:   []string{"d"},
			NodesToAdd:      []string{},
			Model:           partitionModel1Primary1Replica,
			StateStickiness: map[string]int{"primary": 1000000},
			expNumWarnings:  0,
		},
		{
			About: "3 partitions, 2 nodes add 1 node, sm first",
			FromTo: [][]string{
				//        ab    abc
				{"sm", "s m"},
				{"ms", "ms "},
				{"sm", " ms"},
			},
			Nodes:          []string{"a", "b", "c"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{},
			Model:          partitionModel1Primary1Replica,
			expNumWarnings: 0,
		},
		{
			About: "3 partitions, 2 nodes add 1 node, ms first",
			FromTo: [][]string{
				//        ab    abc
				{"ms", " sm"},
				{"sm", "sm "},
				{"ms", "m s"},
			},
			Nodes:          []string{"a", "b", "c"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{},
			Model:          partitionModel1Primary1Replica,
			expNumWarnings: 0,
		},
		{
			About: "8 partitions, 2 nodes add 1 node",
			// ISSUE: result does not have 2nd order of balance'd-ness.
			FromTo: [][]string{
				//        ab    abc
				{"sm", "s m"},
				{"sm", "s m"},
				{"sm", " ms"},
				{"sm", " ms"},
				{"ms", "s m"},
				{"ms", "ms "},
				{"ms", "ms "},
				{"ms", "ms "},
			},
			Nodes:          []string{"a", "b", "c"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{},
			Model:          partitionModel1Primary1Replica,
			expNumWarnings: 0,
		},
		{
			About: "8 partitions, 2 nodes add 1 node, flipped ms",
			// ISSUE: result does not have 2nd order of balance'd-ness.
			FromTo: [][]string{
				//        ab    abc
				{"ms", " sm"},
				{"ms", " sm"},
				{"ms", "m s"},
				{"ms", "m s"},
				{"sm", " sm"},
				{"sm", "sm "},
				{"sm", "sm "},
				{"sm", "sm "},
			},
			Nodes:          []string{"a", "b", "c"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{},
			Model:          partitionModel1Primary1Replica,
			expNumWarnings: 0,
		},
		{
			About: "8 partitions, 2 nodes add 1 node, interleaved m's",
			// ISSUE: not enough partitions moved: c has less than a &
			// b, especially replicas; but it has some 2nd order
			// balance'd-ness.
			FromTo: [][]string{
				//        ab    abc
				{"ms", " sm"},
				{"sm", "s m"},
				{"ms", "m s"},
				{"sm", " ms"},
				{"ms", "ms "},
				{"sm", "sm "},
				{"ms", "ms "},
				{"sm", "sm "},
			},
			Nodes:          []string{"a", "b", "c"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{},
			Model:          partitionModel1Primary1Replica,
			expNumWarnings: 0,
		},
		{
			About: "8 partitions, 2 nodes add 1 node, interleaved s'm",
			// ISSUE: not enough partitions moved: c has less than a &
			// b, especially replicas; but it has some 2nd order
			// balance'd-ness.
			FromTo: [][]string{
				//        ab    abc
				{"sm", "s m"},
				{"ms", " sm"},
				{"sm", " ms"},
				{"ms", "m s"},
				{"sm", "sm "},
				{"ms", "ms "},
				{"sm", "sm "},
				{"ms", "ms "},
			},
			Nodes:          []string{"a", "b", "c"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{},
			Model:          partitionModel1Primary1Replica,
			expNumWarnings: 0,
		},
	}
	testVisTestCases(t, tests)
}

func TestPlanNextMapHierarchy(t *testing.T) {
	partitionModel1Primary1Replica := PartitionModel{
		"primary": &PartitionModelState{
			Priority: 0, Constraints: 1,
		},
		"replica": &PartitionModelState{
			Priority: 1, Constraints: 1,
		},
	}
	nodeHierarchy2Rack := map[string]string{
		"a": "r0",
		"b": "r0",
		"c": "r1",
		"d": "r1",
		"e": "r1",

		// Racks r0 and r1 in the same zone z0.
		"r0": "z0",
		"r1": "z0",
	}
	hierarchyRulesWantSameRack := HierarchyRules{
		"replica": []*HierarchyRule{
			{
				IncludeLevel: 1,
				ExcludeLevel: 0,
			},
		},
	}
	hierarchyRulesWantOtherRack := HierarchyRules{
		"replica": []*HierarchyRule{
			{
				IncludeLevel: 2,
				ExcludeLevel: 1,
			},
		},
	}
	tests := []VisTestCase{
		{
			About: "2 racks, but nil hierarchy rules",
			FromTo: [][]string{
				//            abcd
				{"", "ms  "},
				{"", "sm  "},
				{"", "  ms"},
				{"", "  sm"},
				{"", "m s "},
				{"", " m s"},
				{"", "s m "},
				{"", " s m"},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"a", "b", "c", "d"},
			Model:          partitionModel1Primary1Replica,
			NodeHierarchy:  nodeHierarchy2Rack,
			HierarchyRules: nil,
			expNumWarnings: 0,
		},
		{
			About: "2 racks, favor same rack for replica",
			FromTo: [][]string{
				//            abcd
				{"", "ms  "},
				{"", "sm  "},
				{"", "  ms"},
				{"", "  sm"},
				{"", "ms  "},
				{"", "sm  "},
				{"", "  ms"},
				{"", "  sm"},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"a", "b", "c", "d"},
			Model:          partitionModel1Primary1Replica,
			NodeHierarchy:  nodeHierarchy2Rack,
			HierarchyRules: hierarchyRulesWantSameRack,
			expNumWarnings: 0,
		},
		{
			About: "2 racks, favor other rack for replica",
			FromTo: [][]string{
				//            abcd
				{"", "m s "},
				{"", " m s"},
				{"", "s m "},
				{"", " s m"},
				{"", "m  s"},
				{"", " ms "},
				{"", " sm "},
				{"", "s  m"},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"a", "b", "c", "d"},
			Model:          partitionModel1Primary1Replica,
			NodeHierarchy:  nodeHierarchy2Rack,
			HierarchyRules: hierarchyRulesWantOtherRack,
			expNumWarnings: 0,
		},
		{
			About: "2 racks, add node to 2nd rack",
			FromTo: [][]string{
				//        abcd    abcde
				{"m s ", "s   m"},
				{" m s", " m  s"},
				{"s m ", "s m  "},
				{" s m", " s m "},
				{"m  s", "m  s "},
				{" ms ", " ms  "},
				{" sm ", " sm  "},
				{"s  m", "s  m "},
			},
			Nodes:          []string{"a", "b", "c", "d", "e"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"e"},
			Model:          partitionModel1Primary1Replica,
			NodeHierarchy:  nodeHierarchy2Rack,
			HierarchyRules: hierarchyRulesWantOtherRack,
			expNumWarnings: 0,
		},
		{
			// NOTE: following the hierarchy rules for replicas, node a
			// takes on undue burden after removing node b,
			About: "2 racks, remove 1 node from rack 1",
			FromTo: [][]string{
				//        abcd    abcd
				{"m s ", "m s "},
				{" m s", "m  s"},
				{"s m ", "s m "},
				{" s m", "s  m"},
				{"m  s", "m  s"},
				{" ms ", "s m "},
				{" sm ", "s m "},
				{"s  m", "s  m"},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{"b"},
			NodesToAdd:     []string{},
			Model:          partitionModel1Primary1Replica,
			NodeHierarchy:  nodeHierarchy2Rack,
			HierarchyRules: hierarchyRulesWantOtherRack,
			expNumWarnings: 0,
		},
	}
	testVisTestCases(t, tests)
}

func TestMultiPrimary(t *testing.T) {
	partitionModel2Primary0Replica := PartitionModel{
		"primary": &PartitionModelState{
			Priority: 0, Constraints: 2,
		},
	}
	tests := []VisTestCase{
		{
			About: "1 node",
			FromTo: [][]string{
				//            a
				{"", "m"},
				{"", "m"},
				{"", "m"},
				{"", "m"},
				{"", "m"},
				{"", "m"},
				{"", "m"},
				{"", "m"},
			},
			Nodes:          []string{"a"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"a"},
			Model:          partitionModel2Primary0Replica,
			expNumWarnings: 8,
		},
		{
			// TODO: This seems like a bad layout.
			About: "4 nodes",
			FromTo: [][]string{
				//            abcd
				{"", "mm  "},
				{"", "  mm"},
				{"", "mm  "},
				{"", "  mm"},
				{"", "mm  "},
				{"", "  mm"},
				{"", "mm  "},
				{"", "  mm"},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"a", "b", "c", "d"},
			Model:          partitionModel2Primary0Replica,
			expNumWarnings: 0,
		},
		{
			About: "4 node stability",
			FromTo: [][]string{
				//        abcd
				{"mm  ", "mm  "},
				{"  mm", "  mm"},
				{"mm  ", "mm  "},
				{"  mm", "  mm"},
				{"mm  ", "mm  "},
				{"  mm", "  mm"},
				{"mm  ", "mm  "},
				{"  mm", "  mm"},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"a", "b", "c", "d"},
			Model:          partitionModel2Primary0Replica,
			expNumWarnings: 0,
		},
		{
			// TODO: Test harness isn't powerful enough to encode this case
			// of [c,d] versus [d,c].
			Ignore: true,
			About:  "4 node remove 1 node",
			FromTo: [][]string{
				//        abcd    abcd
				{"mm  ", " mm "},
				{"  mm", "  mm"},
				{"mm  ", " m m"},
				{"  mm", "  mm"},
				{"mm  ", " mm "},
				{"  mm", " mm "},
				{"mm  ", " m m"},
				// TODO: result is [d,c], but expected can only say [c,d].
				{"  mm", "  mm"},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{"a"},
			NodesToAdd:     []string{},
			Model:          partitionModel2Primary0Replica,
			expNumWarnings: 0,
		},
		{
			// TODO: Test harness isn't powerful enough to encode this case
			// of [b,d] versus [d,b].
			Ignore: true,
			About:  "4 node remove 2 nodes",
			FromTo: [][]string{
				//        abcd    abcd
				{"mm  ", " m m"},
				{"  mm", " m m"},
				{"mm  ", " m m"},
				{"  mm", " m m"},
				{"mm  ", " m m"},
				{"  mm", " m m"},
				{"mm  ", " m m"},
				// TODO: result is [d,c], but expected can only say [c,d].
				{"  mm", "  mm"},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{"a", "c"},
			NodesToAdd:     []string{},
			Model:          partitionModel2Primary0Replica,
			expNumWarnings: 0,
		},
	}
	testVisTestCases(t, tests)
}

func Test2Replicas(t *testing.T) {
	partitionModel1Primary2Replica := PartitionModel{
		"primary": &PartitionModelState{
			Priority: 0, Constraints: 1,
		},
		"replica": &PartitionModelState{
			Priority: 1, Constraints: 2,
		},
	}
	tests := []VisTestCase{
		{
			About: "8 partitions, 1 primary, 2 replicas, from 0 to 4 nodes",
			FromTo: [][]string{
				//            a b c d
				{"", "m0s0s1  "},
				{"", "s0m0  s1"},
				{"", "s0s1m0  "},
				{"", "s0  s1m0"},
				{"", "m0s1  s0"},
				{"", "  m0s0s1"},
				{"", "s1  m0s0"},
				{"", "  s0s1m0"},
			},
			FromToPriority: true,
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"a", "b", "c", "d"},
			Model:          partitionModel1Primary2Replica,
			expNumWarnings: 0,
		},
		{
			About: "8 partitions, reconverge 1 primary, 2 replicas, from 4 to 4 nodes",
			FromTo: [][]string{
				//        a b c d     a b c d
				{"m0s0s1  ", "m0s0s1  "},
				{"s0m0  s1", "s0m0  s1"},
				{"s0s1m0  ", "s0s1m0  "},
				{"s1  s0m0", "s0  s1m0"}, // Flipped replicas reconverges.
				{"m0s1  s0", "m0s1  s0"},
				{"  m0s0s1", "  m0s0s1"},
				{"s1  m0s0", "s1  m0s0"},
				{"  s0s1m0", "  s0s1m0"},
			},
			FromToPriority: true,
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{},
			Model:          partitionModel1Primary2Replica,
			expNumWarnings: 0,
		},
		{ // Try case where number of nodes isn't a factor of # partitions.
			About: "7 partitions, 1 primary, 2 replicas, from 0 to 4 nodes",
			FromTo: [][]string{
				//            a b c d
				{"", "m0s0  s1"},
				{"", "s1m0s0  "},
				{"", "s1  m0s0"},
				{"", "  s0s1m0"},
				{"", "m0  s0s1"},
				{"", "s1m0  s0"},
				{"", "s1s0m0  "},
			},
			FromToPriority: true,
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"a", "b", "c", "d"},
			Model:          partitionModel1Primary2Replica,
			expNumWarnings: 0,
		},
		{
			About: "7 partitions, reconverge 1 primary, 2 replicas, from 4 to 4 nodes",
			FromTo: [][]string{
				//        a b c d     a b c d
				{"m0s0  s1", "m0s0  s1"},
				{"s1m0s0  ", "s1m0s0  "},
				{"s1  m0s0", "s1  m0s0"},
				{"  s0s1m0", "  s0s1m0"},
				{"m0  s0s1", "m0  s0s1"},
				{"s1m0  s0", "s1m0  s0"},
				{"s1s0m0  ", "s1s0m0  "},
			},
			FromToPriority: true,
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{},
			Model:          partitionModel1Primary2Replica,
			expNumWarnings: 0,
		},
		{
			About: "16 partitions, 1 primary, 2 replicas, from 0 to 4 nodes",
			FromTo: [][]string{
				//            a b c d
				{"", "m0s0s1  "},
				{"", "s0m0  s1"},
				{"", "  s0m0s1"},
				{"", "s0  s1m0"},
				{"", "m0s1  s0"},
				{"", "  m0s0s1"},
				{"", "s0  m0s1"},
				{"", "  s0s1m0"},
				{"", "m0  s0s1"},
				{"", "s0m0s1  "},
				{"", "  s0m0s1"},
				{"", "s0s1  m0"},
				{"", "m0s0s1  "},
				{"", "s0m0  s1"},
				{"", "s0s1m0  "},
				{"", "s0  s1m0"},
			},
			FromToPriority: true,
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"a", "b", "c", "d"},
			Model:          partitionModel1Primary2Replica,
			expNumWarnings: 0,
		},
		{
			About: "re-feed 16 partitions, 1 primary, 2 replicas, from 4 to 4 nodes",
			FromTo: [][]string{
				//        a b c d     a b c d
				{"m0s0s1  ", "m0s0s1  "},
				{"s0m0  s1", "s0m0  s1"},
				{"  s0m0s1", "  s0m0s1"},
				{"s0  s1m0", "s0  s1m0"},
				{"m0s1  s0", "m0s1  s0"},
				{"  m0s0s1", "  m0s0s1"},
				{"s0  m0s1", "s0  m0s1"},
				{"  s0s1m0", "  s0s1m0"},
				{"m0  s0s1", "m0  s0s1"},
				{"s0m0s1  ", "s0m0s1  "},
				{"  s0m0s1", "  s0m0s1"},
				{"s0s1  m0", "s0s1  m0"},
				{"m0s0s1  ", "m0s0s1  "},
				{"s0m0  s1", "s0m0  s1"},
				{"s0s1m0  ", "s0s1m0  "},
				{"s0  s1m0", "s0  s1m0"},
			},
			FromToPriority: true,
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{},
			Model:          partitionModel1Primary2Replica,
			expNumWarnings: 0,
		},
	}
	testVisTestCases(t, tests)
}
