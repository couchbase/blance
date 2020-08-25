//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

// Package blance provides a partition assignment library, using a
// greedy, heuristic, functional approach.  It supports multiple,
// configurable partition states (primary, replica, read-only, etc),
// configurable multi-level containment hierarchy
// (shelf/rack/zone/datacenter awareness) with inclusion/exclusion
// policies, heterogeneous partition weights, heterogeneous node
// weights, partition stickiness control, and multi-primary support.
package blance

// A PartitionMap represents all the partitions for some logical
// resource, where the partitions are assigned to different nodes and
// with different states.  For example, partition "A-thru-H" is
// assigned to node "x" as a "primary" and to node "y" as a "replica".
// And, partition "I-thru-Z" is assigned to node "y" as a "primary" and
// to nodes "z" & "x" as "replica".
type PartitionMap map[string]*Partition // Keyed by Partition.Name.

// A Partition represents a distinct, non-overlapping subset (or a
// shard) of some logical resource.
type Partition struct {
	// The Name of a Partition must be unique within a PartitionMap.
	Name string `json:"name"`

	// NodesByState is keyed is stateName, and the values are an array
	// of node names.  For example, {"primary": ["a"], "replica": ["b",
	// "c"]}.
	NodesByState map[string][]string `json:"nodesByState"`
}

// A PartitionModel lets applications define different states for each
// partition per node, such as "primary", "replica", "dead", etc.  Key is
// stateName, like "primary", "replica", "dead", etc.
type PartitionModel map[string]*PartitionModelState

// A PartitionModelState lets applications define metadata per
// partition model state.  For example, "primary" state should have
// different priority and constraints than a "replica" state.
type PartitionModelState struct {
	// Priority of zero is the highest.  e.g., "primary" Priority
	// should be < than "replica" Priority, so we can define that
	// as "primary" Priority of 0 and "replica" priority of 1.
	Priority int `json:"priority"`

	// A Constraint defines how many nodes the algorithm strives to
	// assign a partition.  For example, for any given partition,
	// perhaps the application wants 1 node to have "primary" state and
	// wants 2 nodes to have "replica" state.  That is, the "primary"
	// state has Constraints of 1, and the "replica" state has
	// Constraints of 2.  Continuing the example, when the "primary"
	// state has Priority of 0 and the "replica" state has Priority of
	// 1, then "primary" partitions will be assigned to nodes before
	// "replica" partitions.
	Constraints int `json:"constraints"`
}

// HierarchyRules example:
// {"replica":[{IncludeLevel:1,ExcludeLevel:0}]}, which means that after
// a partition is assigned to a node as primary, then assign the first
// replica to a node that is a close sibling node to the primary node
// (e.g., same parent or same rack).  Another example:
// {"replica":[{IncludeLevel:1,ExcludeLevel:0},
// {IncludeLevel:2,ExcludeLevel:1}]}, which means assign the first
// replica same as above, but assign the second replica to a node that is
// not a sibling of the primary (not the same parent, so to a different
// rack).
type HierarchyRules map[string][]*HierarchyRule

// A HierarchyRule is metadata for rack/zone awareness features.
// First, IncludeLevel is processed to find a set of candidate nodes.
// Then, ExcludeLevel is processed to remove or exclude nodes from
// that set.  For example, for this containment tree, (datacenter0
// (rack0 (nodeA nodeB)) (rack1 (nodeC nodeD))), lets focus on nodeA.
// If IncludeLevel is 1, that means go up 1 parent (so, from nodeA up
// to rack0) and then take all of rack0's leaves: nodeA and nodeB.
// So, the candidate nodes of nodeA and nodeB are all on the same rack
// as nodeA, or a "same rack" policy.  If instead the IncludeLevel was
// 2 and ExcludeLevel was 1, then that means a "different rack"
// policy.  With IncludeLevel of 2, we go up 2 ancestors from node A
// (from nodeA to rack0; and then from rack0 to datacenter0) to get to
// datacenter0.  The datacenter0 has leaves of nodeA, nodeB, nodeC,
// nodeD, so those nodes comprise the inclusion candidate set.  But,
// with ExcludeLevel of 1, that means we go up 1 parent from nodeA to
// rack0, take rack0's leaves, giving us an exclusion set of nodeA &
// nodeB.  The inclusion candidate set minus the exclusion set finally
// gives us just nodeC & nodeD as our final candidate nodes.  That
// final candidate set of nodes (just nodeC & nodeD) are from a
// different rack as nodeA.
type HierarchyRule struct {
	// IncludeLevel defines how many parents or ancestors to traverse
	// upwards in a containment hierarchy to find candidate nodes.
	IncludeLevel int `json:"includeLevel"`

	// ExcludeLevel defines how many parents or ancestors to traverse
	// upwards in a containment hierarchy to find an exclusion set of
	// nodes.
	ExcludeLevel int `json:"excludeLevel"`
}

// PlanNextMap is deprecated.  Applications should instead use the
// PlanNextMapEx() and PlanNextMapOptions API's.
func PlanNextMap(
	prevMap PartitionMap,
	nodesAll []string, // Union of nodesBefore, nodesToAdd, nodesToRemove.
	nodesToRemove []string,
	nodesToAdd []string,
	model PartitionModel,
	modelStateConstraints map[string]int, // Keyed by stateName.
	partitionWeights map[string]int, // Keyed by partitionName.
	stateStickiness map[string]int, // Keyed by stateName.
	nodeWeights map[string]int, // Keyed by node.
	nodeHierarchy map[string]string, // Keyed by node, value is node's parent.
	hierarchyRules HierarchyRules,
) (nextMap PartitionMap, warnings []string) {
	return PlanNextMapEx(prevMap, nodesAll, nodesToRemove, nodesToAdd,
		model, PlanNextMapOptions{
			ModelStateConstraints: modelStateConstraints,
			PartitionWeights:      partitionWeights,
			StateStickiness:       stateStickiness,
			NodeWeights:           nodeWeights,
			NodeHierarchy:         nodeHierarchy,
			HierarchyRules:        hierarchyRules,
		})
}

// PlanNextMapEx is the main entry point to the algorithm to assign
// partitions to nodes.  The prevMap must define the partitions.
// Partitions must be stable between PlanNextMapEx() runs.  That is,
// splitting and merging or partitions are an orthogonal concern and
// must be done separately than PlanNextMapEx() invocations.  The
// nodeAll parameters is all nodes (union of existing nodes, nodes to
// be added, nodes to be removed, nodes that aren't changing).  The
// nodesToRemove may be empty.  The nodesToAdd may be empty.  When
// both nodesToRemove and nodesToAdd are empty, partitioning
// assignment may still change, as another PlanNextMapEx() invocation
// may reach more stabilization or balanced'ness.
func PlanNextMapEx(
	prevMap PartitionMap,
	nodesAll []string, // Union of nodesBefore, nodesToAdd, nodesToRemove.
	nodesToRemove []string,
	nodesToAdd []string,
	model PartitionModel,
	options PlanNextMapOptions) (nextMap PartitionMap, warnings []string) {
	return planNextMapEx(prevMap, nodesAll, nodesToRemove, nodesToAdd,
		model, options)
}

// PlanNextMapOptions represents optional parameters to the
// PlanNextMapEx() API.  The ModelStateConstraints allows the caller
// to override the constraints defined in the model.  The
// ModelStateConstraints is keyed by stateName (like "primary",
// "replica", etc).  The PartitionWeights is optional and is keyed by
// partitionName; it allows the caller to specify that some partitions
// are bigger than others (e.g., California has more records than
// Hawaii); default partition weight is 1.  The StateStickiness is
// optional and is keyed by stateName; it allows the caller to prefer
// not moving data at the tradeoff of potentially more imbalance;
// default state stickiness is 1.5.  The NodeWeights is optional and
// is keyed by node name; it allows the caller to specify that some
// nodes can hold more partitions than other nodes; default node
// weight is 1.  The NodeHierarchy defines optional parent
// relationships per node; it is keyed by node and a value is the
// node's parent.  The HierarchyRules allows the caller to optionally
// define replica placement policy (e.g., same/different rack;
// same/different zone; etc).
type PlanNextMapOptions struct {
	ModelStateConstraints map[string]int    // Keyed by stateName.
	PartitionWeights      map[string]int    // Keyed by partitionName.
	StateStickiness       map[string]int    // Keyed by stateName.
	NodeWeights           map[string]int    // Keyed by node.
	NodeHierarchy         map[string]string // Keyed by node; value is node's parent.
	HierarchyRules        HierarchyRules
}
