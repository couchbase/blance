//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package blance

import (
	"errors"
	"fmt"
	"sync"
)

// ErrorStopped is returned when an operation was stopped
var ErrorStopped = errors.New("stopped")

// ErrorInterrupt is returned when an operation was interrupted
var ErrorInterrupt = errors.New("interrupt")

/*
We let the app have detailed control of the prioritization heuristics
via the FindMoveFunc callback.  Here are some move prioritization
ideas or heuristics for FindMoveFunc implementors to consider.

Some apps might first favor easy, single-node promotions and demotions
(e.g., a replica partition graduating to primary on the same node)
because single-node state changes should be fast and so that clients
can have more coverage across all partitions.  The
LowestWeightPartitionMoveForNode() implementation does this now.

Next, favor assignments of partitions that have no replicas assigned
anywhere, where we want to get to that first data partition instance
or replica as soon as possible. Once we have that first replica for a
data partition, though, we should consider favoring other kinds of
moves over building even more replicas of that data partition.

Next, favor reassignments that utilize capacity on newly added nodes,
as the new nodes may be able to help with existing, overtaxed
nodes. But be aware: starting off more KV backfills, for example, may
push existing nodes running at the limit over the edge.

Next, favor reassignments that help get partitions off of nodes that
are leaving the cluster. The idea is to allow us to remove nodes
(which may need servicing) sooner.

Next, favor removals of partitions that are over-replicated. For
example, there might be too many replicas of a partition remaining on
new/existing nodes.

Lastly, favor reassignments that move partitions amongst nodes than
are neither joining nor leaving the cluster. In this case, the system
may need to shuffle partitions to achieve better balance or meet
replication constraints.

Other, more advanced factors to consider in the heuristics, which may
be addressed in future releases, but would just be additions to the
ordering/sorting algorithm.

Some nodes might be slower, less powerful and more impacted than
others.

Some partitions might be way behind compared to others.

Some partitions might be much larger than others.

Some partitions might have data sources under more pressure than
others and less able to handle yet another a request for a data source
full-scan (backfill).

Perhaps consider how about some randomness?
*/

// ------------------------------------------

// An Orchestrator instance holds the runtime state during an
// OrchestrateMoves() operation.
type Orchestrator struct {
	model PartitionModel

	options OrchestratorOptions

	nodesAll []string // Union of all nodes (entering, leaving, remaining).

	begMap PartitionMap // The map state that we start with.
	endMap PartitionMap // The map state we want to end up with.

	assignPartition AssignPartitionFunc
	findMove        FindMoveFunc

	progressCh chan OrchestratorProgress

	// Keyed by node name.
	mapNodeToPartitionMoveReqCh map[string]chan partitionMoveReq

	m sync.Mutex // Protects the fields that follow.

	stopCh   chan struct{} // Becomes nil when stopped.
	pauseCh  chan struct{} // May be nil; non-nil when paused.
	progress OrchestratorProgress

	// Keyed by partition name.
	mapPartitionToNextMoves map[string]*NextMoves
}

// OrchestratorOptions represents advanced config parameters for
// OrchestrateMoves().
type OrchestratorOptions struct {
	MaxConcurrentPartitionMovesPerNode int

	// See blance.CalcPartitionMoves(favorMinNodes).
	FavorMinNodes bool
}

// OrchestratorProgress represents progress counters and/or error
// information as the OrchestrateMoves() operation proceeds.
type OrchestratorProgress struct {
	Errors []error

	TotStop                      int
	TotPauseNewAssignments       int
	TotResumeNewAssignments      int
	TotRunMover                  int
	TotRunMoverDone              int
	TotRunMoverDoneErr           int
	TotMoverLoop                 int
	TotMoverAssignPartition      int
	TotMoverAssignPartitionOk    int
	TotMoverAssignPartitionErr   int
	TotRunSupplyMovesLoop        int
	TotRunSupplyMovesLoopDone    int
	TotRunSupplyMovesFeeding     int
	TotRunSupplyMovesFeedingDone int
	TotRunSupplyMovesDone        int
	TotRunSupplyMovesDoneErr     int
	TotRunSupplyMovesPause       int
	TotRunSupplyMovesResume      int
	TotProgressClose             int
}

// AssignPartitionFunc is a callback invoked by OrchestrateMoves()
// when it wants to synchronously assign a partition to a node at a
// given state, or change the state of an existing partition on a
// node.  The state will be "" if the partition should be removed or
// deleted from the node.
type AssignPartitionFunc func(stopCh chan struct{},
	partition string,
	node string,
	state string,
	op string) error

// FindMoveFunc is a callback invoked by OrchestrateMoves() when it
// wants to find the best partition move out of a set of available
// partition moves for node.  It should return the array index of the
// partition move that should be used next.
type FindMoveFunc func(node string, moves []PartitionMove) int

// A PartitionMove struct represents a state change or operation on a
// partition on a node.
type PartitionMove struct {
	Partition string

	Node string

	// Ex: "primary", "replica".
	State string

	// Same as NodeStateOp.Op: "add", "del", "promote", "demote".
	Op string
}

// LowestWeightPartitionMoveForNode implements the FindMoveFunc
// callback signature, by using the MoveOpWeight lookup table to find
// the lowest weight partition move for a node.
func LowestWeightPartitionMoveForNode(
	node string, moves []PartitionMove) int {
	r := 0
	for i, move := range moves {
		if MoveOpWeight[moves[r].Op] > MoveOpWeight[move.Op] {
			r = i
		}
	}
	return r
}

// MoveOpWeight sets the weight associated with each op
var MoveOpWeight = map[string]int{
	"promote": 1,
	"demote":  2,
	"add":     3,
	"del":     4,
}

// A NextMoves struct is used to track a sequence of moves of a
// partition, including the next move that that needs to be taken.
type NextMoves struct {
	Partition string // Immutable.

	// Mutable index or current position in the moves array that
	// represents the next available move for a partition.
	Next int

	// The sequence of moves can come from the output of the
	// CalcPartitionMoves() function and is immutable.
	Moves []NodeStateOp

	// When non-nil, it means the move is already in-flight (was
	// successfully fed to a mover) but hasn't finished yet, and the
	// move supplier needs to wait for the nextDoneCh to be closed.
	// The nextDoneCh == partitionMoveReq.doneCh.
	nextDoneCh chan error
}

// ------------------------------------------

// A partitionMoveReq wraps a partitionMove, allowing the receiver (a
// mover) to signal that the move is completed by closing the doneCh.
type partitionMoveReq struct {
	partitionMove PartitionMove
	doneCh        chan error
}

// ------------------------------------------

// OrchestrateMoves asynchronously begins reassigning partitions
// amongst nodes in order to transition from the begMap to the endMap
// state, invoking the assignPartition() to affect changes.
// Additionally, the caller must read the progress channel until it's
// closed by OrchestrateMoves to avoid blocking the orchestration, and
// as a way to monitor progress.
//
// The nodesAll must be a union or superset of all the nodes during
// the orchestration (nodes added, removed, unchanged).
//
// The findMove callback is invoked when OrchestrateMoves needs to
// find the best move for a node from amongst a set of available
// moves.
func OrchestrateMoves(
	model PartitionModel,
	options OrchestratorOptions,
	nodesAll []string,
	begMap PartitionMap,
	endMap PartitionMap,
	assignPartition AssignPartitionFunc,
	findMove FindMoveFunc,
) (*Orchestrator, error) {
	if len(begMap) != len(endMap) {
		return nil, fmt.Errorf("mismatched begMap and endMap")
	}

	// Populate the mapNodeToPartitionMoveReqCh, keyed by node name.
	mapNodeToPartitionMoveReqCh := map[string]chan partitionMoveReq{}
	for _, node := range nodesAll {
		mapNodeToPartitionMoveReqCh[node] = make(chan partitionMoveReq)
	}

	states := sortStateNames(model)

	// Populate the mapPartitionToNextMoves, keyed by partition name,
	// with the output from CalcPartitionMoves().
	//
	// As an analogy, this step calculates a bunch of airplane flight
	// plans, without consideration to what the other airplanes are
	// doing, where each flight plan has multi-city, multi-leg hops.
	mapPartitionToNextMoves := map[string]*NextMoves{}

	for partitionName, begPartition := range begMap {
		endPartition := endMap[partitionName]

		moves := CalcPartitionMoves(states,
			begPartition.NodesByState,
			endPartition.NodesByState,
			options.FavorMinNodes,
		)

		mapPartitionToNextMoves[partitionName] = &NextMoves{
			Partition: partitionName,
			Next:      0,
			Moves:     moves,
		}
	}

	o := &Orchestrator{
		model:           model,
		options:         options,
		nodesAll:        nodesAll,
		begMap:          begMap,
		endMap:          endMap,
		assignPartition: assignPartition,
		findMove:        findMove,
		progressCh:      make(chan OrchestratorProgress),

		mapNodeToPartitionMoveReqCh: mapNodeToPartitionMoveReqCh,

		stopCh:  make(chan struct{}),
		pauseCh: nil,

		mapPartitionToNextMoves: mapPartitionToNextMoves,
	}

	stopCh := o.stopCh

	runMoverDoneCh := make(chan error)

	// Start concurrent movers.
	//
	// Following the airplane analogy, a runMover() represents
	// a takeoff runway at a city airport (or node).  There can
	// be multiple takeoff runways at a city's airport (which is
	// controlled by MaxConcurrentPartitionMovesPerNode).
	m := options.MaxConcurrentPartitionMovesPerNode
	if m < 1 {
		m = 1
	}

	for _, node := range o.nodesAll {
		for i := 0; i < m; i++ {
			go o.runMover(stopCh, runMoverDoneCh, node)
		}
	}

	// Supply moves to movers.
	//
	// Following the airplane/airport analogy, a runSupplyMoves()
	// goroutine is like some global, supreme airport controller,
	// remotely controlling all the city airports across the entire
	// realm, and deciding which plane can take off next at each
	// airport.  Each plane is following its multi-leg flight plan
	// that was computed from earlier (via CalcPartitionMoves), but
	// when multiple planes are concurrently ready to takeoff from a
	// city's airport (or node), this global, supreme airport
	// controller chooses which plane (or partition) gets to takeoff
	// next.
	go o.runSupplyMoves(stopCh, m, runMoverDoneCh)

	return o, nil
}

// Stop asynchronously requests the orchestrator to stop, where the
// caller will eventually see a closed progress channel.
func (o *Orchestrator) Stop() {
	o.m.Lock()
	if o.stopCh != nil {
		o.progress.TotStop++
		close(o.stopCh)
		o.stopCh = nil
	}
	o.m.Unlock()
}

// ProgressCh returns a channel that is updated occasionally when
// the orchestrator has made some progress on one or more partition
// reassignments, or has reached an error.  The channel is closed by
// the orchestrator when it is finished, either naturally, or due to
// an error, or via a Stop(), and all the orchestrator's resources
// have been released.
func (o *Orchestrator) ProgressCh() chan OrchestratorProgress {
	return o.progressCh
}

// PauseNewAssignments disallows the orchestrator from starting any
// new assignments of partitions to nodes.  Any inflight partition
// moves will continue to be finished.  The caller can monitor the
// ProgressCh to determine when to pause and/or resume partition
// assignments.  PauseNewAssignments is idempotent.
func (o *Orchestrator) PauseNewAssignments() error {
	o.m.Lock()
	if o.pauseCh == nil {
		o.pauseCh = make(chan struct{})
		o.progress.TotPauseNewAssignments++
	}
	o.m.Unlock()
	return nil
}

// ResumeNewAssignments tells the orchestrator that it may resume
// assignments of partitions to nodes, and is idempotent.
func (o *Orchestrator) ResumeNewAssignments() error {
	o.m.Lock()
	if o.pauseCh != nil {
		o.progress.TotResumeNewAssignments++
		close(o.pauseCh)
		o.pauseCh = nil
	}
	o.m.Unlock()
	return nil
}

// -------------------------------------------------

// VisitNextMoves invokes the supplied callback with the map of
// partitions to *NextMoves, which should be treated as immutable by
// the callback.
func (o *Orchestrator) VisitNextMoves(cb func(map[string]*NextMoves)) {
	o.m.Lock()
	cb(o.mapPartitionToNextMoves)
	o.m.Unlock()
}

// -------------------------------------------------

// runMover handles partition moves for a single node.
//
// There may be >1 runMover's for a single node for higher
// concurrency, just as a city airport might have more than one
// takeoff runway.
func (o *Orchestrator) runMover(
	stopCh chan struct{}, runMoverDoneCh chan error, node string) {
	o.updateProgress(func() {
		o.progress.TotRunMover++
	})

	// The partitionMoveReqCh has commands from the global, supreme
	// airport controller on which airplane (or partition) should
	// takeoff from the city airport next (but the supreme airport
	// controller doesn't care which takeoff runway at that airport is
	// used).
	partitionMoveReqCh := o.mapNodeToPartitionMoveReqCh[node]

	runMoverDoneCh <- o.moverLoop(stopCh, partitionMoveReqCh, node)
}

// moverLoop handles partitionMoveReq's by invoking the
// assignPartition callback.
func (o *Orchestrator) moverLoop(stopCh chan struct{},
	partitionMoveReqCh chan partitionMoveReq, node string) error {
	for {
		o.updateProgress(func() {
			o.progress.TotMoverLoop++
		})

		select {
		case <-stopCh:
			return nil

		case partitionMoveReqVal, ok := <-partitionMoveReqCh:
			if !ok {
				return nil
			}

			partitionMove := partitionMoveReqVal.partitionMove
			partition := partitionMove.Partition
			state := partitionMove.State

			o.updateProgress(func() {
				o.progress.TotMoverAssignPartition++
			})

			err := o.assignPartition(stopCh,
				partition, node, state, partitionMove.Op)

			o.updateProgress(func() {
				if err != nil {
					o.progress.TotMoverAssignPartitionErr++
				} else {
					o.progress.TotMoverAssignPartitionOk++
				}
			})

			if partitionMoveReqVal.doneCh != nil {
				if err != nil {
					select {
					case <-stopCh:
						// NO-OP.
					case partitionMoveReqVal.doneCh <- err:
						// NO-OP.
					}
				}

				close(partitionMoveReqVal.doneCh)
			}
		}
	}
}

// runSupplyMoves "broadcasts" available partitionMoveReq's to movers.
// The broadcast is implemented via repeated "rounds" of spawning off
// concurrent helper goroutines of runSupplyMove()'s for each node.
func (o *Orchestrator) runSupplyMoves(stopCh chan struct{},
	m int, runMoverDoneCh chan error) {
	var errOuter error

	for errOuter == nil {
		o.updateProgress(func() {
			o.progress.TotRunSupplyMovesLoop++
		})

		o.m.Lock()

		// The availableMoves is keyed by node name.
		availableMoves := o.findAvailableMovesUnlocked()

		pauseCh := o.pauseCh

		o.m.Unlock()

		if len(availableMoves) <= 0 {
			break
		}

		// The main pause/resume handling is via pausing/resuming the
		// runSupplyMoves loop.  If caller needs to rebalancer.Stop()
		// while paused, they should resume before Stop()'ing.
		if pauseCh != nil {
			o.updateProgress(func() {
				o.progress.TotRunSupplyMovesPause++
			})

			<-pauseCh

			o.updateProgress(func() {
				o.progress.TotRunSupplyMovesResume++
			})
		}

		// Broadcast to every node mover their next, best move.
		broadcastStopCh := make(chan struct{})
		broadcastDoneCh := make(chan error)

		for node, nextMovesArr := range availableMoves {
			go o.runSupplyMove(stopCh, node,
				o.findNextMoves(node, nextMovesArr),
				broadcastStopCh, broadcastDoneCh)
		}

		o.updateProgress(func() {
			o.progress.TotRunSupplyMovesFeeding++
		})

		// When the one or more node movers is successfully "fed" (via
		// broadcastDoneCh), then stop the broadcast (via
		// broadcastStopCh) so that we can repeat the outer loop to
		// re-calculate another round of available moves.
		broadcastStopChClosed := false

		for range availableMoves {
			err := <-broadcastDoneCh
			if err == nil && !broadcastStopChClosed {
				close(broadcastStopCh)
				broadcastStopChClosed = true
			}

			if err != nil &&
				err != ErrorInterrupt &&
				errOuter == nil {
				errOuter = err
			}
		}

		o.updateProgress(func() {
			o.progress.TotRunSupplyMovesFeedingDone++
		})

		if !broadcastStopChClosed {
			close(broadcastStopCh)
		}

		close(broadcastDoneCh)
	}

	o.updateProgress(func() {
		o.progress.TotRunSupplyMovesLoopDone++
	})

	for _, partitionMoveReqCh := range o.mapNodeToPartitionMoveReqCh {
		close(partitionMoveReqCh)
	}

	o.updateProgress(func() {
		o.progress.TotRunSupplyMovesDone++
		if errOuter != nil &&
			errOuter != ErrorStopped {
			o.progress.Errors = append(o.progress.Errors, errOuter)
			o.progress.TotRunSupplyMovesDoneErr++
		}
	})

	// Wait for movers to finish.
	o.waitForAllMoversDone(m, runMoverDoneCh)

	o.updateProgress(func() {
		o.progress.TotProgressClose++
	})

	close(o.progressCh)
}

// runSupplyMove tries to send a single partitionMoveReq to a single
// node, along with handling the broadcast interruptions.
func (o *Orchestrator) runSupplyMove(stopCh chan struct{},
	node string, nextMoves *NextMoves,
	broadcastStopCh chan struct{},
	broadcastDoneCh chan error) {
	o.m.Lock()
	nodeStateOp := nextMoves.Moves[nextMoves.Next]
	nextDoneCh := nextMoves.nextDoneCh
	o.m.Unlock()

	if nextDoneCh == nil {
		nextDoneCh = make(chan error)

		pmr := partitionMoveReq{
			partitionMove: PartitionMove{
				Partition: nextMoves.Partition,
				Node:      nodeStateOp.Node,
				State:     nodeStateOp.State,
				Op:        nodeStateOp.Op,
			},
			doneCh: nextDoneCh,
		}

		select {
		case <-stopCh:
			broadcastDoneCh <- ErrorStopped
			return

		case <-broadcastStopCh:
			broadcastDoneCh <- ErrorInterrupt
			return

		case o.mapNodeToPartitionMoveReqCh[node] <- pmr:
			o.m.Lock()
			nextMoves.nextDoneCh = nextDoneCh
			o.m.Unlock()
		}
	}

	select {
	case <-stopCh:
		broadcastDoneCh <- ErrorStopped

	case <-broadcastStopCh:
		broadcastDoneCh <- ErrorInterrupt

	case err := <-nextDoneCh:
		o.m.Lock()
		nextMoves.nextDoneCh = nil
		nextMoves.Next++
		o.m.Unlock()

		broadcastDoneCh <- err
	}
}

// findNextMoves invokes the application's FindMoveFunc callback.
func (o *Orchestrator) findNextMoves(
	node string, nextMovesArr []*NextMoves) *NextMoves {
	moves := make([]PartitionMove, len(nextMovesArr))

	for i, nextMoves := range nextMovesArr {
		m := nextMoves.Moves[nextMoves.Next]

		moves[i] = PartitionMove{
			Partition: nextMoves.Partition,
			Node:      m.Node,
			State:     m.State,
			Op:        m.Op,
		}
	}

	return nextMovesArr[o.findMove(node, moves)]
}

// waitForAllMoversDone returns when all concurrent movers have
// finished, propagating any of their errors to the progressCh.
func (o *Orchestrator) waitForAllMoversDone(
	m int, runMoverDoneCh chan error) {
	for i := 0; i < len(o.nodesAll)*m; i++ {
		err := <-runMoverDoneCh

		o.updateProgress(func() {
			o.progress.TotRunMoverDone++
			if err != nil {
				o.progress.Errors = append(o.progress.Errors, err)
				o.progress.TotRunMoverDoneErr++
			}
		})
	}
}

// updateProgress is a helper func to allow for progress updates and
// sends progress events to the progressCh.
func (o *Orchestrator) updateProgress(f func()) {
	o.m.Lock()

	f()

	progress := o.progress

	o.m.Unlock()

	o.progressCh <- progress
}

// findAvailableMovesUnlocked returns the next round of available
// moves.
func (o *Orchestrator) findAvailableMovesUnlocked() (
	availableMoves map[string][]*NextMoves) {
	// The availableMoves is keyed by node name.
	availableMoves = map[string][]*NextMoves{}

	for _, nextMoves := range o.mapPartitionToNextMoves {
		if nextMoves.Next < len(nextMoves.Moves) {
			node := nextMoves.Moves[nextMoves.Next].Node
			availableMoves[node] =
				append(availableMoves[node], nextMoves)
		}
	}

	return availableMoves
}
