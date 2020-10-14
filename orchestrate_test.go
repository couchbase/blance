package blance

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
)

type assignPartitionRec struct {
	partition string
	node      string
	state     string
	op        string
}

var mrPartitionModel = PartitionModel{
	"primary": &PartitionModelState{
		Priority: 0,
	},
	"replica": &PartitionModelState{
		Constraints: 1,
	},
}

var options1 = OrchestratorOptions{
	MaxConcurrentPartitionMovesPerNode: 1,
}

func TestOrchestrateBadMoves(t *testing.T) {
	o, err := OrchestrateMoves(
		mrPartitionModel,
		options1,
		nil,
		PartitionMap{
			"00": &Partition{
				Name:         "00",
				NodesByState: map[string][]string{},
			},
			"01": &Partition{
				Name:         "01",
				NodesByState: map[string][]string{},
			},
		},
		PartitionMap{
			"01": &Partition{
				Name:         "01",
				NodesByState: map[string][]string{},
			},
		},
		nil,
		nil,
	)
	if err == nil || o != nil {
		t.Errorf("expected err on mismatched beg/end maps")
	}
}

func TestOrchestrateErrAssignPartitionFunc(t *testing.T) {
	theErr := fmt.Errorf("theErr")

	errAssignPartitionsFunc := func(stopCh chan struct{},
		node string, partition, state, op []string) error {
		return theErr
	}

	o, err := OrchestrateMoves(
		mrPartitionModel,
		OrchestratorOptions{},
		[]string{"a", "b"},
		PartitionMap{
			"00": &Partition{
				Name: "00",
				NodesByState: map[string][]string{
					"primary": {"a"},
				},
			},
		},
		PartitionMap{
			"00": &Partition{
				Name: "00",
				NodesByState: map[string][]string{
					"primary": {"b"},
				},
			},
		},
		errAssignPartitionsFunc,
		LowestWeightPartitionMoveForNode,
	)
	if err != nil || o == nil {
		t.Errorf("expected nil err")
	}

	gotProgress := 0
	var lastProgress OrchestratorProgress

	for progress := range o.ProgressCh() {
		gotProgress++
		lastProgress = progress
	}

	o.Stop()

	if gotProgress <= 0 {
		t.Errorf("expected progress")
	}

	if len(lastProgress.Errors) <= 0 {
		t.Errorf("expected errs")
	}

	o.VisitNextMoves(func(x map[string]*NextMoves) {
		if x == nil {
			t.Errorf("expected x")
		}
	})
}

func testMkFuncs() (
	map[string]map[string]string,
	map[string][]assignPartitionRec,
	AssignPartitionsFunc,
) {
	var m sync.Mutex

	// Map of partition -> node -> state.
	currStates := map[string]map[string]string{}

	assignPartitionRecs := map[string][]assignPartitionRec{}

	assignPartitionsFunc := func(stopCh chan struct{},
		node string, partitions, states, ops []string) error {
		m.Lock()

		assignPartitionRecs[partitions[0]] =
			append(assignPartitionRecs[partitions[0]],
				assignPartitionRec{partitions[0], node, states[0], ops[0]})

		nodes := currStates[partitions[0]]
		if nodes == nil {
			nodes = map[string]string{}
			currStates[partitions[0]] = nodes
		}

		nodes[node] = states[0]

		m.Unlock()

		return nil
	}

	return currStates, assignPartitionRecs, assignPartitionsFunc
}

func TestOrchestrateEarlyPauseResume(t *testing.T) {
	testOrchestratePauseResume(t, 1)
}

func TestOrchestrateMidPauseResume(t *testing.T) {
	testOrchestratePauseResume(t, 2)
}

func testOrchestratePauseResume(t *testing.T, numProgress int) {
	_, _, assignPartitionsFunc := testMkFuncs()

	pauseCh := make(chan struct{})

	slowAssignPartitionsFunc := func(stopCh chan struct{},
		node string, partitions, states, ops []string) error {
		<-pauseCh
		return assignPartitionsFunc(stopCh, node, partitions, states, ops)
	}

	o, err := OrchestrateMoves(
		mrPartitionModel,
		OrchestratorOptions{},
		[]string{"a", "b"},
		PartitionMap{
			"00": &Partition{
				Name: "00",
				NodesByState: map[string][]string{
					"primary": {"a"},
					"replica": {"b"},
				},
			},
			"01": &Partition{
				Name: "01",
				NodesByState: map[string][]string{
					"primary": {"a"},
					"replica": {"b"},
				},
			},
			"02": &Partition{
				Name: "02",
				NodesByState: map[string][]string{
					"primary": {"a"},
					"replica": {"b"},
				},
			},
		},
		PartitionMap{
			"00": &Partition{
				Name: "00",
				NodesByState: map[string][]string{
					"primary": {"b"},
					"replica": {"a"},
				},
			},
			"01": &Partition{
				Name: "01",
				NodesByState: map[string][]string{
					"primary": {"b"},
					"replica": {"a"},
				},
			},
			"02": &Partition{
				Name: "02",
				NodesByState: map[string][]string{
					"primary": {"b"},
					"replica": {"a"},
				},
			},
		},
		slowAssignPartitionsFunc,
		LowestWeightPartitionMoveForNode,
	)
	if err != nil || o == nil {
		t.Errorf("expected nil err")
	}

	for i := 0; i < numProgress; i++ {
		<-o.ProgressCh()
	}

	o.PauseNewAssignments()
	o.PauseNewAssignments()
	o.PauseNewAssignments()

	o.ResumeNewAssignments()
	o.ResumeNewAssignments()

	close(pauseCh)

	gotProgress := 0
	var lastProgress OrchestratorProgress

	for progress := range o.ProgressCh() {
		gotProgress++
		lastProgress = progress

		o.ResumeNewAssignments()
	}

	o.Stop()

	if gotProgress <= 0 {
		t.Errorf("expected progress")
	}

	if len(lastProgress.Errors) > 0 {
		t.Errorf("expected no errs")
	}

	if lastProgress.TotPauseNewAssignments != 1 ||
		lastProgress.TotResumeNewAssignments != 1 {
		t.Errorf("numProgress: %d, expected pause/resume of 1, got: %#v",
			numProgress, lastProgress)
	}
}

// Another attempt at pause/resume testing that tries to exercise
// pause/resume code paths in the moves supplier.
func TestOrchestratePauseResumeIntoMovesSupplier(t *testing.T) {
	testOrchestratePauseResumeIntoMovesSupplier(t, 2, 1)
}

func testOrchestratePauseResumeIntoMovesSupplier(t *testing.T,
	numProgressBeforePause, numFastAssignPartitionFuncs int) {
	_, _, assignPartitionsFunc := testMkFuncs()

	var m sync.Mutex
	numAssignPartitionFuncs := 0

	slowCh := make(chan struct{})

	slowAssignPartitionsFunc := func(stopCh chan struct{},
		node string, partitions, states, ops []string) error {
		m.Lock()
		numAssignPartitionFuncs++
		n := numAssignPartitionFuncs
		m.Unlock()

		if n > numFastAssignPartitionFuncs {
			<-slowCh
		}

		return assignPartitionsFunc(stopCh, node, partitions, states, ops)
	}

	o, err := OrchestrateMoves(
		mrPartitionModel,
		OrchestratorOptions{},
		[]string{"a", "b", "c"},
		PartitionMap{
			"00": &Partition{
				Name: "00",
				NodesByState: map[string][]string{
					"primary": {"a"},
					"replica": {"b"},
				},
			},
			"01": &Partition{
				Name: "01",
				NodesByState: map[string][]string{
					"primary": {"b"},
					"replica": {"c"},
				},
			},
		},
		PartitionMap{
			"00": &Partition{
				Name: "00",
				NodesByState: map[string][]string{
					"primary": {"b"},
					"replica": {"c"},
				},
			},
			"01": &Partition{
				Name: "01",
				NodesByState: map[string][]string{
					"primary": {"c"},
					"replica": {"a"},
				},
			},
		},
		slowAssignPartitionsFunc,
		LowestWeightPartitionMoveForNode,
	)
	if err != nil || o == nil {
		t.Errorf("expected nil err")
	}

	for i := 0; i < numProgressBeforePause; i++ {
		<-o.ProgressCh()
	}

	o.PauseNewAssignments()
	o.PauseNewAssignments()
	o.PauseNewAssignments()

	o.ResumeNewAssignments()
	o.ResumeNewAssignments()

	close(slowCh)

	gotProgress := 0
	var lastProgress OrchestratorProgress

	for progress := range o.ProgressCh() {
		gotProgress++
		lastProgress = progress

		o.ResumeNewAssignments()
	}

	o.Stop()

	if gotProgress <= 0 {
		t.Errorf("expected progress")
	}

	if len(lastProgress.Errors) > 0 {
		t.Errorf("expected no errs")
	}

	if lastProgress.TotPauseNewAssignments != 1 ||
		lastProgress.TotResumeNewAssignments != 1 {
		t.Errorf("numProgressBeforePause: %d,"+
			" expected pause/resume of 1, got: %#v",
			numProgressBeforePause, lastProgress)
	}
}

func TestOrchestrateEarlyStop(t *testing.T) {
	_, _, assignPartitionFunc := testMkFuncs()

	o, err := OrchestrateMoves(
		mrPartitionModel,
		OrchestratorOptions{},
		[]string{"a", "b"},
		PartitionMap{
			"00": &Partition{
				Name: "00",
				NodesByState: map[string][]string{
					"primary": {"a"},
				},
			},
		},
		PartitionMap{
			"00": &Partition{
				Name: "00",
				NodesByState: map[string][]string{
					"primary": {"b"},
				},
			},
		},
		assignPartitionFunc,
		LowestWeightPartitionMoveForNode,
	)
	if err != nil || o == nil {
		t.Errorf("expected nil err")
	}

	<-o.ProgressCh()

	o.Stop()
	o.Stop()
	o.Stop()

	gotProgress := 0
	var lastProgress OrchestratorProgress

	for progress := range o.ProgressCh() {
		gotProgress++
		lastProgress = progress
	}

	if gotProgress <= 0 {
		t.Errorf("expected some progress")
	}

	if len(lastProgress.Errors) > 0 {
		t.Errorf("expected no errs")
	}

	if lastProgress.TotStop != 1 {
		t.Errorf("expected stop of 1")
	}
}

func TestOrchestrateConcurrentMoves(t *testing.T) {
	options := OrchestratorOptions{}

	tests := []struct {
		skip                    bool
		label                   string
		partitionModel          PartitionModel
		maxConcurrentMoves      int
		numProgress             int
		nodesAll                []string
		begMap                  PartitionMap
		endMap                  PartitionMap
		assignPartitionsFunc    AssignPartitionsFunc
		skipCallbacks           int
		expConcurrentMovesCount int
		expNode                 string
		expMovePartitions       []string
		expMoveStates           []string
		expMoveOps              []string
		expectErr               error
	}{
		{
			label:              "2 node, 2 partition movement",
			partitionModel:     mrPartitionModel,
			maxConcurrentMoves: 2,
			numProgress:        1,
			nodesAll:           []string{"a", "b"},
			begMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"02": &Partition{
					Name: "02",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"03": &Partition{
					Name: "03",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"02": &Partition{
					Name: "02",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {},
					},
				},
				"03": &Partition{
					Name: "3",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {},
					},
				},
			},
			expNode:                 "b",
			expConcurrentMovesCount: 2,
			expMovePartitions:       []string{"02", "03"},
			expMoveStates:           []string{"primary", "primary"},
			expMoveOps:              []string{"add", "add"},
			expectErr:               nil,
		},
		{
			label:              "1 node, 4 partition movement",
			partitionModel:     mrPartitionModel,
			maxConcurrentMoves: 4,
			numProgress:        1,
			nodesAll:           []string{"a"},
			begMap: PartitionMap{
				"00": &Partition{
					Name:         "00",
					NodesByState: map[string][]string{},
				},
				"01": &Partition{
					Name:         "01",
					NodesByState: map[string][]string{},
				},
				"02": &Partition{
					Name:         "02",
					NodesByState: map[string][]string{},
				},
				"03": &Partition{
					Name:         "03",
					NodesByState: map[string][]string{},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"02": &Partition{
					Name: "02",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"03": &Partition{
					Name: "3",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
			},
			expConcurrentMovesCount: 4,
			expNode:                 "a",
			expMovePartitions:       []string{"00", "01", "02", "03"},
			expMoveStates:           []string{"primary", "primary", "primary", "primary"},
			expMoveOps:              []string{"add", "add", "add", "add"},
			expectErr:               nil,
		},
		{
			skip:                    true,
			label:                   "empty assignPartitions callback",
			partitionModel:          mrPartitionModel,
			maxConcurrentMoves:      2,
			expConcurrentMovesCount: 2,
			numProgress:             0,
			nodesAll:                []string{"a", "b"},
			begMap:                  PartitionMap{},
			endMap:                  PartitionMap{},
			expectErr: fmt.Errorf("callback implementation for " +
				"AssignPartitionsFunc is expected"),
		},
		{
			label:              "1 node delete, 2 partition promote",
			partitionModel:     mrPartitionModel,
			maxConcurrentMoves: 4,
			numProgress:        1,
			nodesAll:           []string{"a"},
			begMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
				"02": &Partition{
					Name: "02",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"a"},
					},
				},
				"03": &Partition{
					Name: "03",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"a"},
					},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"02": &Partition{
					Name: "02",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"03": &Partition{
					Name: "3",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
			},
			expConcurrentMovesCount: 2,
			expNode:                 "a",
			expMovePartitions:       []string{"02", "03"},
			expMoveStates:           []string{"primary", "primary"},
			expMoveOps:              []string{"promote", "promote"},
			expectErr:               nil,
		},
		{
			label:              "1 node delete, 2 partition del",
			partitionModel:     mrPartitionModel,
			maxConcurrentMoves: 2,
			numProgress:        2,
			nodesAll:           []string{"a", "b"},
			begMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
				"02": &Partition{
					Name: "02",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"a"},
					},
				},
				"03": &Partition{
					Name: "03",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"a"},
					},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"02": &Partition{
					Name: "02",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"03": &Partition{
					Name: "03",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
			},
			expConcurrentMovesCount: 2,
			expNode:                 "b",
			expMovePartitions:       []string{"00", "01"},
			expMoveStates:           []string{"", ""},
			expMoveOps:              []string{"del", "del"},
			expectErr:               nil,
		},
		{
			label:              "2 node deletions out of 3 node cluster",
			partitionModel:     mrPartitionModel,
			maxConcurrentMoves: 2,
			numProgress:        6,
			nodesAll:           []string{"a", "b", "c"},
			begMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"c"},
					},
				},
				"02": &Partition{
					Name: "02",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"a"},
					},
				},
				"03": &Partition{
					Name: "03",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"c"},
					},
				},
				"04": &Partition{
					Name: "04",
					NodesByState: map[string][]string{
						"primary": {"c"},
						"replica": {"a"},
					},
				},
				"05": &Partition{
					Name: "05",
					NodesByState: map[string][]string{
						"primary": {"c"},
						"replica": {"b"},
					},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"02": &Partition{
					Name: "02",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"03": &Partition{
					Name: "03",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"04": &Partition{
					Name: "04",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"05": &Partition{
					Name: "05",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
			},
			expConcurrentMovesCount: 2,
			skipCallbacks:           1,
			expNode:                 "a",
			expMovePartitions:       []string{"03", "05"},
			expMoveStates:           []string{"primary", "primary"},
			expMoveOps:              []string{"add", "add"},
			expectErr:               nil,
		},
		{
			label:              "2 node deletions out of 3 node cluster",
			partitionModel:     mrPartitionModel,
			maxConcurrentMoves: 4,
			numProgress:        6,
			nodesAll:           []string{"a", "b", "c"},
			begMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"c"},
					},
				},
				"02": &Partition{
					Name: "02",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"a"},
					},
				},
				"03": &Partition{
					Name: "03",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"c"},
					},
				},
				"04": &Partition{
					Name: "04",
					NodesByState: map[string][]string{
						"primary": {"c"},
						"replica": {"a"},
					},
				},
				"05": &Partition{
					Name: "05",
					NodesByState: map[string][]string{
						"primary": {"c"},
						"replica": {"b"},
					},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"02": &Partition{
					Name: "02",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"03": &Partition{
					Name: "03",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"04": &Partition{
					Name: "04",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
				"05": &Partition{
					Name: "05",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {},
					},
				},
			},
			expConcurrentMovesCount: 4,
			expNode:                 "a",
			expMovePartitions:       []string{"02", "03", "04", "05"},
			expMoveStates:           []string{"primary", "primary", "primary", "primary"},
			expMoveOps:              []string{"promote", "promote", "add", "add"},
			expectErr:               nil,
		},
	}

	for testi, test := range tests {
		_, _, assignPartitionsFunc := testMkFuncs()

		if !test.skip {
			test.assignPartitionsFunc = func(stopCh chan struct{},
				node string, partitions []string, states []string, ops []string) error {
				if test.expNode != node {
					return nil
				}
				if test.skipCallbacks > 0 {
					test.skipCallbacks--
					return nil
				}

				if len(partitions) != test.expConcurrentMovesCount {
					t.Errorf("testi: %d, label: %s, concurrent partition moves expected: %d, but got only: %d",
						testi, test.label, test.expConcurrentMovesCount, len(partitions))
				}

				sort.Strings(partitions)
				if !reflect.DeepEqual(test.expMovePartitions, partitions) {
					t.Errorf("testi: %d, label: %s, moving partitions expected: %+v, but got: %+v",
						testi, test.label, test.expMovePartitions, partitions)
				}

				sort.Strings(states)
				if !reflect.DeepEqual(test.expMoveStates, states) {
					t.Errorf("testi: %d, label: %s, moving states expected: %+v, but got: %+v",
						testi, test.label, test.expMoveStates, states)
				}

				if !reflect.DeepEqual(test.expMoveOps, ops) {
					t.Errorf("testi: %d, label: %s, moving ops expected: %+v, but got: %+v",
						testi, test.label, test.expMoveStates, ops)
				}

				assignPartitionsFunc(stopCh, node, partitions, states, ops)
				return nil
			}
		}

		options.MaxConcurrentPartitionMovesPerNode = test.maxConcurrentMoves

		o, err := OrchestrateMoves(
			test.partitionModel,
			options,
			test.nodesAll,
			test.begMap,
			test.endMap,
			test.assignPartitionsFunc,
			LowestWeightPartitionMoveForNode,
		)
		if test.expectErr == nil && o == nil {
			t.Errorf("testi: %d, label: %s,"+
				" expected o",
				testi, test.label)
		}
		if err != nil && test.expectErr != nil && err.Error() != test.expectErr.Error() {
			t.Errorf("testi: %d, label: %s,"+
				" expectErr: %v, got: %v",
				testi, test.label,
				test.expectErr, err)
		}

		if !test.skip {
			for {
				prog := <-o.ProgressCh()

				if prog.TotMoverAssignPartitionOk >= test.numProgress {
					break
				}
			}

			o.Stop()
		}
	}
}

func TestOrchestrateMoves(t *testing.T) {
	tests := []struct {
		skip           bool
		label          string
		partitionModel PartitionModel
		options        OrchestratorOptions
		nodesAll       []string
		begMap         PartitionMap
		endMap         PartitionMap
		expectErr      error

		// Keyed by partition.
		expectAssignPartitions map[string][]assignPartitionRec
	}{
		{
			label:          "do nothing",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string(nil),
			begMap:         PartitionMap{},
			endMap:         PartitionMap{},
			expectErr:      nil,
		},
		{
			label:          "1 node, no assignments or changes",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a"},
			begMap:         PartitionMap{},
			endMap:         PartitionMap{},
			expectErr:      nil,
		},
		{
			label:          "no nodes, but some partitions",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string(nil),
			begMap: PartitionMap{
				"00": &Partition{
					Name:         "00",
					NodesByState: map[string][]string{},
				},
				"01": &Partition{
					Name:         "01",
					NodesByState: map[string][]string{},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name:         "00",
					NodesByState: map[string][]string{},
				},
				"01": &Partition{
					Name:         "01",
					NodesByState: map[string][]string{},
				},
			},
			expectErr: nil,
		},
		{
			label:          "add node a, 1 partition",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a"},
			begMap: PartitionMap{
				"00": &Partition{
					Name:         "00",
					NodesByState: map[string][]string{},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
					},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": {
					{
						partition: "00", node: "a", state: "primary",
					},
				},
			},
			expectErr: nil,
		},
		{
			label:          "add node a & b, 1 partition",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a", "b"},
			begMap: PartitionMap{
				"00": &Partition{
					Name:         "00",
					NodesByState: map[string][]string{},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": {
					{
						partition: "00", node: "a", state: "primary",
					},
					{
						partition: "00", node: "b", state: "replica",
					},
				},
			},
			expectErr: nil,
		},
		{
			label:          "add node a & b & c, 1 partition",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a", "b", "c"},
			begMap: PartitionMap{
				"00": &Partition{
					Name:         "00",
					NodesByState: map[string][]string{},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": {
					{
						partition: "00", node: "a", state: "primary",
					},
					{
						partition: "00", node: "b", state: "replica",
					},
				},
			},
			expectErr: nil,
		},
		{
			label:          "del node a, 1 partition",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a"},
			begMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
					},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name:         "00",
					NodesByState: map[string][]string{},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": {
					{
						partition: "00", node: "a", state: "",
					},
				},
			},
			expectErr: nil,
		},
		{
			label:          "swap a to b, 1 partition",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a", "b"},
			begMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
					},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"b"},
					},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": {
					{
						partition: "00", node: "b", state: "primary",
					},
					{
						partition: "00", node: "a", state: "",
					},
				},
			},
			expectErr: nil,
		},
		{
			label:          "swap a to b, 1 partition, c unchanged",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a", "b", "c"},
			begMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"c"},
					},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"c"},
					},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": {
					{
						partition: "00", node: "b", state: "primary",
					},
					{
						partition: "00", node: "a", state: "",
					},
				},
			},
			expectErr: nil,
		},
		{
			label:          "1 partition from a|b to c|a",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a", "b", "c"},
			begMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"c"},
						"replica": {"a"},
					},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": {
					{
						partition: "00", node: "c", state: "primary",
					},
					{
						partition: "00", node: "a", state: "replica",
					},
					{
						partition: "00", node: "b", state: "",
					},
				},
			},
			expectErr: nil,
		},
		{
			label:          "add node a & b, 2 partitions",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a", "b"},
			begMap: PartitionMap{
				"00": &Partition{
					Name:         "00",
					NodesByState: map[string][]string{},
				},
				"01": &Partition{
					Name:         "01",
					NodesByState: map[string][]string{},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"a"},
					},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": {
					{
						partition: "00", node: "a", state: "primary",
					},
					{
						partition: "00", node: "b", state: "replica",
					},
				},
				"01": {
					{
						partition: "01", node: "b", state: "primary",
					},
					{
						partition: "01", node: "a", state: "replica",
					},
				},
			},
			expectErr: nil,
		},
		{
			label:          "swap ab to cd, 2 partitions",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a", "b", "c", "d"},
			begMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"a"},
					},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"c"},
						"replica": {"d"},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"primary": {"d"},
						"replica": {"c"},
					},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": {
					{
						partition: "00", node: "c", state: "primary",
					},
					{
						partition: "00", node: "a", state: "",
					},
					{
						partition: "00", node: "d", state: "replica",
					},
					{
						partition: "00", node: "b", state: "",
					},
				},
				"01": {
					{
						partition: "01", node: "d", state: "primary",
					},
					{
						partition: "01", node: "b", state: "",
					},
					{
						partition: "01", node: "c", state: "replica",
					},
					{
						partition: "01", node: "a", state: "",
					},
				},
			},
			expectErr: nil,
		},
		{
			// TODO: This test is intended to get coverage on
			// LowestWeightPartitionMoveForNode() on its inner
			// MoveOpWeight if statement, but seems to be
			// intermittent -- perhaps goroutine race?
			label:          "concurrent moves on b, 2 partitions",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a", "b", "c"},
			begMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"a"},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"a"},
					},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"primary": {"c"},
						"replica": {"a"},
					},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": {
					{
						partition: "00", node: "a", state: "primary",
					},
					{
						partition: "00", node: "b", state: "replica",
					},
				},
				"01": {
					{
						partition: "01", node: "c", state: "primary",
					},
					{
						partition: "01", node: "b", state: "",
					},
				},
			},
			expectErr: nil,
		},
		{
			label:          "nodes with not much work",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a", "b", "c", "d", "e"},
			begMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"a", "d", "e"},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"a", "d", "e"},
					},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b", "d", "e"},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"primary": {"c"},
						"replica": {"a", "d", "e"},
					},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": {
					{
						partition: "00", node: "a", state: "primary",
					},
					{
						partition: "00", node: "b", state: "replica",
					},
				},
				"01": {
					{
						partition: "01", node: "c", state: "primary",
					},
					{
						partition: "01", node: "b", state: "",
					},
				},
			},
			expectErr: nil,
		},
		{
			label:          "more concurrent moves",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a", "b", "c", "d", "e", "f", "g"},
			begMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"a"},
						"replica": {"b"},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"c"},
					},
				},
				"02": &Partition{
					Name: "02",
					NodesByState: map[string][]string{
						"primary": {"c"},
						"replica": {"d"},
					},
				},
				"03": &Partition{
					Name: "03",
					NodesByState: map[string][]string{
						"primary": {"d"},
						"replica": {"e"},
					},
				},
				"04": &Partition{
					Name: "04",
					NodesByState: map[string][]string{
						"primary": {"e"},
						"replica": {"f"},
					},
				},
				"05": &Partition{
					Name: "05",
					NodesByState: map[string][]string{
						"primary": {"f"},
						"replica": {"g"},
					},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"primary": {"b"},
						"replica": {"c"},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"primary": {"c"},
						"replica": {"d"},
					},
				},
				"02": &Partition{
					Name: "02",
					NodesByState: map[string][]string{
						"primary": {"d"},
						"replica": {"e"},
					},
				},
				"03": &Partition{
					Name: "03",
					NodesByState: map[string][]string{
						"primary": {"e"},
						"replica": {"f"},
					},
				},
				"04": &Partition{
					Name: "04",
					NodesByState: map[string][]string{
						"primary": {"f"},
						"replica": {"g"},
					},
				},
				"05": &Partition{
					Name: "05",
					NodesByState: map[string][]string{
						"primary": {"g"},
						"replica": {"a"},
					},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": {
					{
						partition: "00", node: "b", state: "primary",
					},
					{
						partition: "00", node: "a", state: "",
					},
					{
						partition: "00", node: "c", state: "replica",
					},
				},
				"01": {
					{
						partition: "01", node: "c", state: "primary",
					},
					{
						partition: "01", node: "b", state: "",
					},
					{
						partition: "01", node: "d", state: "replica",
					},
				},
				"02": {
					{
						partition: "02", node: "d", state: "primary",
					},
					{
						partition: "02", node: "c", state: "",
					},
					{
						partition: "02", node: "e", state: "replica",
					},
				},
				"03": {
					{
						partition: "03", node: "e", state: "primary",
					},
					{
						partition: "03", node: "d", state: "",
					},
					{
						partition: "03", node: "f", state: "replica",
					},
				},
				"04": {
					{
						partition: "04", node: "f", state: "primary",
					},
					{
						partition: "04", node: "e", state: "",
					},
					{
						partition: "04", node: "g", state: "replica",
					},
				},
				"05": {
					{
						partition: "05", node: "g", state: "primary",
					},
					{
						partition: "05", node: "f", state: "",
					},
					{
						partition: "05", node: "a", state: "replica",
					},
				},
			},
			expectErr: nil,
		},
	}

	for testi, test := range tests {
		if test.skip {
			continue
		}

		_, assignPartitionRecs, assignPartitionFunc := testMkFuncs()

		o, err := OrchestrateMoves(
			test.partitionModel,
			test.options,
			test.nodesAll,
			test.begMap,
			test.endMap,
			assignPartitionFunc,
			LowestWeightPartitionMoveForNode,
		)
		if o == nil {
			t.Errorf("testi: %d, label: %s,"+
				" expected o",
				testi, test.label)
		}
		if err != test.expectErr {
			t.Errorf("testi: %d, label: %s,"+
				" expectErr: %v, got: %v",
				testi, test.label,
				test.expectErr, err)
		}

		debug := false

		if debug {
			o.m.Lock()
			fmt.Printf("test: %q\n  START progress: %#v\n",
				test.label, o.progress)
			o.m.Unlock()
		}

		for progress := range o.ProgressCh() {
			if debug {
				fmt.Printf("test: %q\n  progress: %#v\n",
					test.label, progress)
			}
		}

		o.Stop()

		if len(assignPartitionRecs) != len(test.expectAssignPartitions) {
			t.Errorf("testi: %d, label: %s,"+
				" len(assignPartitionRecs == %d)"+
				" != len(test.expectAssignPartitions == %d),"+
				" assignPartitionRecs: %#v,"+
				" test.expectAssignPartitions: %#v",
				testi, test.label,
				len(assignPartitionRecs),
				len(test.expectAssignPartitions),
				assignPartitionRecs,
				test.expectAssignPartitions)
		}

		for partition, eapm := range test.expectAssignPartitions {
			for eapi, eap := range eapm {
				apr := assignPartitionRecs[partition][eapi]
				if eap.partition != apr.partition ||
					eap.node != apr.node ||
					eap.state != apr.state {
					t.Errorf("testi: %d, label: %s,"+
						" mismatched assignment,"+
						" eapi: %d, eap: %#v, apr: %#v",
						testi, test.label,
						eapi, eap, apr)
				}
			}
		}
	}
}
