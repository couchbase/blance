blance
======

blance implements a straightforward partition assignment algorithm,
using a greedy, heuristic, functional approach.

blance provides features like multiple, user-configurable partition
states (master, replica, read-only, etc), multi-level containment
hierarchy (shelf/rack/row/zone/datacenter awareness) with configurable
inclusion/exclusion policies, heterogeneous partition weights,
heterogeneous node weights, partition stickiness control, and multi-master
support.

[![Build Status](https://travis-ci.org/couchbase/blance.svg)](https://travis-ci.org/couchbase/blance) [![GoDoc](https://godoc.org/github.com/couchbase/blance?status.svg)](https://godoc.org/github.com/couchbase/blance) [![Coverage Status](https://coveralls.io/repos/couchbase/blance/badge.svg?branch=master&service=github)](https://coveralls.io/github/couchbase/blance?branch=master)

LICENSE: Apache 2.0

### Usage

See the PlanNextMap() function as a starting point.

### For developers

To get local coverage reports with heatmaps...

    go test -coverprofile=coverage.out -covermode=count && go tool cover -html=coverage.out
