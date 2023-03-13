/*
Copyright 2014-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package blance

import (
	"reflect"
	"testing"
)

func TestStringsToMap(t *testing.T) {
	s := []string{}
	m := StringsToMap(s)
	if m == nil || len(m) != 0 {
		t.Errorf("expected StringsToMap to work on empty array")
	}
	m = StringsToMap([]string{"a"})
	if m == nil || !reflect.DeepEqual(m, map[string]bool{"a": true}) {
		t.Errorf("expected single string arr to work")
	}
	m = StringsToMap([]string{"a", "b", "a"})
	if m == nil || !reflect.DeepEqual(m, map[string]bool{"a": true, "b": true}) {
		t.Errorf("expected 3 string arr to work with dupe removal")
	}
}

func TestStringsRemoveStrings(t *testing.T) {
	tests := []struct {
		a   []string
		b   []string
		exp []string
	}{
		{[]string{}, []string{}, []string{}},
		{[]string{"a"}, []string{}, []string{"a"}},
		{[]string{"a"}, []string{"a"}, []string{}},
		{[]string{"a"}, []string{"b"}, []string{"a"}},
		{[]string{}, []string{"b"}, []string{}},
		{[]string{"a", "b", "c"}, []string{"a"}, []string{"b", "c"}},
		{[]string{"a", "b", "c"}, []string{"b"}, []string{"a", "c"}},
		{[]string{"a", "b", "c"}, []string{"c"}, []string{"a", "b"}},
		{[]string{"a", "b", "c"}, []string{"a", "b"}, []string{"c"}},
		{[]string{"a", "b", "c"}, []string{"a", "b", "c"}, []string{}},
		{[]string{"a", "b", "c"}, []string{"b", "c"}, []string{"a"}},
		{[]string{"a", "b", "c"}, []string{"c", "c"}, []string{"a", "b"}},
	}
	for i, c := range tests {
		r := StringsRemoveStrings(c.a, c.b)
		if !reflect.DeepEqual(r, c.exp) {
			t.Errorf("i: %d, a: %#v, b: %#v, exp: %#v, got: %#v",
				i, c.a, c.b, c.exp, r)
		}
	}
}

func TestStringsIntersectStrings(t *testing.T) {
	tests := []struct {
		a   []string
		b   []string
		exp []string
	}{
		{[]string{}, []string{}, []string{}},
		{[]string{"a"}, []string{}, []string{}},
		{[]string{"a"}, []string{"a"}, []string{"a"}},
		{[]string{"a"}, []string{"b"}, []string{}},
		{[]string{}, []string{"b"}, []string{}},
		{[]string{"a", "b", "c"}, []string{"a"}, []string{"a"}},
		{[]string{"a", "b", "c"}, []string{"b"}, []string{"b"}},
		{[]string{"a", "b", "c"}, []string{"c"}, []string{"c"}},
		{[]string{"a", "b", "c"}, []string{"a", "b"}, []string{"a", "b"}},
		{[]string{"a", "b", "c"}, []string{"a", "b", "c"}, []string{"a", "b", "c"}},
		{[]string{"a", "b", "c"}, []string{"b", "c"}, []string{"b", "c"}},
		{[]string{"a", "b", "c"}, []string{"c", "c"}, []string{"c"}},
		{[]string{"a", "b", "a", "b"}, []string{"a", "b"}, []string{"a", "b"}},
	}
	for i, c := range tests {
		r := StringsIntersectStrings(c.a, c.b)
		if !reflect.DeepEqual(r, c.exp) {
			t.Errorf("i: %d, a: %#v, b: %#v, exp: %#v, got: %#v",
				i, c.a, c.b, c.exp, r)
		}
	}
}

func TestStringsDeduplicate(t *testing.T) {
	tests := []struct {
		a   []string
		exp []string
	}{
		{[]string{}, []string{}},
		{[]string{"a"}, []string{"a"}},
		{[]string{"a", "b", "c"}, []string{"a", "b", "c"}},
		{[]string{"a", "b", "a", "b"}, []string{"a", "b"}},
	}
	for i, c := range tests {
		r := stringsDeduplicate(c.a)
		if !reflect.DeepEqual(r, c.exp) {
			t.Errorf("i: %d, a: %#v, exp: %#v, got: %#v",
				i, c.a, c.exp, r)
		}
	}
}
