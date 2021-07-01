//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package blance

// StringsToMap converts an array of strings to an map keyed by
// strings, so the caller can have faster lookups.
func StringsToMap(strsArr []string) map[string]bool {
	if strsArr == nil {
		return nil
	}
	strs := map[string]bool{}
	for _, str := range strsArr {
		strs[str] = true
	}
	return strs
}

// StringsRemoveStrings returns a copy of stringArr, but with any
// strings from removeArr removed, keeping the same order as
// stringArr.  So, stringArr subtract removeArr.
func StringsRemoveStrings(stringArr, removeArr []string) []string {
	removeMap := StringsToMap(removeArr)
	rv := make([]string, 0, len(stringArr))
	for _, s := range stringArr {
		if !removeMap[s] {
			rv = append(rv, s)
		}
	}
	return rv
}

// StringsIntersectStrings returns a brand new array that has the
// intersection of a and b.
func StringsIntersectStrings(a, b []string) []string {
	bMap := StringsToMap(b)
	rMap := map[string]bool{}
	rv := make([]string, 0, len(a))
	for _, s := range a {
		if bMap[s] && !rMap[s] {
			rMap[s] = true
			rv = append(rv, s)
		}
	}
	return rv
}

// stringsDeduplicate returns a brand new array that has the
// all the unique of a preserving the order.
func stringsDeduplicate(a []string) []string {
	bMap := make(map[string]struct{})
	rv := make([]string, 0, len(bMap))
	for _, s := range a {
		if _, ok := bMap[s]; ok {
			continue
		}
		bMap[s] = struct{}{}
		rv = append(rv, s)
	}
	return rv
}
