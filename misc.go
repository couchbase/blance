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
