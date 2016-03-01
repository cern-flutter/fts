/*
 * Copyright (c) CERN 2016
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tasks

import (
	"testing"
)

// Test merging two sets that can not be merged
func TestBadMerge(t *testing.T) {
	ts1 := &Batch{
		Type:         BatchSimple,
		DelegationID: "1234",
	}
	ts2 := &Batch{
		Type:         BatchBulk,
		DelegationID: "1234",
	}
	if _, err := ts1.Merge(ts2); err == nil {
		t.Error("Should have not been able to merge")
	}

	ts1.Type = BatchBulk
	ts1.DelegationID = "abcde"
	if _, err := ts1.Merge(ts2); err == nil {
		t.Error("Should have not been able to merge")
	}
}

// Test merging two sets that can be merged
func TestMerge(t *testing.T) {
	ts1 := &Batch{
		Type:         BatchBulk,
		DelegationID: "1234",
		Transfers:    []*Transfer{&Transfer{JobID: "abcde"}},
	}
	ts2 := &Batch{
		Type:         BatchBulk,
		DelegationID: "1234",
		Transfers:    []*Transfer{&Transfer{JobID: "1234"}},
	}

	var err error
	var res *Batch
	if res, err = ts1.Merge(ts2); err != nil {
		t.Fatal(err)
	}

	if res.Type != BatchBulk {
		t.Error("Unexpected transfer set type")
	}
	if res.DelegationID != ts1.DelegationID {
		t.Error("Expecting delegation ID to be ", ts1.DelegationID)
	}
	if len(res.Transfers) != 2 {
		t.Fatal("Unexpected merge length")
	}
	if res.Transfers[0] != ts1.Transfers[0] {
		t.Error("Unexpected transfer on the merge result")
	}
	if res.Transfers[1] != ts2.Transfers[0] {
		t.Error("Unexpected transfer on the merge result")
	}
}

// Test split a set that can not be split
func TestBadSplit(t *testing.T) {
	ts := &Batch{
		Type:         BatchBulk,
		DelegationID: "1234",
		Transfers: []*Transfer{
			&Transfer{JobID: "abcde"},
			&Transfer{JobID: "12345"},
		},
	}

	split := ts.Split()
	if len(split) != 1 {
		t.Fatal("Expecting a noop split")
	}
	if split[0] != ts {
		t.Error("Unexpected transfer set")
	}
}

// Test split a set that can be split
func TestSplit(t *testing.T) {
	ts := &Batch{
		Type:         BatchSimple,
		DelegationID: "1234",
		Transfers: []*Transfer{
			&Transfer{JobID: "abcde"},
			&Transfer{JobID: "12345"},
		},
	}

	split := ts.Split()
	if len(split) != 2 {
		t.Fatal("Expecting two transfer sets")
	}
	if split[0].DelegationID != ts.DelegationID || split[1].DelegationID != ts.DelegationID {
		t.Error("Expecting delegation ID to be ", ts.DelegationID)
	}
	if split[0].Type != BatchSimple && split[1].Type != BatchSimple {
		t.Error("Unexpected transfer set types")
	}
	if len(split[0].Transfers) != 1 || split[0].Transfers[0] != ts.Transfers[0] {
		t.Error("Unexpected transfer")
	}
	if len(split[1].Transfers) != 1 || split[1].Transfers[0] != ts.Transfers[1] {
		t.Error("Unexpected transfer")
	}
}

// Test the GetId method
func TestGetId(t *testing.T) {
	ts := &Batch{
		Type:         BatchSimple,
		DelegationID: "1234",
		Transfers: []*Transfer{
			&Transfer{JobID: "abcde"},
			&Transfer{JobID: "12345"},
		},
	}
	if id := ts.GetID(); id == "" {
		t.Fatal("Empty Id for the batch")
	} else {
		t.Log(id)
	}
}
