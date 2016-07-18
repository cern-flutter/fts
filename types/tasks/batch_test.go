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
	"gitlab.cern.ch/flutter/fts/types/surl"
	"reflect"
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
		Transfers:    []*Transfer{{JobID: "abcde"}},
	}
	ts2 := &Batch{
		Type:         BatchBulk,
		DelegationID: "1234",
		Transfers:    []*Transfer{{JobID: "1234"}},
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

// Test split a set that can be split
func TestSplit(t *testing.T) {
	ts := &Batch{
		Type:         BatchSimple,
		DelegationID: "1234",
		Transfers: []*Transfer{
			{JobID: "abcde"},
			{JobID: "12345"},
		},
	}

	split := ts.splitSimple()
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
			{JobID: "abcde"},
			{JobID: "12345"},
		},
	}
	if id := ts.GetID(); id == "" {
		t.Fatal("Empty Id for the batch")
	} else {
		t.Log(id)
	}
}

// Test normalization with a simple batch
func TestNormSimple(t *testing.T) {
	ts := &Batch{
		Type:         BatchSimple,
		DelegationID: "1234",
		Transfers: []*Transfer{
			{JobID: "abcde"},
			{JobID: "12345"},
		},
	}

	normalized := ts.Normalize()
	if len(normalized) != 2 {
		t.Fatal("Expecting 2 batches, got", len(normalized))
	}

	for _, batch := range normalized {
		if len(batch.Transfers) != 1 {
			t.Error("Expecting 1 transfer per batch, got", len(batch.Transfers))
		}
	}
}

// Test normalization of a bulk where both transfers can be scheduled together
func TestNormBulkConsistent(t *testing.T) {
	src1, _ := surl.Parse("mock://a/patch")
	dst1, _ := surl.Parse("mock://b/path")
	src2, _ := surl.Parse("mock://a/patch2")
	dst2, _ := surl.Parse("mock://b/path2")

	ts := &Batch{
		Type:         BatchBulk,
		DelegationID: "1234",
		Transfers: []*Transfer{
			{
				JobID: "abcde", TransferID: "1234",
				Source: *src1, Destination: *dst1,
			},
			{
				JobID: "abcde", TransferID: "1234",
				Source: *src2, Destination: *dst2,
			},
		},
	}

	normalized := ts.Normalize()
	if len(normalized) != 1 {
		t.Fatal("Expecting the bulk to remain together")
	}

	if !reflect.DeepEqual(ts.Transfers[0], normalized[0].Transfers[0]) {
		t.Fatal("Bulks are different")
	}

	if normalized[0].SourceSe != "mock://a" {
		t.Fatal("Unexpected source se")
	}
	if normalized[0].DestSe != "mock://b" {
		t.Fatal("Unexpected destination se")
	}
}

// Test normalization of a bulk where both transfers can not be scheduled together
func TestNormBulkSplit(t *testing.T) {
	src1, _ := surl.Parse("mock://a/patch")
	dst1, _ := surl.Parse("mock://b/path")
	src2, _ := surl.Parse("mock://c/patch2")
	dst2, _ := surl.Parse("mock://d/path2")

	ts := &Batch{
		Type:         BatchBulk,
		DelegationID: "1234",
		Transfers: []*Transfer{
			{
				JobID: "abcde", TransferID: "1234",
				Source: *src1, Destination: *dst1,
			},
			{
				JobID: "abcde", TransferID: "1234",
				Source: *src2, Destination: *dst2,
			},
		},
	}

	normalized := ts.Normalize()
	if len(normalized) != 2 {
		t.Fatal("Expecting the bulk to be split together")
	}

	if !reflect.DeepEqual(ts.Transfers[0], normalized[0].Transfers[0]) {
		t.Fatal("Bulks are different")
	}
	if !reflect.DeepEqual(ts.Transfers[1], normalized[1].Transfers[0]) {
		t.Fatal("Bulks are different")
	}

	if normalized[0].SourceSe != "mock://a" {
		t.Fatal("Unexpected source se")
	}
	if normalized[0].DestSe != "mock://b" {
		t.Fatal("Unexpected destination se")
	}
	if normalized[1].SourceSe != "mock://c" {
		t.Fatal("Unexpected source se")
	}
	if normalized[1].DestSe != "mock://d" {
		t.Fatal("Unexpected destination se")
	}
}

// Call normalization with a single batch
func TestNormalizeOne(t *testing.T) {
	src1, _ := surl.Parse("mock://a/patch")
	dst1, _ := surl.Parse("mock://b/path")

	ts := &Batch{
		Type:         BatchSimple,
		DelegationID: "1234",
		Transfers: []*Transfer{
			{
				JobID: "abcde", TransferID: "1234",
				Source: *src1, Destination: *dst1,
			},
		},
	}

	normalized := ts.Normalize()
	if len(normalized) != 1 {
		t.Fatal("Expecting one single batch")
	}

	if !reflect.DeepEqual(ts.Transfers[0], normalized[0].Transfers[0]) {
		t.Fatal("Bulks are different")
	}

	if normalized[0].SourceSe != "mock://a" {
		t.Fatal("Unexpected source se")
	}
	if normalized[0].DestSe != "mock://b" {
		t.Fatal("Unexpected destination se")
	}
}
