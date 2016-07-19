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
	"crypto/md5"
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
)

type (
	// BatchType defines how the batch must be run
	BatchType string

	// BatchState models the transitions for a batch
	BatchState string

	// Batch contains a set of transfer that form a logical unit of work
	Batch struct {
		Type      BatchType   `json:"type"`
		State     BatchState  `json:"state"`
		Transfers []*Transfer `json:"transfers"`

		DelegationID string `json:"delegation_id"`

		// Batches are scheduled using these fields as key
		SourceSe, DestSe string
		Vo               string `json:"vo"`
		Activity         string
	}
)

const (
	BatchSubmitted = BatchState("Submitted")
	BatchReady     = BatchState("Ready")
	BatchRunning   = BatchState("Running")
	BatchDone      = BatchState("Done")

	// BatchSimple each transfer is independent
	BatchSimple = BatchType("Simple")
	// BatchBulk each transfer are independent, but the network connection and ssl session is to be reused
	BatchBulk = BatchType("Bulk")
	// BatchMultihop transfers are to be executed in order. If one fails, the following will not run
	BatchMultihop = BatchType("Multihop")
	// BatchMultisource transfers are alternatives. If one succeeds, the following will not run
	BatchMultisource = BatchType("Multisource")
)

var (
	// ErrEmptyTransferSet is returned when the batch is empty (has no transfers)
	ErrEmptyTransferSet = errors.New("Empty batch")
	// ErrCannotMerge is returned when the two batches can not be merged
	ErrCannotMerge = errors.New("Batches can not be merged")
	// ErrMissingInformation is returned when there are missing fields required for the batch to be routed
	ErrMissingInformation = errors.New("Missing fields")
)

// Validate checks if a batch is properly defined
func (ts *Batch) Validate() error {
	if len(ts.Transfers) == 0 {
		return ErrEmptyTransferSet
	}
	if ts.SourceSe == "" || ts.DestSe == "" || ts.Vo == "" || ts.Activity == "" {
		return ErrMissingInformation
	}
	for _, t := range ts.Transfers {
		if err := t.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// GetID gets a unique id associated to the batch
func (ts *Batch) GetID() string {
	hash := md5.New()
	for _, transfer := range ts.Transfers {
		hash.Write([]byte(transfer.TransferID))
	}
	var sum []byte
	sum = hash.Sum(sum)
	return fmt.Sprintf("%x", sum)
}

// Merge returns a new TransferSet merging b into ts
// Note that a batch is only mergeable if both are of type Bulk
func (ts *Batch) Merge(b *Batch) (*Batch, error) {
	if ts.Type != BatchBulk || b.Type != BatchBulk {
		return nil, ErrCannotMerge
	}
	if ts.DelegationID != b.DelegationID {
		return nil, ErrCannotMerge
	}
	if ts.SourceSe != b.SourceSe || ts.DestSe != b.DestSe || ts.Vo != b.Vo || ts.Activity != b.Activity {
		return nil, ErrCannotMerge
	}

	newTs := &Batch{
		Type:         BatchBulk,
		DelegationID: ts.DelegationID,
		Vo:           ts.Vo,
		SourceSe:     ts.SourceSe,
		DestSe:       ts.DestSe,
		Activity:     ts.Activity,
	}
	newTs.Transfers = append(newTs.Transfers, ts.Transfers...)
	newTs.Transfers = append(newTs.Transfers, b.Transfers...)
	return newTs, nil
}

// splitSimple splits one batch into multiple ones, if possible
func (ts *Batch) splitSimple() []*Batch {
	set := make([]*Batch, 0, len(ts.Transfers))
	for _, transfer := range ts.Transfers {
		if transfer.Activity == "" {
			transfer.Activity = "default"
		}
		transfer.State = TransferSubmitted
		set = append(set, &Batch{
			Type:         BatchSimple,
			DelegationID: ts.DelegationID,
			Transfers:    []*Transfer{transfer},
			Vo:           ts.Vo,
			SourceSe:     transfer.Source.GetStorageName(),
			DestSe:       transfer.Destination.GetStorageName(),
			Activity:     transfer.Activity,
		})
	}
	return set
}

// splitBulk splits one bulk batch into as many as necessary, so each one apply only to a
// source, destination, vo and activity
func (ts *Batch) splitBulk() []*Batch {
	type key struct {
		source, dest, activity string
	}
	batches := make(map[key]*Batch)
	for _, transfer := range ts.Transfers {
		if transfer.Activity == "" {
			transfer.Activity = "default"
		}
		transfer.State = TransferSubmitted

		sourceSe := transfer.Source.GetStorageName()
		destSe := transfer.Destination.GetStorageName()
		k := key{sourceSe, destSe, transfer.Activity}
		batch := batches[k]

		if batch == nil {
			batch = &Batch{
				Type:         BatchBulk,
				State:        ts.State,
				Transfers:    make([]*Transfer, 0),
				DelegationID: ts.DelegationID,
				SourceSe:     transfer.Source.GetStorageName(),
				DestSe:       transfer.Destination.GetStorageName(),
				Vo:           ts.Vo,
				Activity:     transfer.Activity,
			}
		}
		batch.Transfers = append(batch.Transfers, transfer)
		batches[k] = batch
	}

	slice := make([]*Batch, 0, len(batches))
	for _, batch := range batches {
		slice = append(slice, batch)
	}

	return slice
}

// Normalize splits a batch into as many as necessary to keep consistent routing
// This is, all transfers within a batch must be scheduled together, so they must
// apply to the same source, destination, vo and activity
func (ts *Batch) Normalize() []*Batch {
	switch ts.Type {
	case BatchSimple:
		return ts.splitSimple()
	case BatchBulk:
		return ts.splitBulk()
	// For both multihop and multisources, we care about the first item on the batch
	case BatchMultihop:
		fallthrough
	case BatchMultisource:
		ts.SourceSe = ts.Transfers[0].Source.GetStorageName()
		ts.DestSe = ts.Transfers[0].Destination.GetStorageName()
		if ts.Transfers[0].Activity != "" {
			ts.Activity = ts.Transfers[0].Activity
		} else {
			ts.Activity = "default"
		}
		for i := range ts.Transfers {
			if i == 0 {
				ts.Transfers[i].State = TransferSubmitted
			} else {
				ts.Transfers[i].State = TransferOnHold
			}
		}
	default:
		log.Panic("Unexpected batch type: ", ts.Type)
	}

	return []*Batch{ts}
}
