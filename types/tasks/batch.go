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
)

type (
	// BatchType defines how the batch must be run
	BatchType string

	// Batch contains a set of transfer that form a logical unit of work
	Batch struct {
		Type         BatchType   `json:"type" bson:"type"`
		Transfers    []*Transfer `json:"transfers" bson:"transfers"`
		DelegationID string      `json:"delegation_id" bson:"delegation_id"`
		Vo           string      `json:"vo" bson:"vo"`
	}
)

const (
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
	ErrCannotMerge      = errors.New("Batches can not be merged")
)

// Validate checks if a batch is properly defined
func (ts *Batch) Validate() error {
	if len(ts.Transfers) == 0 {
		return ErrEmptyTransferSet
	}
	for _, t := range ts.Transfers {
		if err := t.Validate(); err != nil {
			return err
		}
	}
	return nil
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

	newTs := &Batch{
		Type:         BatchBulk,
		DelegationID: ts.DelegationID,
		Vo:           ts.Vo,
	}
	newTs.Transfers = append(newTs.Transfers, ts.Transfers...)
	newTs.Transfers = append(newTs.Transfers, b.Transfers...)
	return newTs, nil
}

// Split splits one batch into multiple ones, if possible
func (ts *Batch) Split() []*Batch {
	if ts.Type != BatchSimple {
		return []*Batch{ts}
	}
	set := make([]*Batch, 0, len(ts.Transfers))
	for _, transfer := range ts.Transfers {
		set = append(set, &Batch{
			Type:         BatchSimple,
			DelegationID: ts.DelegationID,
			Transfers:    []*Transfer{transfer},
			Vo:           ts.Vo,
		})
	}
	return set
}

// GetID gets a unique Id associated to the batch
func (ts *Batch) GetID() string {
	hash := md5.New()
	for _, transfer := range ts.Transfers {
		hash.Write([]byte(transfer.TransferID))
	}
	var sum []byte
	sum = hash.Sum(sum)
	return fmt.Sprintf("%x", sum)
}
