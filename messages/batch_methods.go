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

package messages

import (
	"crypto/md5"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrEmptyTransferSet is returned when the batch is empty (has no transfers)
	ErrEmptyTransferSet = errors.New("Empty batch")
	// ErrCannotMerge is returned when the two batches can not be merged
	ErrCannotMerge = errors.New("Batches can not be merged")
	// ErrMissingInformation is returned when there are missing fields required for the batch to be routed
	ErrMissingInformation = errors.New("Missing fields")
	// ErrMissingTransferId is returned when the transfer id is missing
	ErrMissingTransferId = errors.New("Missing transfer id")
)

// Validate checks if a batch is properly defined
func (b *Batch) Validate() error {
	if len(b.Transfers) == 0 {
		return ErrEmptyTransferSet
	}
	if b.SourceSe == "" || b.DestSe == "" || b.Vo == "" || b.Activity == "" {
		return ErrMissingInformation
	}
	for _, t := range b.Transfers {
		if err := t.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// GetID gets a unique id associated to the batch
func (b *Batch) GetID() string {
	hash := md5.New()
	for _, transfer := range b.Transfers {
		hash.Write([]byte(transfer.TransferId))
	}
	var sum []byte
	sum = hash.Sum(sum)
	return fmt.Sprintf("%x", sum)
}

// GetPath is called by the scheduler to decide the scheduling levels
func (b *Batch) GetPath() []string {
	return []string{b.DestSe, b.Vo, b.Activity, b.SourceSe}
}

// GetTimestamp returns the submit timestamp of the batch
func (b *Batch) GetTimestamp() time.Time {
	return time.Unix(b.Submitted.Seconds, int64(b.Submitted.Nanos)).UTC()
}

// Validate checks if a transfer is properly defined
func (t *Transfer) Validate() error {
	if t.TransferId == "" {
		return ErrMissingTransferId
	}
	if t.Source == "" {
		return fmt.Errorf("Empty source SURL for %s", t.TransferId)
	}
	if t.Destination == "" {
		return fmt.Errorf("Empty destination SURL for %s", t.TransferId)
	}
	return nil
}
