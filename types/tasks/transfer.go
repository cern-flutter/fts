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
	"errors"
	"fmt"
	"gitlab.cern.ch/flutter/fts/types/surl"
	"time"
)

type (
	// TransferID is a unique identifier of a transfer
	TransferID string
	// JobID is an identifier used to bulk several transfers together from the user's point of view
	JobID string
	// Metadata is any random string sent by the user
	Metadata string
	// Checksum is of the form algorithm:value
	Checksum string
	// ChecksumMode defines how (and if) to perform the checksum validation
	ChecksumMode string

	// TransferParameters determines the transfer behaviour
	TransferParameters struct {
		// If true, there will be no preparatory steps
		OnlyCopy bool `json:"only_copy"`
		// TCP buffer size
		TCPBufferSize uint32 `json:"tcp_buffer_size"`
		// Number of TCP connections if supported by the protocol
		NoStreams uint8 `json:"nstreams"`
		// Transfer timeout
		Timeout int `json:"timeout"`
		// Number of times to retry on failure
		Retry uint8 `json:"retry"`
		// How long to wait between attempts
		RetryDelay time.Duration `json:"retry_delay"`

		// Staging operation timeout, enforced by the storage
		StagingTimeout uint32 `json:"staging_timeout"`
		// How long should the storage keep the file on disk
		PinLifetime uint32 `json:"pin_lifetime"`
		// Source space token, for staging operations
		SourceSpaceToken *string `json:"source_spacetoken"`
		// Destination space token, for the transfer
		DestSpaceToken *string `json:"dest_spacetoken"`

		// Checksum mode
		ChecksumMode ChecksumMode `json:"checksum_mode"`

		// Overwrite
		Overwrite bool `json:"overwrite"`

		// If true, try UDT for GridFTP transfers
		EnableUdt bool `json:"enable_udt"`
		// If true, enable IPv6 for GridFTP transfers
		EnableIpv6 bool `json:"enable_ipv6"`
	}

	// Transfer to be run by FTS
	Transfer struct {
		// Job to which this transfer belongs
		JobID JobID `json:"job_id"`
		// Transfer Id
		TransferID `json:"transfer_id"`
		// Retry index
		Retry int `json:"retry"`

		// Expiration time
		ExpirationTime *time.Time `json:"expiration_time"`

		// Source file
		Source surl.SURL `json:"source"`
		// Destination file
		Destination surl.SURL `json:"destination"`

		// Activity share
		Activity string

		// File size
		Filesize *int64 `json:"filesize"`
		// File checksum
		Checksum *Checksum `json:"checksum"`

		// Custom metadata
		Metadata Metadata `json:"metadata"`

		// Additional parameters for the transfer
		Parameters TransferParameters `json:"params"`

		// State info for when it is due
		Status *TransferStatus `json:"status"`
	}
)

const (
	// ChecksumSkip skips the checksum validation
	ChecksumSkip = ChecksumMode("Skip")
	// ChecksumRelaxed performs the validation, but a missing checksum on the source is not considered a failure
	ChecksumRelaxed = ChecksumMode("Relaxed")
	// ChecksumStrict performs a full checksum validation
	ChecksumStrict = ChecksumMode("Strict")
)

// Validate checks if a transfer is properly defined
func (t *Transfer) Validate() error {
	if t.TransferID == "" {
		return errors.New("Missing transfer id")
	}
	if t.JobID == "" {
		return fmt.Errorf("Missing job id for %s", t.TransferID)
	}
	if t.Source.Empty() {
		return fmt.Errorf("Empty source SURL for %s", t.TransferID)
	}
	if t.Destination.Empty() {
		return fmt.Errorf("Empty destination SURL for %s", t.TransferID)
	}
	return nil
}
