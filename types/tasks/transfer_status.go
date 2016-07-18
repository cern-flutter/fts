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
	"gitlab.cern.ch/flutter/fts/types/interval"
	"syscall"
)

const (
	// ScopeSource when the error was triggered by the source storage
	ScopeSource = "Source"
	// ScopeDestination when the error was triggered by the destination storage
	ScopeDestination = "Destination"
	// ScopeTransfer when the error was triggered during the transfer
	ScopeTransfer = "Transfer"
	// ScopeAgent when the error is related to FTS (misconfiguration, bugs...)
	ScopeAgent = "Agent"
)

// Normal flow
// 	Submitted -> Ready -> Active -> Finished/Failed
// Optionally, if staging is required
//	Staging -> Started -> [Submitted -> Ready -> Active] -> Finished/Failed
const (
	Staging       = "STAGING"
	Started       = "STARTED"
	Submitted     = "SUBMITTED"
	Ready         = "READY"
	Active        = "ACTIVE"
	Finished      = "FINISHED"
	Failed        = "FAILED"
	Canceled      = "CANCELED"
	FinishedDirty = "FINISHEDDIRTY"
)

type (
	// TransferError holds details about a transfer error
	TransferError struct {
		Scope       string        `json:"scope"`
		Code        syscall.Errno `json:"code"`
		Description string        `json:"description"`
		Recoverable bool          `json:"recoverable"`
	}

	// TransferIntervals holds details about the time it took each stage
	TransferIntervals struct {
		Total interval.Interval `json:"total"`

		Transfer        *interval.Interval `json:"transfer_time,omitempty"`
		SourceChecksum  *interval.Interval `json:"source_checksum,omitempty"`
		DestChecksum    *interval.Interval `json:"dest_checksum,omitempty"`
		SrmPreparation  *interval.Interval `json:"srm_preparation,omitempty"`
		SrmFinalization *interval.Interval `json:"srm_finalization,omitempty"`
	}

	// TransferRunStatistics holds details about a transfer execution
	TransferRunStatistics struct {
		Throughput       float32 `json:"throughput"`
		TransferredBytes int64   `json:"transferred"`

		Intervals TransferIntervals `json:"intervals"`
	}

	// TransferStatus holds the specific status of a transfer during the whole process chain
	TransferStatus struct {
		// Transfer state
		State string `json:"state"`
		// Error, if any
		Error *TransferError `json:"error"`
		// Associated message
		Message string `json:"message"`
		// Statistics, when it is done
		Stats *TransferRunStatistics `json:"stats"`
		// Log file, for when it s running
		LogFile *string `json:"log"`
	}
)
