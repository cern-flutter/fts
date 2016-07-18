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

package perf

import (
	"gitlab.cern.ch/flutter/fts/types/tasks"
	"time"
)

type (
	// Marker holds data indicating the progress of a transfer
	Marker struct {
		Timestamp          time.Time        `json:"timestamp"`
		JobID              tasks.JobID      `json:"job_id"`
		TransferID         tasks.TransferID `json:"transfer_id"`
		SourceStorage      string           `json:"source_se"`
		DestinationStorage string           `json:"dest_se"`
		Throughput         uint64           `json:"throughput"`
		TransferredBytes   uint64           `json:"transferred"`
	}
)
