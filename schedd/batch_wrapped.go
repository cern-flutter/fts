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

package main

import (
	"crypto/md5"
	"fmt"
	"gitlab.cern.ch/flutter/fts/messages"
	"time"
)

// BatchWrapped decorates a Batch with three required methods for Echelon
type BatchWrapped struct {
	messages.Batch
}

// GetID gets a unique id associated to the batch
func (ts *BatchWrapped) GetID() string {
	hash := md5.New()
	for _, transfer := range ts.Transfers {
		hash.Write([]byte(transfer.TransferId))
	}
	var sum []byte
	sum = hash.Sum(sum)
	return fmt.Sprintf("%x", sum)
}

// GetPath is called by the scheduler to decide the scheduling levels
func (ts *BatchWrapped) GetPath() []string {
	return []string{ts.DestSe, ts.Vo, ts.Activity, ts.SourceSe}
}

// GetTimestamp returns the submit timestamp of the batch
func (ts *BatchWrapped) GetTimestamp() time.Time {
	return time.Unix(ts.Timestamp.Seconds, int64(ts.Timestamp.Nanos)).UTC()
}
