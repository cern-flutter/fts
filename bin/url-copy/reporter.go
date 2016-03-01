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
	"encoding/json"
	"flag"
	"gitlab.cern.ch/flutter/fts/types/perf"
	"gitlab.cern.ch/flutter/fts/types/tasks"
	"gitlab.cern.ch/flutter/go-dirq"
	"path"
	"time"
)

var dirqBasePath = flag.String("DirQ", "/var/lib/fts3", "Base dir for dirq messages")

// ReportStart sends the start message.
func ReportStart(transfer *tasks.Transfer) error {
	startPath := path.Join(*dirqBasePath, "start")
	startDirq, err := dirq.New(startPath)
	if err != nil {
		return err
	}
	data, err := json.Marshal(transfer)
	if err != nil {
		return err
	}
	return startDirq.Produce(data)
}

// ReportStart sends the end (or terminal) message.
func ReportTerminal(transfer *tasks.Transfer) error {
	endPath := path.Join(*dirqBasePath, "end")
	endDirq, err := dirq.New(endPath)
	if err != nil {
		return err
	}
	data, err := json.Marshal(transfer)
	if err != nil {
		return err
	}
	return endDirq.Produce(data)
}

// ReportPerformance sends the progress of a transfer.
func ReportPerformance(perf *perf.Marker) error {
	perf.Timestamp = time.Now()

	perfPath := path.Join(*dirqBasePath, "perf")
	perfDirq, err := dirq.New(perfPath)
	if err != nil {
		return err
	}
	data, err := json.Marshal(perf)
	if err != nil {
		return err
	}
	return perfDirq.Produce(data)
}
