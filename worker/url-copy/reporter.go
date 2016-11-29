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
	"flag"
	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"gitlab.cern.ch/flutter/fts/messages"
	"gitlab.cern.ch/flutter/go-dirq"
	"path"
)

var dirqBasePath = flag.String("DirQ", "/var/lib/fts3", "Base dir for dirq messages")

// reportBatchStart sends the start message.
func (copy *urlCopy) reportBatchStart() {
	startPath := path.Join(*dirqBasePath, "start")
	startDirq, err := dirq.New(startPath)
	if err != nil {
		log.Panic(err)
	}
	copy.batch.State = messages.Batch_RUNNING
	data, err := proto.Marshal(&copy.batch)
	if err != nil {
		log.Panic(err)
	}
	if err = startDirq.Produce(data); err != nil {
		log.Panic(err)
	}
}

// reportBatchEnd sends the end (or terminal) message.
func (copy *urlCopy) reportBatchEnd() {
	copy.mutex.Lock()
	defer copy.mutex.Unlock()

	if copy.terminalSent {
		return
	}

	nFinished := 0
	for _, t := range copy.batch.Transfers {
		if t.Info != nil && t.State == messages.Transfer_FINISHED {
			nFinished++
		}
	}

	copy.batch.State = messages.Batch_DONE

	endPath := path.Join(*dirqBasePath, "end")
	endDirq, err := dirq.New(endPath)
	if err != nil {
		log.Panic(err)
	}
	data, err := proto.Marshal(&copy.batch)
	if err != nil {
		log.Panic(err)
	}
	if err = endDirq.Produce(data); err != nil {
		log.Panic(err)
	}

	copy.terminalSent = true
}

// ReportPerformance sends the progress of a transfer.
func (copy *urlCopy) reportPerformance(perf *messages.PerformanceMarker) error {
	perf.Timestamp = messages.Now()
	perfPath := path.Join(*dirqBasePath, "perf")
	perfDirq, err := dirq.New(perfPath)
	if err != nil {
		return err
	}
	data, err := proto.Marshal(perf)
	if err != nil {
		return err
	}
	return perfDirq.Produce(data)
}
