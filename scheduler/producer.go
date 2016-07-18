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

package scheduler

import (
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"gitlab.cern.ch/flutter/echelon"
	"gitlab.cern.ch/flutter/fts/config"
	"gitlab.cern.ch/flutter/fts/types/tasks"
	"gitlab.cern.ch/flutter/stomp"
	"time"
)

// Run the scheduler producer
func (s *Scheduler) RunProducer() error {
	sendParams := stomp.SendParams{Persistent: true, ContentType: "application/json"}

	for {

		var err error
		batch := &tasks.Batch{}
		for err = s.echelon.Dequeue(batch); err == nil; err = s.echelon.Dequeue(batch) {
			l := log.WithField("batch", batch.GetID())

			batch.State = tasks.Ready

			if data, err := json.Marshal(batch); err != nil {
				l.Error(err)
				continue
			} else if err = s.producer.Send(config.TransferTopic, string(data), sendParams); err != nil {
				l.Error(err)
				break
			}
			for _, t := range batch.Transfers {
				l.Info("Scheduled ", t.JobID, "/", t.TransferID)
			}
		}

		switch err {
		case echelon.ErrEmpty:
			log.Info("Empty queue")
		case echelon.ErrNotEnoughSlots:
			log.Info("Run out of available slots")
		default:
			log.Error("Unexpected error: ", err)
		}

		// TODO: Make configurable the sleep interval
		time.Sleep(15 * time.Second)
	}
}

// Go runs the scheduler producer as a goroutine
func (s *Scheduler) GoProducer() <-chan error {
	c := make(chan error)
	go func() {
		if err := s.RunProducer(); err != nil {
			c <- err
		}
		close(c)
	}()
	return c
}
