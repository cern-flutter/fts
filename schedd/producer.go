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
	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"gitlab.cern.ch/flutter/echelon"
	"gitlab.cern.ch/flutter/fts/config"
	"gitlab.cern.ch/flutter/fts/messages"
	"gitlab.cern.ch/flutter/stomp"
	"time"
)

// RunProducer runs the scheduler producer
func (s *Scheduler) RunProducer() error {
	sendParams := stomp.SendParams{Persistent: true, ContentType: "application/json"}

	log.Info("Producer started")

	for {
		var err error
		decorated := &BatchWrapped{}
		for err = s.echelon.Dequeue(decorated); err == nil; err = s.echelon.Dequeue(decorated) {
			l := log.WithField("batch", decorated.GetID())
			decorated.State = messages.Batch_READY

			var data []byte
			if data, err = proto.Marshal(&decorated.Batch); err != nil {
				l.WithError(err).Error("Failed to marshal task")
				continue
			}

			if err := s.scoreboard.ConsumeSlot(&decorated.Batch); err != nil {
				l.WithError(err).Error("Failed to mark task as busy")
			} else if err = s.producer.Send(config.TransferTopic, string(data), sendParams); err != nil {
				l.WithError(err).Error("Failed to send the batch to que message queue")
			}

			if err != nil {
				l.Warn("Trying to requeue the batch")
				if err = s.echelon.Enqueue(decorated); err != nil {
					l.Panic(err)
				}
			} else {
				for _, t := range decorated.Transfers {
					l.Info("Scheduled ", t.JobId, "/", t.TransferId)
				}
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
