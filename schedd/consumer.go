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
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/satori/go.uuid"
	"gitlab.cern.ch/flutter/fts/config"
	"gitlab.cern.ch/flutter/fts/messages"
	"gitlab.cern.ch/flutter/stomp"
)

// RunConsumer runs the scheduler consumer
func (s *Scheduler) RunConsumer() error {
	consumerID := fmt.Sprint("fts-scheduler-", uuid.NewV4().String())
	taskChannel, errorChannel, err := s.consumer.Subscribe(
		config.SchedulerQueue,
		consumerID,
		stomp.AckIndividual,
	)
	if err != nil {
		return err
	}

	log.Info("Consumer started")

	for {
		select {
		case msg, ok := <-taskChannel:
			if !ok {
				return nil
			}
			batch := messages.Batch{}
			if err = proto.Unmarshal(msg.Body, &batch); err != nil {
				msg.Nack()
				log.WithError(err).Error("Could not parse batch")
				continue
			}

			l := log.WithField("batch", batch.GetID())
			for _, t := range batch.Transfers {
				l.Debugf("Transfer %s", t.TransferId)
			}

			// We are only interested on SUBMITTED batches
			switch batch.State {
			case messages.Batch_SUBMITTED:
				err = s.echelon.Enqueue(&batch)
				if err != nil {
					return err
				}
				l.Info("Enqueued batch job")
			case messages.Batch_DONE:
				if err = s.scoreboard.ReleaseSlot(&batch); err != nil {
					return err
				}
				l.Info("Batch job done, released slots")
			default:
				l.Debug("Ignoring batch with state ", batch.State)
			}
			msg.Ack()
		case error, ok := <-errorChannel:
			if !ok {
				return nil
			}
			log.WithError(error).Warn("Got an error from the subcription channel")
		}
	}
}
