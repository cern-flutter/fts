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
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/satori/go.uuid"
	"gitlab.cern.ch/flutter/fts/config"
	"gitlab.cern.ch/flutter/fts/types/tasks"
	"gitlab.cern.ch/flutter/stomp"
)

type (
	// Scheduler wraps the scheduler status
	Scheduler struct {
		producer *stomp.Producer
		consumer *stomp.Consumer
	}
)

// New creates a new scheduler
func New(params stomp.ConnectionParameters) (*Scheduler, error) {
	var err error
	sched := &Scheduler{}

	if sched.producer, err = stomp.NewProducer(params); err != nil {
		return nil, err
	}
	if sched.consumer, err = stomp.NewConsumer(params); err != nil {
		return nil, err
	}
	return sched, nil
}

// Close finishes the scheduler
func (s *Scheduler) Close() {
	s.consumer.Close()
	s.producer.Close()
}

// Run the scheduler
func (s *Scheduler) Run() error {
	consumerId := fmt.Sprint("fts-scheduler-", uuid.NewV4().String())
	taskChannel, errorChannel, err := s.consumer.Subscribe(
		config.SchedulerQueue,
		consumerId,
		stomp.AckIndividual,
	)
	if err != nil {
		return err
	}

	for {
		select {
		case msg, ok := <-taskChannel:
			if !ok {
				return nil
			}
			batch := tasks.Batch{}
			if err = json.Unmarshal(msg.Body, &batch); err != nil {
				msg.Nack()
				log.WithError(err).Error("Could not parse batch")
			}
			msg.Ack()

			// We are only interested on SUBMITTED batches
			if batch.State == tasks.Submitted {
				// This is an identity dummy scheduler, so forward to workers
				log.WithField("batch", batch.GetID()).Info("Forwarding batch")
				batch.State = tasks.Ready
				body, err := json.Marshal(batch)
				if err != nil {
					return err
				}

				err = s.producer.Send(
					config.TransferTopic,
					string(body),
					stomp.SendParams{
						Persistent:  true,
						ContentType: "application/json",
					},
				)
				if err != nil {
					return err
				}
			} else {
				log.WithField("batch", batch.GetID()).Debug("Ignoring batch with state ", batch.State)
			}
		case error, ok := <-errorChannel:
			if !ok {
				return nil
			}
			log.WithError(error).Warn("Got an error from the subcription channel")
		}
	}
}

// Go runs the scheduler as a goroutine
func (s *Scheduler) Go() <-chan error {
	c := make(chan error)
	go func() {
		if err := s.Run(); err != nil {
			c <- err
		}
		close(c)
	}()
	return c
}
