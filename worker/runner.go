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

package worker

import (
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"github.com/satori/go.uuid"
	"gitlab.cern.ch/flutter/fts/config"
	"gitlab.cern.ch/flutter/fts/types/tasks"
	"gitlab.cern.ch/flutter/stomp"
)

type (
	// Runner subsystem gets batches and runs the corresponding url-copy
	Runner struct {
		Context  *Context
		consumer *stomp.Consumer
	}
)

// Run executes the Runner subroutine
func (r *Runner) Run() error {
	var err error

	if r.consumer, err = stomp.NewConsumer(r.Context.params.StompParams); err != nil {
		return err
	}

	var taskChannel <-chan stomp.Message
	var errorChannel <-chan error
	if taskChannel, errorChannel, err = r.consumer.Subscribe(
		config.WorkerQueue,
		"fts-worker-"+uuid.NewV4().String(),
		stomp.AckIndividual,
	); err != nil {
		return err
	}

	log.Info("Runner started")
	for {
		select {
		case m, ok := <-taskChannel:
			if !ok {
				return nil
			}
			m.Ack()

			go func() {
				var batch tasks.Batch
				if err := json.Unmarshal(m.Body, &batch); err != nil {
					log.Error("Malformed task: ", err)
				} else if err := batch.Validate(); err != nil {
					log.Error("Invalid task: ", err)
				} else if batch.State == tasks.Ready {
					log.WithField("batch", batch.GetID()).Info("Received batch")
					if pid, err := RunTransfer(r.Context, &batch); err != nil {
						log.Error("Failed to run the batch: ", err)
						// TODO: Notify failure
					} else {
						log.Info("Spawn with pid ", pid)
						// TODO: Store PID
					}
				} else {
					log.WithField("batch", batch.GetID()).Info("Ignoring batch in state ", batch.State)
				}
			}()
		case error, ok := <-errorChannel:
			if !ok {
				return nil
			}
			log.WithError(error).Warn("Got an error from the subcription channel")
		}
	}
}

// Go executes the Runner subroutine as a goroutine
func (r *Runner) Go() <-chan error {
	c := make(chan error)
	go func() {
		if err := r.Run(); err != nil {
			c <- err
		}
		close(c)
	}()
	return c
}
