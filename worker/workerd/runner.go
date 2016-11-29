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
	"github.com/satori/go.uuid"
	"gitlab.cern.ch/flutter/fts/config"
	"gitlab.cern.ch/flutter/fts/messages"
	"gitlab.cern.ch/flutter/stomp"
	"syscall"
)

type (
	// Runner subsystem gets batches and runs the corresponding url-copy
	Runner struct {
		Context  *Worker
		consumer *stomp.Consumer
		producer *stomp.Producer
	}
)

// Run executes the Runner subroutine
func (r *Runner) Run() error {
	var err error

	if r.consumer, err = stomp.NewConsumer(r.Context.params.StompParams); err != nil {
		return err
	}
	if r.producer, err = stomp.NewProducer(r.Context.params.StompParams); err != nil {
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
				batch := &messages.Batch{}
				if err := proto.Unmarshal(m.Body, batch); err != nil {
					log.Error("Malformed task: ", err)
					return
				} else if err := batch.Validate(); err != nil {
					log.Error("Invalid task: ", err)
					return
				}

				l := log.WithField("batch", batch.GetID())

				if batch.State != messages.Batch_READY {
					log.WithField("batch", batch.GetID()).Info("Ignoring batch in state ", batch.State)
					return
				}

				l.Info("Received batch")
				if pid, err := RunTransfer(r.Context, batch); err != nil {
					l.Error("Failed to run the batch: ", err)
					r.notifyBatchFailure(batch, err.Error(), int32(syscall.EINPROGRESS))
				} else {
					l.Info("Spawn with pid ", pid)
					if err := r.Context.supervisor.RegisterProcess(batch, pid); err != nil {
						l.WithError(err).Error("Failed to register batch into local DB")
					}
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

// notifyBatchFailure sends an error when failed to run the transfer
func (r *Runner) notifyBatchFailure(batch *messages.Batch, error string, code int32) {
	batch.State = messages.Batch_DONE
	for _, t := range batch.Transfers {
		t.Info = &messages.TransferInfo{
			Error: &messages.TransferError{
				Scope:       messages.TransferError_AGENT,
				Code:        code,
				Description: error,
				Recoverable: false,
			},
		}
	}
	payload, err := proto.Marshal(batch)
	if err != nil {
		log.Panicf("Failed to marshal the message with the error: %s", err.Error())
	}
	err = r.producer.Send(config.TransferTopic, string(payload), stomp.SendParams{Persistent: true})
	if err != nil {
		log.Error(err)
	}
}
