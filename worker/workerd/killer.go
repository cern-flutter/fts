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
	log "github.com/Sirupsen/logrus"
	"github.com/satori/go.uuid"
	"gitlab.cern.ch/flutter/fts/config"
	"gitlab.cern.ch/flutter/fts/messages"
	"gitlab.cern.ch/flutter/stomp"
)

type (
	// Killer subsystem listen for cancellations, and kill local processes if needed
	Killer struct {
		Context  *Worker
		consumer *stomp.Consumer
	}
)

// Run executes the Killer subroutine
func (k *Killer) Run() error {
	var err error

	if k.consumer, err = stomp.NewConsumer(k.Context.params.StompParams); err != nil {
		return err
	}

	var killChannel <-chan stomp.Message
	var errorChannel <-chan error
	if killChannel, errorChannel, err = k.consumer.Subscribe(
		config.KillTopic,
		"fts-worker-"+uuid.NewV4().String(),
		stomp.AckAuto,
	); err != nil {
		return err
	}

	log.Info("Killer started")
	for {
		select {
		case m, ok := <-killChannel:
			if !ok {
				return nil
			}
			var kill messages.Kill
			if err := json.Unmarshal(m.Body, &kill); err != nil {
				log.WithError(err).Error("Malformed kill message")
			} else {
				log.Info("Got kill signal")
				pids := k.Context.supervisor.GetPidsForKillTask(&kill)
				for _, pid := range pids {
					k.Context.supervisor.Kill(pid)
				}
			}
		case error, ok := <-errorChannel:
			if !ok {
				return nil
			}
			log.WithError(error).Warn("Got an error from the subcription channel")
		}
	}
}
