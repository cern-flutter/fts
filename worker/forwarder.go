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
	"fmt"
	log "github.com/Sirupsen/logrus"
	"gitlab.cern.ch/flutter/fts/config"
	"gitlab.cern.ch/flutter/go-dirq"
	"gitlab.cern.ch/flutter/stomp"
	"path"
	"time"
)

type (
	// Forwarder subsystem sends local messages to the global bus
	Forwarder struct {
		Context          *Worker
		producer         *stomp.Producer
		start, end, perf *dirq.Dirq
	}
)

// Run executes the Forwarder subroutine
func (f *Forwarder) Run() error {
	var err error

	if err = f.subscribeLocalQueues(); err != nil {
		return err
	}
	if f.producer, err = stomp.NewProducer(f.Context.params.StompParams); err != nil {
		return err
	}

	log.Info("Forwarder started")
	for {
		if err := f.forwardStart(); err != nil {
			return err
		}
		if err := f.forwardEnd(); err != nil {
			return err
		}
		if err := f.forwardPerf(); err != nil {
			return err
		}
		time.Sleep(5 * time.Second)
	}
}

// subscribeLocalQueues subscribes to local directory queues
func (f *Forwarder) subscribeLocalQueues() error {
	var err error
	if f.start, err = dirq.New(path.Join(f.Context.params.DirQPath, "start")); err != nil {
		return fmt.Errorf("Could not subscribe to start queue: %s", err.Error())
	}
	if f.end, err = dirq.New(path.Join(f.Context.params.DirQPath, "end")); err != nil {
		return fmt.Errorf("Could not subscribe to end queue: %s", err.Error())
	}
	if f.perf, err = dirq.New(path.Join(f.Context.params.DirQPath, "perf")); err != nil {
		return fmt.Errorf("Could not subscribe to performance queue: %s", err.Error())
	}
	return nil
}

// forwardStart consumes local start messages and forward them to amqp.
func (f *Forwarder) forwardStart() error {
	for start := range f.start.Consume() {
		if start.Error != nil {
			return start.Error
		}
		if err := f.producer.Send(
			config.TransferTopic,
			string(start.Message),
			stomp.SendParams{
				Persistent:  true,
				ContentType: "application/json",
			},
		); err != nil {
			return err
		}
		log.Debug("Forwarded start message")
		log.Debug(string(start.Message))
	}
	return nil
}

// forwardEnd consumes local end messages and forward them to amqp.
func (f *Forwarder) forwardEnd() error {
	for end := range f.end.Consume() {
		if end.Error != nil {
			return end.Error
		}
		if err := f.producer.Send(
			config.TransferTopic,
			string(end.Message),
			stomp.SendParams{
				Persistent:  true,
				ContentType: "application/json",
			},
		); err != nil {
			return err
		}
		log.Debug("Forwarded end message")
		log.Debug(string(end.Message))
	}
	return nil
}

// forwardPerf consumes local performance messages and forward them to amqp.
func (f *Forwarder) forwardPerf() error {
	for perf := range f.perf.Consume() {
		if perf.Error != nil {
			return perf.Error
		}
		if err := f.producer.Send(
			config.PerformanceTopic,
			string(perf.Message),
			stomp.SendParams{
				Persistent:  false,
				ContentType: "application/json",
			},
		); err != nil {
			return err
		}
		log.Debug("Forwarded performance message")
		log.Debug(string(perf.Message))
	}
	return nil
}
