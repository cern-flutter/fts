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
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
	"gitlab.cern.ch/flutter/fts/bus/exchanges"
	"gitlab.cern.ch/flutter/fts/types/tasks"
	"gitlab.cern.ch/flutter/go-dirq"
	"path"
	"time"
)

type (
	// Forwarder subsystem sends local messages to the global bus
	Forwarder struct {
		Context          *Context
		channel          *amqp.Channel
		start, end, perf *dirq.Dirq
	}
)

// Run executes the Forwarder subroutine
func (f *Forwarder) Run() error {
	if err := f.subscribeLocalQueues(); err != nil {
		return err
	}
	if err := f.openChannel(); err != nil {
		return err
	}
	if err := exchanges.Transition.Declare(f.channel); err != nil {
		return err
	}
	if err := exchanges.Performance.Declare(f.channel); err != nil {
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

// Go executes the Forwarder subroutine as a goroutine
func (f *Forwarder) Go() <-chan error {
	c := make(chan error)
	go func() {
		c <- f.Run()
	}()
	return c
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

// openChannel opens a new channel for the forwarder
func (f *Forwarder) openChannel() error {
	var err error
	f.channel, err = f.Context.amqpConnection.Channel()
	if err != nil {
		return err
	}
	return nil
}

// forwardStart consumes local start messages and forward them to amqp.
func (f *Forwarder) forwardStart() error {
	for start := range f.start.Consume() {
		if start.Error != nil {
			return start.Error
		}
		if err := f.channel.Publish(
			exchanges.Transition.Name,
			tasks.Active,
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp.Persistent,
				Timestamp:    time.Now(),
				Body:         start.Message,
			},
		); err != nil {
			return err
		}
		log.Debug("Forwarded start message")
	}
	return nil
}

// forwardEnd consumes local end messages and forward them to amqp.
func (f *Forwarder) forwardEnd() error {
	for end := range f.end.Consume() {
		if end.Error != nil {
			return end.Error
		}
		transfer := tasks.Transfer{}
		if err := json.Unmarshal(end.Message, &transfer); err != nil {
			return err
		} else if transfer.Status == nil {
			return errors.New("Status is nil")
		}
		if err := f.channel.Publish(
			exchanges.Transition.Name,
			transfer.Status.State,
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp.Persistent,
				Timestamp:    time.Now(),
				Body:         end.Message,
			},
		); err != nil {
			return err
		}
		log.Debug("Forwarded end message")
	}
	return nil
}

// forwardPerf consumes local performance messages and forward them to amqp.
func (f *Forwarder) forwardPerf() error {
	for perf := range f.perf.Consume() {
		if perf.Error != nil {
			return perf.Error
		}
		if err := f.channel.Publish(
			exchanges.Performance.Name,
			"perf",
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp.Persistent,
				Timestamp:    time.Now(),
				Body:         perf.Message,
			},
		); err != nil {
			return err
		}
		log.Debug("Forwarded performance message")
	}
	return nil
}
