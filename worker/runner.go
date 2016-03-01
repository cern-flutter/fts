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
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
	"gitlab.cern.ch/flutter/fts/bus/exchanges"
	"gitlab.cern.ch/flutter/fts/bus/queues"
	"gitlab.cern.ch/flutter/fts/types/tasks"
)

type (
	// Runner subsystem gets batches and runs the corresponding url-copy
	Runner struct {
		Context *Context
		channel *amqp.Channel
	}
)

// Run executes the Runner subroutine
func (r *Runner) Run() error {
	if err := r.openChannel(); err != nil {
		return err
	}
	if err := r.subscribe(); err != nil {
		return err
	}

	log.Info("Runner started")

	taskChannel, err := r.consume()
	if err != nil {
		return err
	}

	for {
		select {
		case m := <-taskChannel:
			m.Ack(false)
			go func() {
				var batch tasks.Batch
				if err := json.Unmarshal(m.Body, &batch); err != nil {
					log.Error("Malformed task: ", err)
				} else if err := batch.Validate(); err != nil {
					log.Error("Invalid task: ", err)
				} else {
					log.Info("Received batch ", batch.GetID())
					if pid, err := RunTransfer(r.Context, &batch); err != nil {
						log.Error("Failed to run the batch: ", err)
						// TODO: Notify failure
					} else {
						log.Info("Spawn with pid ", pid)
						// TODO: Store PID
					}
				}
			}()
		case e := <-r.channel.NotifyClose(make(chan *amqp.Error)):
			return e
		}
	}
}

// Go executes the Runner subroutine as a goroutine
func (r *Runner) Go() <-chan error {
	c := make(chan error)
	go func() {
		c <- r.Run()
	}()
	return c
}

// openChannel opens a new channel for the runner
func (r *Runner) openChannel() error {
	var err error
	r.channel, err = r.Context.amqpConnection.Channel()
	if err != nil {
		return err
	}
	if err = r.channel.Qos(1, 0, false); err != nil {
		return fmt.Errorf("Could not set the QoS: %s", err.Error())
	}
	return nil
}

// subscribe prepares the subscription
func (r *Runner) subscribe() error {
	var err error

	if err = exchanges.Transition.Declare(r.channel); err != nil {
		return err
	}

	if err = queues.Worker.Declare(r.channel); err != nil {
		return err
	}
	return nil
}

// consume starts consuming from the worker task queue
func (r *Runner) consume() (<-chan amqp.Delivery, error) {
	var err error
	var msgs <-chan amqp.Delivery

	if msgs, err = r.channel.Consume(
		queues.Worker.Name,
		"",    // consumer id
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	); err != nil {
		return nil, err
	}
	return msgs, nil
}
