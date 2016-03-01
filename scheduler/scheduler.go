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
	"github.com/streadway/amqp"
	"gitlab.cern.ch/flutter/fts/bus/exchanges"
	"gitlab.cern.ch/flutter/fts/bus/queues"
	"gitlab.cern.ch/flutter/fts/types/tasks"
)

type (
	// Scheduler wraps the scheduler status
	Scheduler struct {
		connection          *amqp.Connection
		subscriptionChannel *amqp.Channel
		publishChannel      *amqp.Channel
	}
)

// New creates a new scheduler
func New(amqpAddress string) (*Scheduler, error) {
	var err error
	sched := &Scheduler{}

	if sched.connection, err = amqp.Dial(amqpAddress); err != nil {
		return nil, err
	}
	if sched.subscriptionChannel, err = sched.connection.Channel(); err != nil {
		return nil, err
	}
	if sched.publishChannel, err = sched.connection.Channel(); err != nil {
		return nil, err
	}
	if err = exchanges.Transition.Declare(sched.subscriptionChannel); err != nil {
		return nil, err
	}
	if err = queues.Submissions.Declare(sched.subscriptionChannel); err != nil {
		return nil, err
	}
	return sched, nil
}

// Run the scheduler
func (s *Scheduler) Run() error {
	var err error
	var taskChannel <-chan amqp.Delivery

	if taskChannel, err = s.subscriptionChannel.Consume(
		queues.Submissions.Name,
		"",    // consumer id
		false, // auto-ack
		true,  // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	); err != nil {
		return err
	}

	for {
		select {
		case msg := <-taskChannel:
			batch := tasks.Batch{}
			if err = json.Unmarshal(msg.Body, &batch); err != nil {
				msg.Reject(false)
				log.Error("Could not parse batch: ", err)
			}
			msg.Ack(false)
			// This is an identity dummy scheduler, so forward to workers
			log.Info("Forwarding batch ", batch.GetID())
			s.publishChannel.Publish(
				exchanges.Transition.Name,
				tasks.Ready,
				false, // mandatory
				false, // immediate
				amqp.Publishing{
					ContentType:  "application/json",
					DeliveryMode: amqp.Persistent,
					Priority:     0,
					Body:         msg.Body,
				},
			)
		case e := <-s.subscriptionChannel.NotifyClose(make(chan *amqp.Error)):
			return e
		}
	}
}

// Go runs the scheduler as a goroutine
func (s *Scheduler) Go() <-chan error {
	c := make(chan error)
	go func() {
		c <- s.Run()
	}()
	return c
}
