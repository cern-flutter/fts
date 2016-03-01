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

package queues

import (
	"github.com/streadway/amqp"
	"gitlab.cern.ch/flutter/fts/bus/exchanges"
	"gitlab.cern.ch/flutter/fts/types/tasks"
)

// Bind defines a bind between a queue and an exchange
type Bind struct {
	Exchange   *exchanges.Exchange
	RoutingKey string
}

// Queue defines a queue on the message broker
type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Bind       *Bind
}

var (
	// Worker is the queue used by all workers to split the load
	Worker = &Queue{
		Name:       "fts.q.worker",
		Durable:    true,
		AutoDelete: false,
		Bind: &Bind{
			Exchange:   exchanges.Transition,
			RoutingKey: tasks.Ready,
		},
	}

	// Submissions is the queue used by the scheduler. Only one scheduler should
	// consume from this.
	Submissions = &Queue{
		Name:       "fts.q.submitted",
		Durable:    true,
		AutoDelete: false,
		Bind: &Bind{
			Exchange:   exchanges.Transition,
			RoutingKey: tasks.Submitted,
		},
	}
)

// Declare a new queue, and do the binding as well if configured.
func (q *Queue) Declare(channel *amqp.Channel) error {
	if _, err := channel.QueueDeclare(
		q.Name,
		q.Durable,
		q.AutoDelete,
		false, // exclusive
		false, // no-wait
		nil,   // args
	); err != nil {
		return err
	}

	if q.Bind != nil {
		if err := channel.QueueBind(
			q.Name,
			q.Bind.RoutingKey,
			q.Bind.Exchange.Name,
			false, // no-wait
			nil,   // args
		); err != nil {
			return err
		}
	}
	return nil
}
