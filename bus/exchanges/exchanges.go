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

package exchanges

import "github.com/streadway/amqp"

// Exchange defines an exchange on the message broker
type Exchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
}

var (
	// Transition is the exchange point for state transition messages
	// FTS workflow is mainly based on messages going through this exchange!
	Transition = &Exchange{
		Name:       "fts.e.transition",
		Type:       "topic",
		Durable:    true,
		AutoDelete: false,
	}

	// Performance is the exchange point for performance markers
	Performance = &Exchange{
		Name:       "fts.e.perf",
		Type:       "fanout",
		Durable:    true,
		AutoDelete: false,
	}
)

// Declare a new exchange
func (e *Exchange) Declare(channel *amqp.Channel) error {
	return channel.ExchangeDeclare(
		e.Name, e.Type, e.Durable, e.AutoDelete,
		false, // internal
		false, // no-wait,
		nil,   // arguments
	)
}
