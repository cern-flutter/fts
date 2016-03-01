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
	"github.com/streadway/amqp"
	"gitlab.cern.ch/flutter/fts/bus/exchanges"
	"gitlab.cern.ch/flutter/fts/types/tasks"
	"gitlab.cern.ch/flutter/fts/version"
	"net/http"
	"encoding/json"
)

type GatewayRPC struct {
	amqpConn  *amqp.Connection
	amqpChann *amqp.Channel
}

type PingReply struct {
	Version string
	Echo    string
}

func newRPC(amqpAddr string) (*GatewayRPC, error) {
	var err error
	rpc := &GatewayRPC{}

	if rpc.amqpConn, err = amqp.Dial(amqpAddr); err != nil {
		return nil, err
	}
	if rpc.amqpChann, err = rpc.amqpConn.Channel(); err != nil {
		return nil, err
	}
	if err = exchanges.Transition.Declare(rpc.amqpChann); err != nil {
		return nil, err
	}

	return rpc, nil
}

// Ping method
func (c *GatewayRPC) Ping(r *http.Request, args *string, reply *PingReply) error {
	log.WithFields(log.Fields{
		"echo":   *args,
		"remote": r.RemoteAddr,
	}).Debug("Ping")
	reply.Echo = *args
	reply.Version = version.Version
	return nil
}

// Submit adds a new set of transfers to the system.
// nBatches is the total number of subsets the original task has been split into.
func (c *GatewayRPC) Submit(r *http.Request, set *tasks.Batch, nBatches *int) error {
	l := log.WithField("delegation_id", set.DelegationID)
	normalized := set.Split()
	*nBatches = len(normalized)
	l = l.WithField("count", *nBatches)
	l.Info("Accepted submission with ", *nBatches, " batches")

	for _, batch := range normalized {
		data, err := json.Marshal(batch)
		if err != nil {
			return err
		}
		err = c.amqpChann.Publish(
			exchanges.Transition.Name,
			tasks.Submitted,
			false, // mandatory
			false, // immediate
			 amqp.Publishing{
				 ContentType: "application/json",
				 DeliveryMode: amqp.Persistent,
				 Priority: 0,
				 Body: data,
			 },
		)
		if err != nil {
			return err
		}
		log.Info("Submitted batch ", batch.GetID())
	}

	return nil
}
