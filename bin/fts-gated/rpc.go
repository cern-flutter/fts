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
	"gitlab.cern.ch/flutter/fts/config"
	"gitlab.cern.ch/flutter/fts/types/tasks"
	"gitlab.cern.ch/flutter/fts/version"
	"gitlab.cern.ch/flutter/stomp"
	"net/http"
)

type GatewayRPC struct {
	producer *stomp.Producer
}

type PingReply struct {
	Version string
	Echo    string
}

// newRPC creates a new RPC instance
func newRPC(params stomp.ConnectionParameters) (*GatewayRPC, error) {
	var err error
	rpc := &GatewayRPC{}

	if rpc.producer, err = stomp.NewProducer(params); err != nil {
		return nil, err
	}
	return rpc, nil
}

// close closes remote connections
func (c *GatewayRPC) close() {
	c.producer.Close()
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
	normalized := set.Normalize()
	*nBatches = len(normalized)
	l = l.WithField("count", *nBatches)
	l.Info("Accepted submission with ", *nBatches, " batches")

	for _, batch := range normalized {
		batch.State = tasks.Submitted
		data, err := json.Marshal(batch)
		if err != nil {
			return err
		}
		err = c.producer.Send(
			config.TransferTopic,
			string(data),
			stomp.SendParams{
				Persistent:  true,
				ContentType: "application/json",
			},
		)
		if err != nil {
			return err
		}
		log.Infof("Submitted batch %s => %s (%s)", batch.SourceSe, batch.DestSe, batch.Activity)
	}

	return nil
}
