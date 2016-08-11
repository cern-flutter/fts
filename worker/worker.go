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
	log "github.com/Sirupsen/logrus"
	"gitlab.cern.ch/flutter/http-jsonrpc"
	"gitlab.cern.ch/flutter/stomp"
	"net/rpc"
)

type (
	// Params defines the configuration for the worker
	Params struct {
		StompParams     stomp.ConnectionParameters
		URLCopyBin      string
		TransferLogPath string
		DirQPath        string
		X509Address     string
		PidDBPath       string
	}

	// Worker is used by each subsystem
	Worker struct {
		params     Params
		x509d      *rpc.Client
		supervisor *Supervisor
	}

	// Reply coming from the credential service
	pingReply struct {
		Version string
		Echo    string
	}
)

// New creates a new Worker Context
func New(params Params) (w *Worker, err error) {
	w = &Worker{
		params: params,
	}

	if w.supervisor, err = NewSupervisor(params.PidDBPath); err != nil {
		return nil, err
	}
	log.Debugf("Started supervisor with DB %s", params.PidDBPath)

	codec, err := http_jsonrpc.NewClientCodec(params.X509Address)
	if err != nil {
		return
	}
	w.x509d = rpc.NewClientWithCodec(codec)

	var x509Reply pingReply
	if err = w.x509d.Call("X509.Ping", "Echo", &x509Reply); err != nil {
		return
	}

	log.Debugf("Connected to X509 %s (%s)", params.X509Address, x509Reply.Version)
	return
}

// Close finalizes all the connections and processes
func (c *Worker) Close() {
	c.x509d.Close()
	c.supervisor.Close()
}

// Run sub-services, and return a channel where errors are written
func (c *Worker) Run() <-chan error {
	errors := make(chan error, 10)

	go func() {
		errors <- (&Runner{Context: c}).Run()
	}()
	go func() {
		errors <- (&Killer{Context: c}).Run()
	}()
	go func() {
		errors <- (&Forwarder{Context: c}).Run()
	}()
	go func() {
		c.supervisor.Run()
	}()

	return errors
}
