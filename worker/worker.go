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
	"github.com/streadway/amqp"
	"gitlab.cern.ch/flutter/http-jsonrpc"
	"net/rpc"
)

type (
	// Params defines the configuration for the worker
	Params struct {
		AmqpAddress     string
		URLCopyBin      string
		TransferLogPath string
		DirQPath        string
		X509Address     string
	}

	// Context is used by each subsystem
	Context struct {
		params         Params
		amqpConnection *amqp.Connection
		x509d          *rpc.Client
	}

	// Reply coming from the credential service
	pingReply struct {
		Version string
		Echo    string
	}
)

// New creates a new Worker Context
func New(params Params) (context *Context, err error) {
	context = &Context{
		params: params,
	}

	codec, err := http_jsonrpc.NewClientCodec(params.X509Address)
	if err != nil {
		return
	}
	context.x509d = rpc.NewClientWithCodec(codec)

	var x509Reply pingReply
	if err = context.x509d.Call("X509.Ping", "Echo", &x509Reply); err != nil {
		return
	}

	if context.amqpConnection, err = amqp.Dial(params.AmqpAddress); err != nil {
		context.x509d.Close()
		return
	}

	// Called if the connection is lost
	go func(c chan *amqp.Error) {
		for e := range c {
			log.Panic("Lost connection with AMQP: ", e)
		}
	}(context.amqpConnection.NotifyClose(make(chan *amqp.Error)))

	log.Debugf("Connected to X509 %s (%s)", params.X509Address, x509Reply.Version)
	log.Debugf("Connected to AMQP %s (%d.%d)", params.AmqpAddress, context.amqpConnection.Major, context.amqpConnection.Minor)

	return
}

// Close finalizes all the connections and processes
func (c *Context) Close() {
	c.amqpConnection.Close()
	c.x509d.Close()
}
