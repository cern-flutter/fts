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
	"database/sql"
	log "github.com/Sirupsen/logrus"
	_ "github.com/lib/pq"
	"gitlab.cern.ch/flutter/stomp"
	"net/url"
)

type (
	// Params defines the configuration for the worker
	Params struct {
		StompParams     stomp.ConnectionParameters
		URLCopyBin      string
		TransferLogPath string
		DirQPath        string
		Database        string
		PidDBPath       string
	}

	// Worker is used by each subsystem
	Worker struct {
		params     Params
		supervisor *Supervisor
		db         *sql.DB
	}

	// Reply coming from the credential service
	pingReply struct {
		Version string
		Echo    string
	}
)

// Connects to a remote database from a url-like connection string
func connectDatabase(dbUrl string) (*sql.DB, error) {
	parsed, err := url.Parse(dbUrl)
	if err != nil {
		return nil, err
	}
	return sql.Open(parsed.Scheme, parsed.String())
}

// New creates a new Worker Context
func NewWorker(params Params) (w *Worker, err error) {
	w = &Worker{
		params: params,
	}

	if w.supervisor, err = NewSupervisor(params.PidDBPath); err != nil {
		return nil, err
	}
	log.Debugf("Started supervisor with DB %s", params.PidDBPath)

	if w.db, err = connectDatabase(params.Database); err != nil {
		return
	}
	if err = w.db.Ping(); err != nil {
		return
	}

	log.Debugf("Connected to the database")
	return
}

// Close finalizes all the connections and processes
func (c *Worker) Close() {
	c.db.Close()
	c.supervisor.Close()
}

// Run sub-services, and return a channel where errors are written
func (c *Worker) Run() error {
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

	return <-errors
}
