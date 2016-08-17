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
	"gitlab.cern.ch/flutter/echelon"
	"gitlab.cern.ch/flutter/fts/types/tasks"
	"gitlab.cern.ch/flutter/stomp"
)

type (
	// Scheduler data
	Scheduler struct {
		producer *stomp.Producer
		consumer *stomp.Consumer

		echelon *echelon.Echelon
	}
)

// New creates a new scheduler
func New(params stomp.ConnectionParameters, echelonDir string) (*Scheduler, error) {
	var err error
	sched := &Scheduler{}

	if sched.producer, err = stomp.NewProducer(params); err != nil {
		return nil, err
	}
	if sched.consumer, err = stomp.NewConsumer(params); err != nil {
		return nil, err
	}
	db, err := echelon.NewLevelDb(echelonDir)
	if err != nil {
		return nil, err
	}
	if sched.echelon, err = echelon.New(&tasks.Batch{}, db, &SchedInfoProvider{}); err != nil {
		return nil, err
	}
	if err = sched.echelon.Restore(); err != nil {
		return nil, err
	}
	return sched, nil
}

// Close finishes the scheduler
func (s *Scheduler) Close() {
	s.consumer.Close()
	s.producer.Close()
	s.echelon.Close()
}
