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

package sink

import (
	log "github.com/Sirupsen/logrus"
	"gitlab.cern.ch/flutter/stomp"
)

// Purge consumes all messages from the given queue, and just dumps them
func Purge(params stomp.ConnectionParameters, destination, id string) error {
	consumer, err := stomp.NewConsumer(params)
	if err != nil {
		return err
	}
	msgs, errors, err := consumer.Subscribe(destination, id, stomp.AckAuto)
	if err != nil {
		return err
	}

	log.Info("Subcribed to ", destination)

	for {
		select {
		case m := <-msgs:
			log.Info(m.Headers)
			log.Info(string(m.Body))
		case e := <-errors:
			log.Warn(e)
		}
	}
}
