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
	log "github.com/Sirupsen/logrus"
	"github.com/garyburd/redigo/redis"
	"gitlab.cern.ch/flutter/echelon"
	"gitlab.cern.ch/flutter/fts/types/tasks"
	"gitlab.cern.ch/flutter/stomp"
	"time"
)

type (
	// Scheduler data
	Scheduler struct {
		producer *stomp.Producer
		consumer *stomp.Consumer

		echelon *echelon.Echelon
		pool    *redis.Pool
	}
)

// New creates a new scheduler
func New(params stomp.ConnectionParameters, redisAddr string) (*Scheduler, error) {
	var err error
	sched := &Scheduler{}

	if sched.producer, err = stomp.NewProducer(params); err != nil {
		return nil, err
	}
	if sched.consumer, err = stomp.NewConsumer(params); err != nil {
		return nil, err
	}
	echelonRedis := &echelon.RedisDb{
		Pool: &redis.Pool{
			Dial: func() (redis.Conn, error) {
				log.Debug("Dial Redis connection")
				return redis.Dial("tcp", redisAddr)
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
			MaxIdle:     10,
			MaxActive:   50,
			IdleTimeout: 60 * time.Second,
			Wait:        true,
		},
		Prefix: "fts-sched-",
	}

	if sched.echelon, err = echelon.New(&tasks.Batch{}, echelonRedis, &SchedInfoProvider{}); err != nil {
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
