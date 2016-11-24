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
	"github.com/garyburd/redigo/redis"
	"gitlab.cern.ch/flutter/fts/messages"
	"strings"
)

const (
	// DefaultSlots is the number of parallel transfers by default
	DefaultSlots = 2
	// KeySeparator is used to join scoreboard keys together
	KeySeparator = "#"

	fieldCounter = "counter"
	fieldMax     = "max"
)

type (
	// Scoreboard implements accounting on the number of transfer running
	// for a given source/destination/pair
	Scoreboard struct {
		pool *redis.Pool
	}

	//ts.DestSe, ts.Vo, ts.Activity, ts.SourceSe
)

// GetWeight returns the weight of the given route
func (info *Scoreboard) GetWeight(route []string) float32 {
	// TODO: Access config
	return 1.0
}

func availableSlots(conn redis.Conn, keys ...string) (bool, error) {
	key := strings.Join(keys, KeySeparator)
	l := log.WithField("key", key)

	values, err := redis.Values(conn.Do("HMGET", key, fieldCounter, fieldMax))
	if err != nil {
		return false, err
	}

	var count, max int
	if _, err := redis.Scan(values, &count, &max); err != nil {
		return false, err
	}

	if max == 0 {
		l.Debug("No entry, assuming there are available slots")
		_, err := conn.Do("HSET", key, fieldMax, DefaultSlots)
		return true, err
	}
	l.WithFields(log.Fields{"slots": max, "count": count}).Debug("Available slots")
	return count < max, nil
}

// IsThereAvailableSlots returns true if there can be a new transfer for the given route
func (info *Scoreboard) IsThereAvailableSlots(route []string) (bool, error) {
	conn := info.pool.Get()
	defer conn.Close()

	switch len(route) {
	// Root node, overall FTS, so there are slots
	case 0:
		return true, nil
	// Destination storage
	case 1:
		return availableSlots(conn, route[0])
	// Destination/Vo, we do not have slots per vo, so always available
	case 2:
		return true, nil
	// Destination/Vo/Activity, still no cap per activity
	case 3:
		return true, nil
	// Destination/Vo/Activity/Source, we get two caps: link and source
	case 4:
		if forSource, err := availableSlots(conn, route[3]); err != nil {
			return false, err
		} else if forLink, err := availableSlots(conn, route[3], route[0]); err != nil {
			return false, err
		} else {
			return forSource && forLink, nil
		}
	}
	return true, nil
}

func increaseActiveCount(conn redis.Conn, keys ...string) error {
	key := strings.Join(keys, KeySeparator)
	l := log.WithField("key", key)

	newCount, err := redis.Int(conn.Do("HINCRBY", key, fieldCounter, 1))
	if err != nil {
		return err
	}

	l.WithField("count", newCount).Debug("Increment active count")
	return nil
}

// ConsumeSlot reduces by one the number of available slots for the source, destination,
// and link.
func (info *Scoreboard) ConsumeSlot(batch *messages.Batch) error {
	conn := info.pool.Get()
	defer conn.Close()
	if err := increaseActiveCount(conn, batch.SourceSe); err != nil {
		return err
	}
	if err := increaseActiveCount(conn, batch.DestSe); err != nil {
		return err
	}
	if err := increaseActiveCount(conn, batch.SourceSe, batch.DestSe); err != nil {
		return err
	}
	return nil
}

func decreaseActiveCount(conn redis.Conn, keys ...string) error {
	key := strings.Join(keys, KeySeparator)
	l := log.WithField("key", key)

	newCount, err := redis.Int(conn.Do("HINCRBY", key, fieldCounter, -1))
	if err != nil {
		return err
	}
	if newCount < 0 {
		l.Warn("New active counter below 0, reset value")
		newCount = 0
		conn.Do("HSET", key, fieldCounter, newCount)
	}

	l.WithField("count", newCount).Debug("Decrement active count")
	return nil
}

// ReleaseSlot increases by one the number of available slots for the source, destination,
// and link.
func (info *Scoreboard) ReleaseSlot(batch *messages.Batch) error {
	conn := info.pool.Get()
	defer conn.Close()
	if err := decreaseActiveCount(conn, batch.SourceSe); err != nil {
		return err
	}
	if err := decreaseActiveCount(conn, batch.DestSe); err != nil {
		return err
	}
	if err := decreaseActiveCount(conn, batch.SourceSe, batch.DestSe); err != nil {
		return err
	}
	return nil
}
