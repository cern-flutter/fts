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

package messages

import (
	"github.com/golang/protobuf/ptypes/timestamp"
	"time"
)

// Elapsed return the interval between the start and the end
// If end is Nil, then returns the different between start and now
func (m *Interval) Elapsed() time.Duration {
	if m.Start == nil {
		return 0 * time.Second
	}

	var start, end time.Time
	start = time.Unix(m.Start.Seconds, int64(m.Start.Nanos))
	if m.End == nil {
		end = time.Now()
	} else {
		end = time.Unix(m.End.Seconds, int64(m.End.Nanos))
	}

	return end.Sub(start)
}

// SetStart sets the start time from a Go time struct
func (m *Interval) SetStart(t time.Time) {
	utc := t.UTC()
	m.Start = &timestamp.Timestamp{
		Seconds: int64(utc.Second()),
		Nanos:   int32(utc.Nanosecond()),
	}
}

// SetEnd sets the end time from a Go time struct
func (m *Interval) SetEnd(t time.Time) {
	utc := t.UTC()
	m.End = &timestamp.Timestamp{
		Seconds: int64(utc.Second()),
		Nanos:   int32(utc.Nanosecond()),
	}
}

// Now returns an UTC Google Protobuf timestamp with the current time
func Now() *timestamp.Timestamp {
	utc := time.Now().UTC()
	return &timestamp.Timestamp{
		Seconds: int64(utc.Second()),
		Nanos:   int32(utc.Nanosecond()),
	}
}
