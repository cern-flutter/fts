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

package surl

import (
	"encoding/json"
	"net"
	"net/url"
)

// SURL extends url.URL with some additional methods
type SURL struct {
	url.URL
}

// Parse a raw string containing an URL
func Parse(rawurl string) (surl *SURL, err error) {
	parsed, err := url.Parse(rawurl)
	if err != nil {
		return
	}
	surl = &SURL{*parsed}
	return
}

// GetStorageName returns the storage name, which is scheme://host (without the port)
func (s *SURL) GetStorageName() string {
	if host, _, err := net.SplitHostPort(s.Host); err == nil {
		return s.Scheme + "://" + host
	}
	return s.Scheme + "://" + s.Host
}

// GetHostName returns only the hostname, without the port
func (s *SURL) GetHostName() string {
	if host, _, err := net.SplitHostPort(s.Host); err == nil {
		return host
	}
	return s.Host
}

// Empty returns true if the url is empty
func (s *SURL) Empty() bool {
	return s.String() == ""
}

// MarshalJSON marshals the SURL struct to a simple string (rather than a set of fields)
func (s *SURL) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// UnmarshalJSON converts a string (defined as SURL in the type struct) to a SURL
func (s *SURL) UnmarshalJSON(data []byte) (err error) {
	var str string
	if err = json.Unmarshal(data, &str); err != nil {
		return
	}
	parsed, err := url.Parse(str)
	if err != nil {
		return
	}
	s.URL = *parsed
	return
}

// UnmarshalBinary unserializes the URL into a binary string
func (s *SURL) UnmarshalBinary(data []byte) error {
	str := string(data)
	parsed, err := url.Parse(str)
	if err != nil {
		return err
	}
	s.URL = *parsed
	return nil
}

// MarshalBinary serializes the URL into a binary string
func (s *SURL) MarshalBinary() ([]byte, error) {
	return []byte(s.String()), nil
}
