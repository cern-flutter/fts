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
	"testing"
)

func TestMock(t *testing.T) {
	original := "mock://host.domain:1243/path?size_post=10&time=1"

	var err error
	var surl *SURL
	if surl, err = Parse(original); err != nil {
		t.Fatal(err)
	}

	if surl.Scheme != "mock" {
		t.Fatal("Unexpected scheme")
	}
	if surl.Host != "host.domain:1243" {
		t.Fatal("Unexpected host: ", surl.Host)
	}
	if surl.GetHostName() != "host.domain" {
		t.Fatal("Unexpected hostname: ", surl.GetHostName())
	}
	if surl.GetStorageName() != "mock://host.domain" {
		t.Fatal("Unexpected storage name: ", surl.GetStorageName())
	}
	if surl.Path != "/path" {
		t.Fatal("Unexpected path: ", surl.Path)
	}
}

func TestMock2(t *testing.T) {
	original := "mock://host/path?size_post=10&time=1"

	var err error
	var surl *SURL
	if surl, err = Parse(original); err != nil {
		t.Fatal(err)
	}

	if surl.Scheme != "mock" {
		t.Fatal("Unexpected scheme")
	}
	if surl.Host != "host" {
		t.Fatal("Unexpected host: ", surl.Host)
	}
	if surl.GetHostName() != "host" {
		t.Fatal("Unexpected hostname: ", surl.GetHostName())
	}
	if surl.GetStorageName() != "mock://host" {
		t.Fatal("Unexpected storage name: ", surl.GetStorageName())
	}
	if surl.Path != "/path" {
		t.Fatal("Unexpected path: ", surl.Path)
	}
}

func TestSerializeJson(t *testing.T) {
	original := "http://subdomain.domain.com:8080/path?query=args"

	var err error
	var surl *SURL

	if surl, err = Parse(original); err != nil {
		t.Fatal(err)
	}
	var serialized []byte
	if serialized, err = json.Marshal(surl); err != nil {
		t.Fatal(err)
	}

	if string(serialized) != ("\"" + original + "\"") {
		t.Fatal("Unexpected serialization", string(serialized))
	}
}

func TestDeserializeJson(t *testing.T) {
	original := []byte("\"http://subdomain.domain.com:8080/path?query=args\"")

	var surl SURL
	err := json.Unmarshal(original, &surl)
	if err != nil {
		t.Fatal(err)
	}

	if surl.Scheme != "http" || surl.Host != "subdomain.domain.com:8080" ||
		surl.GetHostName() != "subdomain.domain.com" ||
		surl.GetStorageName() != "http://subdomain.domain.com" {
		t.Fatal("Deserialization failed", surl)
	}
}

