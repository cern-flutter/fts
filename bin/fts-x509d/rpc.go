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
	json "github.com/gorilla/rpc/v2/json2"
	"gitlab.cern.ch/flutter/fts/credentials/x509"
	"gitlab.cern.ch/flutter/fts/errors"
	"gitlab.cern.ch/flutter/fts/version"
	"net/http"
)

// Interface
type X509RPC struct {
	store x509.Store
}

type PingReply struct {
	Version string
	Echo    string
}

// Ping method
func (c *X509RPC) Ping(r *http.Request, args *string, reply *PingReply) error {
	log.WithFields(log.Fields{
		"echo":   *args,
		"remote": r.RemoteAddr,
	}).Debug("Ping")
	reply.Echo = *args
	reply.Version = version.Version
	return nil
}

// Get the proxy stored associated with the given delegation id
func (c *X509RPC) Get(r *http.Request, delegationID *string, proxy *x509.Proxy) error {
	var err error
	var stored *x509.Proxy

	l := log.WithField("delegation_id", *delegationID)

	if stored, err = c.store.Get(*delegationID); err != nil {
		l.WithError(err).Error("Could not retrieve the proxy")
	} else {
		l.Info("Get proxy")
		*proxy = *stored
	}
	if de, ok := err.(errors.Error); ok {
		return &json.Error{Code: json.ErrorCode(de.Code), Message: de.Message}
	}
	return err
}

// Update the proxy associated with the given delegation id.
// An existing stored request is required, since the pem string is not assumed to have a private key.
func (c *X509RPC) Update(r *http.Request, signed *x509.Proxy, delegationID *string) error {
	var err error
	*delegationID = signed.DelegationID

	l := log.WithField("delegation_id", *delegationID)

	// Need the request
	var req *x509.ProxyRequest
	if req, err = c.store.GetRequest(signed.DelegationID); err != nil {
		l.WithError(err).Error("Update proxy")
		return err
	}

	// Verify the new proxy corresponds to the stored request
	if !req.Matches(&signed.X509Proxy) {
		err = &json.Error{
			Code:    json.ErrorCode(errors.ErrInvalid.Code),
			Message: "The proxy does not match the stored key",
		}
		l.WithError(err).Error("Update proxy")
		return err
	}

	// Build full proxy, including private key
	signed.Key = req.Key

	// Store it
	if err = c.store.Update(signed); err != nil {
		l.WithError(err).Error("Update proxy")
	} else {
		l.Info("Update proxy")
	}
	return err
}

// Store the proxy as is, associated to the given delegation id.
func (c *X509RPC) Put(r *http.Request, proxy *x509.Proxy, delegationID *string) error {
	*delegationID = proxy.DelegationID

	l := log.WithField("delegation_id", *delegationID)

	if err := c.store.Update(proxy); err != nil {
		l.WithError(err).Error("Put proxy")
		return err
	}
	l.Info("Put proxy")
	return nil
}

// Delete the delegated proxy
func (c *X509RPC) Delete(r *http.Request, delegationID *string, out *string) (err error) {
	*out = *delegationID
	l := log.WithField("delegation_id", *delegationID)
	if err = c.store.Delete(*delegationID); err != nil {
		l.WithError(err).Error("Delete proxy")
	} else {
		l.Info("Delete proxy")
	}
	return
}

// Get a request associated with the given delegation id
func (c *X509RPC) GetRequest(r *http.Request, delegationID *string, out *x509.ProxyRequest) (err error) {
	l := log.WithField("delegation_id", *delegationID)
	var req *x509.ProxyRequest
	if req, err = c.store.GetRequest(*delegationID); err == nil {
		*out = *req
		out.Key = nil // Do not send the private key
		l.Info("Get proxy request")
	} else {
		l.WithError(err).Error("Get proxy request")
	}
	return
}
