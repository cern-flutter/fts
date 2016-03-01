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

package x509

import (
	"crypto/x509"
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"gitlab.cern.ch/flutter/fts/errors"
	"gitlab.cern.ch/flutter/go-proxy"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"
)

type (
	// Proxy is the user's parsed proxy plus its delegation id
	Proxy struct {
		proxy.X509Proxy
		DelegationID string
	}

	// ProxyRequest is a parsed proxy request plus its delegation id
	ProxyRequest struct {
		proxy.X509ProxyRequest
		DelegationID string
	}

	serialProxy struct {
		DelegationID string    `json:"delegation_id" bson:"delegation_id"`
		NotAfter     time.Time `json:"not_after" bson:"not_after"`
		Pem          string    `json:"pem" bson:"pem"`
	}

	serialRequest struct {
		DelegationID string `json:"delegation_id" bson:"delegation_id"`
		Request      string `json:"request" bson:"request"`
		Key          string `json:"key" bson:"key"`
	}

	// Store stores X509 proxies and requests.
	Store interface {
		// Close connection to the underlying database.
		Close()
		// Get the X509 proxy.
		Get(delegationID string) (*Proxy, error)
		// Update, or insert, a new proxy. If the proxy already exists and is newer, nothing is done.
		Update(proxy *Proxy) error
		// Delete the existing proxy.
		Delete(delegationID string) error
		// List existing delegation ids.
		List() ([]string, error)

		// Get a proxy request
		GetRequest(delegationID string) (*ProxyRequest, error)
	}
)

// MarshalJSON serializes a proxy into a JSON message.
func (p *Proxy) MarshalJSON() ([]byte, error) {
	aux := serialProxy{
		DelegationID: p.DelegationID,
		NotAfter:     p.Certificate.NotAfter,
		Pem:          string(p.Encode()),
	}
	return json.Marshal(&aux)
}

// UnmarshalJSON deserializes a proxy from a JSON struct.
func (p *Proxy) UnmarshalJSON(data []byte) error {
	var aux serialProxy
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	if err := p.Decode([]byte(aux.Pem)); err != nil {
		return err
	}
	p.DelegationID = aux.DelegationID
	return nil
}

// MarshalJSON serializes a proxy request into a JSON message.
func (r *ProxyRequest) MarshalJSON() ([]byte, error) {
	aux := serialRequest{
		DelegationID: r.DelegationID,
		Request:      string(r.EncodeRequest()),
		Key:          string(r.EncodeKey()),
	}
	return json.Marshal(&aux)
}

// UnmarshalJSON deserializes a request from a JSON struct.
func (r *ProxyRequest) UnmarshalJSON(data []byte) error {
	var aux serialRequest
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	if err := r.Decode([]byte(aux.Request), []byte(aux.Key)); err != nil {
		return err
	}

	return nil
}

// GetBSON serializes a proxy into a BSON struct.
func (p *Proxy) GetBSON() (interface{}, error) {
	aux := serialProxy{
		DelegationID: p.DelegationID,
		NotAfter:     p.Certificate.NotAfter,
		Pem:          string(p.Encode()),
	}
	return aux, nil
}

// SetBSON deserializes a proxy from a BSON struct.
func (p *Proxy) SetBSON(raw bson.Raw) error {
	var aux serialProxy
	if err := raw.Unmarshal(&aux); err != nil {
		return err
	}

	if err := p.Decode([]byte(aux.Pem)); err != nil {
		return err
	}

	return nil
}

// GetBSON serializes a proxy request into a BSON message.
func (r *ProxyRequest) GetBSON() (interface{}, error) {
	aux := serialRequest{
		DelegationID: r.DelegationID,
		Request:      string(r.EncodeRequest()),
		Key:          string(r.EncodeKey()),
	}
	return aux, nil
}

// SetBSON deserializes a request from a BSON struct.
func (r *ProxyRequest) SetBSON(raw bson.Raw) error {
	var aux serialRequest
	if err := raw.Unmarshal(&aux); err != nil {
		return err
	}

	if err := r.Decode([]byte(aux.Request), []byte(aux.Key)); err != nil {
		return err
	}

	return nil
}

// Implementation
type storeImpl struct {
	session *mgo.Session
	db      *mgo.Database
}

// NewStore creates a new instance of a X509 store.
func NewStore(url string, db string) (Store, error) {
	session, err := mgo.Dial(url)
	if err != nil {
		return nil, err
	}
	return &storeImpl{session, session.DB(db)}, err
}

func (s *storeImpl) Close() {
	s.session.Close()
}

func (s *storeImpl) Get(delegationID string) (*Proxy, error) {
	var proxy Proxy
	if err := s.db.C("x509_proxies").Find(bson.M{"delegation_id": delegationID}).One(&proxy); err != nil {
		if err == mgo.ErrNotFound {
			err = errors.ErrNotFound
			return nil, err
		}
		return nil, err
	}
	return &proxy, nil
}

func (s *storeImpl) Update(proxy *Proxy) error {
	existing, err := s.Get(proxy.DelegationID)
	if err != nil && err != errors.ErrNotFound {
		return err
	} else if err != errors.ErrNotFound {
		if existing.Certificate.NotAfter.Sub(proxy.Certificate.NotAfter) > 0 {
			err = errors.ErrIgnored
			return err
		}
		s.Delete(proxy.DelegationID)
	}

	collection := s.db.C("x509_proxies")
	if _, err := collection.Upsert(bson.M{"delegation_id": proxy.DelegationID}, proxy); err != nil {
		return err
	}
	return nil
}

func (s *storeImpl) Delete(delegationID string) error {
	collection := s.db.C("x509_proxies")
	if err := collection.Remove(bson.M{"delegation_id": delegationID}); err != nil {
		if err == mgo.ErrNotFound {
			return errors.ErrNotFound
		}
		return err
	}
	return nil
}

func (s *storeImpl) List() ([]string, error) {
	var ids []string
	var proxy Proxy
	iter := s.db.C("x509_proxies").Find(nil).Iter()
	for iter.Next(&proxy) {
		ids = append(ids, proxy.DelegationID)
	}
	err := iter.Close()
	return ids, err
}

func (s *storeImpl) GetRequest(delegationID string) (req *ProxyRequest, err error) {
	var existingReq ProxyRequest

	// Try existing one first
	collection := s.db.C("x509_proxy_request")
	if err = collection.Find(bson.M{"delegation_id": delegationID}).One(&existingReq); err == nil {
		req = &existingReq
		log.WithField("delegation_id", delegationID).Debug("Using existing proxy request")
		return
	} else if err != mgo.ErrNotFound {
		return
	}

	// Otherwise, generate new one and store
	req = &ProxyRequest{
		DelegationID: delegationID,
	}
	if err = req.Init(2048, x509.SHA256WithRSA); err != nil {
		return
	}
	log.WithField("delegation_id", delegationID).Debug("Generated new proxy request")
	_, err = collection.Upsert(bson.M{"delegation_id": delegationID}, req)
	return
}

