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
	"database/sql"
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	_ "github.com/lib/pq"
	"gitlab.cern.ch/flutter/fts/errors"
	"gitlab.cern.ch/flutter/go-proxy"
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
		DelegationID string    `json:"delegation_id"`
		NotAfter     time.Time `json:"not_after"`
		Pem          string    `json:"pem"`
	}

	serialRequest struct {
		DelegationID string `json:"delegation_id"`
		Request      string `json:"request"`
		Key          string `json:"key"`
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

// Implementation
type storeImpl struct {
	db *sql.DB
}

// NewStore creates a new instance of a X509 store.
func NewStore(addr string) (Store, error) {
	db, err := sql.Open("postgres", addr)
	if err != nil {
		return nil, err
	}
	return &storeImpl{db}, nil
}

func (s *storeImpl) Close() {
	s.db.Close()
}

func (s *storeImpl) Get(delegationID string) (*Proxy, error) {
	var proxy Proxy

	rows, err := s.db.Query("SELECT pem FROM t_x509_proxies WHERE delegation_id = $1", delegationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, errors.ErrNotFound
	}

	var pem string
	if err = rows.Scan(&pem); err != nil {
		return nil, err
	}
	if err = proxy.Decode([]byte(pem)); err != nil {
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

	_, err = s.db.Exec(
		`INSERT INTO t_x509_proxies
			(delegation_id, user_dn, not_after, pem) VALUES
			($1, $2, $3, $4)
		`, proxy.DelegationID, proxy.Subject, proxy.Certificate.NotAfter, proxy.Encode())
	return err
}

func (s *storeImpl) Delete(delegationID string) error {
	_, err := s.db.Exec(`DELETE FROM t_x509_proxies WHERE delegation_id = $1`, delegationID)
	return err
}

func (s *storeImpl) List() ([]string, error) {
	var ids []string
	rows, err := s.db.Query("SELECT delegation_id FROM t_x509_proxies")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			ids = append(ids, id)
		}
	}

	return ids, err
}

func (s *storeImpl) GetRequest(delegationID string) (req *ProxyRequest, err error) {
	// Try existing one first
	rows, err := s.db.Query("SELECT request, private_key FROM t_x509_requests WHERE delegation_id = $1", delegationID)
	if err != nil {
		return nil, err
	}

	if rows.Next() {
		var pemReq, pemKey string
		if err = rows.Scan(&pemReq, &pemKey); err != nil {
			return nil, err
		}
		var req ProxyRequest
		if err = req.Decode([]byte(pemReq), []byte(pemKey)); err != nil {
			return nil, err

		}
		log.WithField("delegation_id", delegationID).Debug("Using existing proxy request")
		return &req, nil
	}

	// Otherwise, generate new one and store
	req = &ProxyRequest{
		DelegationID: delegationID,
	}
	if err = req.Init(2048, x509.SHA256WithRSA); err != nil {
		return nil, err
	}
	log.WithField("delegation_id", delegationID).Debug("Generated new proxy request")

	// Insert
	_, err = s.db.Exec(
		`INSERT INTO t_x509_requests
			(delegation_id, request, private_key) VALUES
			($1, $2, $3)`,
		req.DelegationID, req.EncodeRequest(), req.EncodeKey())
	return req, err
}
