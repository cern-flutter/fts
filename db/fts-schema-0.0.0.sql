--
-- Copyright (c) CERN 2016
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

--
-- X509 credentials
--

CREATE TABLE t_x509_requests (
    delegation_id CHAR(40) NOT NULL PRIMARY KEY,
    request       TEXT NOT NULL,
    private_key   TEXT NOT NULL
);

CREATE TABLE t_x509_proxies (
    delegation_id CHAR(40) NOT NULL PRIMARY KEY,
    user_dn       VARCHAR(255) NOT NULL,
    not_after     TIMESTAMP WITHOUT TIME ZONE,
    pem           TEXT NOT NULL
);

--
-- Database schema versioning
--
CREATE TABLE t_schema_version (
    major INT NOT NULL,
    minor INT NOT NULL,
    patch INT NOT NULL,
    message TEXT,
    PRIMARY KEY (major, minor, patch)
);
INSERT INTO t_schema_version (major, minor, patch, message) VALUES
    (0, 0, 0, 'Experimental');
