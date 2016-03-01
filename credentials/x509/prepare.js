//
//  Copyright (c) CERN 2016
// 
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//


db.createCollection("x509_proxies", {})
db.createCollection("x509_proxy_requests", {})

// Create index on delegation_id
db.x509_proxies.createIndex(
    {
        delegation_id: 1,
    },
    {
        background: false,
        unique: true,
    }
)
db.x509_proxy_requests.createIndex(
    {
        delegation_id: 1,
    },
    {
        background: false,
        unique: true,
    }
)

// Create index on not_after, so entries get auto-purged when expired
db.x509_proxies.createIndex(
    {
        not_after: 1,
    },
    {
        background: false,
        unique: false,
        expireAfterSeconds: 600
    }
)
