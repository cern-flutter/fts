#
# Copyright (c) CERN 2016
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# FTS image required to be able to build sources

FROM docker.cern.ch/dmc-dev/gfal2

RUN yum install -y gcc gfal2-devel gfal2-plugin-mock
RUN yum install -y git make

# Go
ENV GOLANG_VERSION 1.6.2
ENV GOLANG_URL https://golang.org/dl/go$GOLANG_VERSION.linux-amd64.tar.gz

ADD $GOLANG_URL "/"
RUN tar -C "/usr/local" -xzf "go$GOLANG_VERSION.linux-amd64.tar.gz"
RUN rm -f "go$GOLANG_VERSION.linux-amd64.tar.gz"

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

RUN mkdir -p "$GOPATH/src" "$GOPATH/bin"
RUN mkdir -p "$GOPATH/src/gitlab.cern.ch/flutter/fts"

# Mount points
VOLUME ["$GOPATH/src/gitlab.cern.ch/fts"]
