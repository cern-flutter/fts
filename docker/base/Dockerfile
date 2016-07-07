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

# FTS base container

FROM centos:7

# Required repositories
RUN yum install -y epel-release
ADD "https://grid-deployment.web.cern.ch/grid-deployment/dms/fts3/repos/fts3-continuous-el7.repo" "/etc/yum.repos.d/"
ADD "https://grid-deployment.web.cern.ch/grid-deployment/dms/dmc/repos/dmc-ci-el7.repo" "/etc/yum.repos.d/"
ADD "http://repository.egi.eu/sw/production/cas/1/current/repo-files/EGI-trustanchors.repo" "/etc/yum.repos.d/"
RUN yum clean all && yum update -y
RUN yum install -y supervisor

# FTS user and group
RUN groupadd --system --gid 497 "fts"
RUN useradd --system --gid "fts" --uid 497 "fts"

# VO and Root CA
RUN yum install -y lcg-CA voms-config-all

# Log directories
RUN mkdir -p "/var/log/fts"
RUN mkdir -p "/var/lib/fts"

RUN chown fts.fts "/var/log/fts"
RUN chown fts.fts "/var/lib/fts"
RUN chmod 0775 "/var/log/fts"
RUN chmod 0775 "/var/lib/fts"

# Go
ADD "https://storage.googleapis.com/golang/go1.6.2.linux-amd64.tar.gz" "/"
RUN tar xzf "go1.6.2.linux-amd64.tar.gz"
RUN rm -f "go1.6.2.linux-amd64.tar.gz"
ADD "docker/base/go.sh" "/etc/profile.d/"

# gfal2
RUN yum install -y gfal2-plugin-srm gfal2-plugin-gridftp gfal2-plugin-http gfal2-plugin-xrootd gfal2-plugin-mock

