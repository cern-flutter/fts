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

# FTS REST container

FROM docker.cern.ch/flutter-dev/base

# Copy required binaries
ADD "build/fts-gated" "build/fts-x509d" "/usr/bin/"

# Install dependencies
RUN yum install -y httpd gridsite mod_wsgi python-pip
RUN pip install --upgrade pip
RUN yum install -y gcc openssl-devel python-devel swig

# Add apache user to fts group
RUN usermod -a -G fts apache

# Supervisor configuration
COPY "docker/rest/supervisord.conf" "/etc/supervisor/conf.d/supervisord.conf"

# Prune default httpd configuration
RUN rm -rf /etc/httpd/conf.d/*

# Install REST
COPY "rest" "/tmp/rest/"
RUN pip install "/tmp/rest"
RUN pip install "/tmp/rest/src/fts3rest"
COPY "rest/src/fts3rest/fts3rest.wsgi" "/usr/libexec/fts3rest.wsgi"

# Install REST configuration
COPY "docker/rest/fts3rest.conf" "/etc/httpd/conf.d/"
COPY "docker/rest/fts3rest.ini" "/etc/fts3/"

# Cleanup
RUN rm -rfv "/tmp/rest"

# Expose REST port
EXPOSE 8446

# Start supervisor
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
