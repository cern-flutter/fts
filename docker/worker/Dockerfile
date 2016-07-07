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

FROM flutter-dev/base

# gfal2 
RUN yum install -y gfal2-plugin-srm gfal2-plugin-gridftp gfal2-plugin-http gfal2-plugin-xrootd gfal2-plugin-mock

# Copy required binaries
ADD "build/fts-x509d" "build/url-copy" "build/fts-workerd" "/usr/bin/"

# Supervisor configuration
COPY "docker/worker/supervisord.conf" "/etc/supervisor/conf.d/supervisord.conf"

# Start supervisor
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
