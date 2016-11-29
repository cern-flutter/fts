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

# FTS Woker container

FROM gitlab-registry.cern.ch/dmc/gfal2

# supervisord
RUN yum install -y supervisor
COPY "supervisord.conf" "/etc/supervisor/conf.d/supervisord.conf"
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]

# Dirq dir
RUN mkdir "/var/lib/fts" && chown nobody.nobody "/var/lib/fts"

# Copy required binaries
ENV PATH="${PATH}:/app/"
COPY "workerd/workerd" "url-copy/url-copy" "/app/"
