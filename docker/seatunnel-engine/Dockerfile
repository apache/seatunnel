#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

ARG BASE_IMAGE='eclipse-temurin:8-jre'

FROM $BASE_IMAGE

ENV SEATUNNEL_HOME /opt/seatunnel

WORKDIR $SEATUNNEL_HOME

COPY . $SEATUNNEL_HOME

EXPOSE 5801

CMD [ "/bin/bash", "-c", "$SEATUNNEL_HOME/bin/seatunnel-cluster.sh" ]
