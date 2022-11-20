# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

ARG BASE_IMAGE=adoptopenjdk/openjdk8:jre

FROM $BASE_IMAGE

ARG SCALA_VERSION
ARG FLINK_VERSION

RUN mkdir -p /flink ; cd /flink ; \
    tar_file=flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz ; \
    curl -LsO https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/$tar_file ; \
    tar -zxf $tar_file --strip 1 -C . ; \
    rm $tar_file

ENV FLINK_HOME=/flink

WORKDIR /seatunnel

COPY target/seatunnel-core-flink.jar /seatunnel/starter/
COPY src/main/bin /seatunnel/bin/

ENTRYPOINT [ "/seatunnel/bin/start-seatunnel-flink-connector-v2.sh" ]
