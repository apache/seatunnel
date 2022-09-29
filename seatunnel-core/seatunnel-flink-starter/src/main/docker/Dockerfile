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
ARG SEATUNNEL_VERSION

RUN mkdir -p /flink ; cd /flink ; \
    tar_file=flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz ; \
    curl -LsO https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/$tar_file ; \
    tar -zxf $tar_file --strip 1 -C . ; \
    rm $tar_file
    
ENV FLINK_HOME=/flink

RUN mkdir /seatunnel ; cd /seatunnel ; \
    seatunnel_archive_name=apache-seatunnel-incubating-${SEATUNNEL_VERSION}-bin.tar.gz ; \
    curl -LsO https://archive.apache.org/dist/incubator/seatunnel/${SEATUNNEL_VERSION}/$seatunnel_archive_name ; \
    tar -zxf $seatunnel_archive_name --strip 1 -C . ; \
    rm $seatunnel_archive_name; \
    rm -rf /seatunnel/connectors/spark /seatunnel/bin/start-seatunnel-{"spark","sql"}.sh /seatunnel/lib/seatunnel-core-spark.jar /seatunnel/config/seatunnel-env.sh

WORKDIR /seatunnel

RUN ep="/seatunnel/entrypoint.sh" ; \
    echo "#!/bin/bash" > $ep ; \
    echo "${FLINK_HOME}/bin/start-cluster.sh" >> $ep ; \
    echo "/seatunnel/bin/start-seatunnel-flink.sh \$1" >> $ep ; \
    chmod ugo+x /seatunnel/entrypoint.sh

ENTRYPOINT [ "/seatunnel/entrypoint.sh" ]
