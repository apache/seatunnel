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
FROM openjdk:8-jre-buster
ENV FLINK_HOME /opt/flink
ENV SPARK_HOME /opt/spark
ENV SEATUNNEL_HOME /opt/seatunnel
ENTRYPOINT ["/seatunnel/entrypoint.sh"]
COPY entrypoint.sh /seatunnel/entrypoint.sh
RUN if [ "${install.spark}" = "true" ]; then\
    mkdir -p /opt/spark &&\
    curl ${spark.archive.mirror}/${spark.archive.name}\
    | tar xzf - --strip-components 1 -C /opt/spark; fi;\
    if [ "${install.flink}" = "true" ]; then\
    mkdir -p /opt/flink &&\
    curl ${flink.archive.mirror}/${flink.archive.name}\
    | tar xzf - --strip-components 1 -C /opt/flink; fi;
COPY maven /opt/seatunnel