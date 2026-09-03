# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM eclipse-temurin:8-jre-jammy

ARG SPARK_VERSION
ARG HADOOP_PROFILE

RUN test -n "${SPARK_VERSION}" \
    && test -n "${HADOOP_PROFILE}" \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y \
        bash \
        ca-certificates \
        curl \
        procps \
        tini \
    && curl --fail --location --retry 3 \
        "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz" \
        --output /tmp/spark.tgz \
    && tar --extract --gzip --file /tmp/spark.tgz --directory /opt \
    && mv "/opt/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}" /opt/spark \
    && rm /tmp/spark.tgz \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/spark \
    SPARK_MODE=master \
    PATH=/opt/spark/bin:/opt/spark/sbin:${PATH}

COPY --chmod=755 entrypoint.sh /opt/seatunnel-e2e/entrypoint.sh

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/seatunnel-e2e/entrypoint.sh"]
