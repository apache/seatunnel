#
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

nohup java -Xmx512M -Xms512M -server \
    -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 \
    -XX:+ExplicitGCInvokesConcurrent -XX:MaxInlineLevel=15 -Djava.awt.headless=true \
    -Xlog:gc*:file=/var/log/kafka/zookeeper-gc.log:time,tags:filecount=10,filesize=100M \
    -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false \
    -Dcom.sun.management.jmxremote.ssl=false -Dkafka.logs.dir=/var/log/kafka \
    -Dlog4j.configuration=file:/etc/kafka/log4j.properties \
    -cp /usr/bin/../share/java/kafka/*:/usr/bin/../share/java/confluent-telemetry/* \
    -Dsun.security.krb5.debug=true org.apache.zookeeper.server.quorum.QuorumPeerMain \
    /etc/kafka/zookeeper.properties &

ZK_READY=false
for i in $(seq 1 30); do
    if nc -z localhost 2181 2>/dev/null; then
        echo "ZooKeeper is ready after ${i}s"
        ZK_READY=true
        break
    fi
    sleep 1
done
if [ "$ZK_READY" != "true" ]; then
    echo "ERROR: ZooKeeper failed to start within 30s" >&2
    exit 1
fi

echo "Waiting for Kafka config..."
while [ ! -f /tmp/start_kafka ]; do sleep 0.1; done
echo "Kafka config received, starting broker..."

nohup java -Xmx1G -Xms1G -server \
    -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 \
    -XX:+ExplicitGCInvokesConcurrent -XX:MaxInlineLevel=15 -Djava.awt.headless=true \
    -Xlog:gc*:file=/var/log/kafka/kafkaServer-gc.log:time,tags:filecount=10,filesize=100M \
    -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false \
    -Dcom.sun.management.jmxremote.ssl=false -Dkafka.logs.dir=/var/log/kafka \
    -Dlog4j.configuration=file:/etc/kafka/log4j.properties \
    -cp /usr/bin/../share/java/kafka/*:/usr/bin/../share/java/confluent-telemetry/* \
    -Dsun.security.krb5.debug=true -Djava.security.auth.login.config=/etc/kafka/kafka_server_jaas.conf \
    -Djava.security.krb5.conf=/etc/krb5.conf kafka.Kafka /etc/kafka/kafka.properties &

KAFKA_READY=false
for i in $(seq 1 60); do
    if grep -q "started (kafka.server.KafkaServer)" /var/log/kafka/server.log 2>/dev/null; then
        echo "Kafka broker is ready after ${i}s"
        KAFKA_READY=true
        break
    fi
    sleep 1
done
if [ "$KAFKA_READY" != "true" ]; then
    echo "ERROR: Kafka broker failed to start within 60s" >&2
    cat /var/log/kafka/server.log >&2 2>/dev/null
    exit 1
fi

tail -f /var/log/kafka/server.log