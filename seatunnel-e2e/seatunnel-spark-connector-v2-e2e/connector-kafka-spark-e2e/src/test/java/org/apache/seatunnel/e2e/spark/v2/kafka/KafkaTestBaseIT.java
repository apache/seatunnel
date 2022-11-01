/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.spark.v2.kafka;

import org.apache.seatunnel.e2e.spark.SparkContainer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.com.google.common.collect.Lists;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class KafkaTestBaseIT extends SparkContainer {
    protected static final int KAFKA_PORT = 9094;

    protected static final String KAFKA_HOST = "kafkaCluster";

    protected KafkaProducer<byte[], byte[]> producer;

    protected KafkaContainer kafkaContainer;

    @BeforeEach
    public void startKafkaContainer() {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
            .withNetwork(NETWORK)
            .withNetworkAliases(KAFKA_HOST)
            .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger("confluentinc/cp-kafka:6.2.1")));
        kafkaContainer.setPortBindings(Lists.newArrayList(
            String.format("%s:%s", KAFKA_PORT, KAFKA_PORT)));
        Startables.deepStart(Stream.of(kafkaContainer)).join();
        log.info("Kafka container started");
        Awaitility.given().ignoreExceptions()
            .atLeast(100, TimeUnit.MILLISECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .atMost(180, TimeUnit.SECONDS)
            .untilAsserted(() -> initKafkaProducer());
        generateTestData();
    }

    protected void initKafkaProducer() {
        Properties props = new Properties();
        String bootstrapServers = kafkaContainer.getBootstrapServers();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producer = new KafkaProducer<>(props);
    }

    @SuppressWarnings("checkstyle:Indentation")
    protected void generateTestData() {

    }

    @AfterEach
    public void close() {
        if (producer != null) {
            producer.close();
        }
        if (kafkaContainer != null) {
            kafkaContainer.close();
        }
    }
}
