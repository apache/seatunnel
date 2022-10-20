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

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.kafka.serialize.DefaultSeaTunnelRowSerializer;
import org.apache.seatunnel.e2e.spark.SparkContainer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.com.google.common.collect.Lists;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
/**
 * This test case is used to verify that the kafka source is able to send data to the console.
 * Make sure the SeaTunnel job can submit successfully on spark engine.
 */
@SuppressWarnings("checkstyle:EmptyLineSeparator")
@Slf4j
public class KafkaSourceJsonToConsoleIT extends SparkContainer {

    private static final int KAFKA_PORT = 9093;

    private static final String KAFKA_HOST = "kafkaCluster";

    private KafkaProducer<byte[], byte[]> producer;

    private KafkaContainer kafkaContainer;

    @BeforeEach
    public void startKafkaContainer() {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
                .withNetwork(NETWORK)
                .withNetworkAliases(KAFKA_HOST)
                .withLogConsumer(new Slf4jLogConsumer(log));
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

    @SuppressWarnings("checkstyle:Indentation")
    private void generateTestData() {

        SeaTunnelRowType seatunnelRowType = new SeaTunnelRowType(
                new String[]{
                        "id",
                        "c_map",
                        "c_array",
                        "c_string",
                        "c_boolean",
                        "c_tinyint",
                        "c_smallint",
                        "c_int",
                        "c_bigint",
                        "c_float",
                        "c_double",
                        "c_decimal",
                        "c_bytes",
                        "c_date",
                        "c_timestamp"
                },
                new SeaTunnelDataType[]{
                        BasicType.LONG_TYPE,
                        new MapType(BasicType.STRING_TYPE, BasicType.SHORT_TYPE),
                        ArrayType.BYTE_ARRAY_TYPE,
                        BasicType.STRING_TYPE,
                        BasicType.BOOLEAN_TYPE,
                        BasicType.BYTE_TYPE,
                        BasicType.SHORT_TYPE,
                        BasicType.INT_TYPE,
                        BasicType.LONG_TYPE,
                        BasicType.FLOAT_TYPE,
                        BasicType.DOUBLE_TYPE,
                        new DecimalType(2, 1),
                        PrimitiveByteArrayType.INSTANCE,
                        LocalTimeType.LOCAL_DATE_TYPE,
                        LocalTimeType.LOCAL_DATE_TIME_TYPE
                }
        );

        DefaultSeaTunnelRowSerializer serializer = new DefaultSeaTunnelRowSerializer(seatunnelRowType);

        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row = new SeaTunnelRow(
                    new Object[]{
                            Long.valueOf(i),
                            Collections.singletonMap("key", Short.parseShort("1")),
                            new Byte[]{Byte.parseByte("1")},
                            "string",
                            Boolean.FALSE,
                            Byte.parseByte("1"),
                            Short.parseShort("1"),
                            Integer.parseInt("1"),
                            Long.parseLong("1"),
                            Float.parseFloat("1.1"),
                            Double.parseDouble("1.1"),
                            BigDecimal.valueOf(11, 1),
                            "test".getBytes(),
                            LocalDate.now(),
                            LocalDateTime.now()
                    });
            ProducerRecord<byte[], byte[]> producerRecord = serializer.serializeRow("test_topic",row);
            producer.send(producerRecord);
        }
    }

    @Test
    public void testKafkaSource() throws IOException, InterruptedException {
        Container.ExecResult execResult =  executeSeaTunnelSparkJob("/kafka/kafkasource_json_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    private void initKafkaProducer() {
        Properties props = new Properties();
        String bootstrapServers = kafkaContainer.getBootstrapServers();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producer = new KafkaProducer<>(props);
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
