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

package org.apache.seatunnel.e2e.connector.pulsar;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.stream.Stream;

@Slf4j
@DisabledOnContainer(
        value = {TestContainerId.FLINK_1_13},
        type = {EngineType.FLINK, EngineType.SPARK},
        disabledReason = "")
public class PulsarIT extends TestSuiteBase implements TestResource {
    private static final String IMAGE = "apachepulsar/pulsar:2.8.0";
    private static final String HOST = "pulsar-e2e";
    private static final String TOPIC = "json_topic_source";

    PulsarClient client;
    Producer<byte[]> producer;
    private PulsarContainer pulsarContainer;

    private static final SeaTunnelRowType SEATUNNEL_ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {
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
                    new SeaTunnelDataType[] {
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
                    });

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        this.pulsarContainer =
                new PulsarContainer(DockerImageName.parse(IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)));

        Startables.deepStart(Stream.of(pulsarContainer)).join();
        log.info("pulsar container started");
        client = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build();
        generateTestData(TOPIC, 0, 10);
    }

    private void initProducer(String topic) throws PulsarClientException {
        producer = client.newProducer(Schema.BYTES).topic(topic).create();
    }

    private void generateTestData(String topic, int start, int end) throws PulsarClientException {
        initProducer(topic);
        JsonSerializationSchema jsonSerializationSchema =
                new JsonSerializationSchema(SEATUNNEL_ROW_TYPE);
        for (int i = start; i < end; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                Long.valueOf(i),
                                Collections.singletonMap("key", Short.parseShort("1")),
                                new Byte[] {Byte.parseByte("1")},
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
            producer.send(jsonSerializationSchema.serialize(row));
        }
    }

    @Override
    public void tearDown() throws Exception {
        if (producer != null) {
            producer.close();
        }
        if (client != null) {
            client.close();
        }
        if (this.pulsarContainer != null) {
            this.pulsarContainer.close();
        }
    }

    @TestTemplate
    public void testSourceJsonToConsole(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/source_json_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @TestTemplate
    public void testSourceEarliestToConsole(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/pulsar/source_earliest_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @TestTemplate
    public void testSourceLatestToConsole(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/pulsar/source_latest_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @TestTemplate
    public void testSourceSubscriptionToConsole(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/pulsar/source_subscription_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @TestTemplate
    public void testSourceTimestampToConsole(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/pulsar/source_timestamp_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }
}
