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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
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
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.format.avro.AvroDeserializationSchema;
import org.apache.seatunnel.format.avro.AvroSerializationSchema;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.time.temporal.ChronoUnit.SECONDS;

@Slf4j
public class PulsarFormatIT extends TestSuiteBase implements TestResource {

    private static final String PULSAR_IMAGE_NAME = "apachepulsar/pulsar:2.3.1";
    public static final String PULSAR_HOST = "pulsar.e2e.format";
    private static final String FAKE_SOURCE_TOPIC = "test_avro_topic_fake_source";
    private static final String SOURCE_TOPIC = "test_avro_topic";

    private static final SeaTunnelRowType FAKE_SOURCE_ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {
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
                        "c_bytes",
                        "c_date",
                        "c_decimal",
                        "c_timestamp"
                    },
                    new SeaTunnelDataType[] {
                        new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE),
                        ArrayType.INT_ARRAY_TYPE,
                        BasicType.STRING_TYPE,
                        BasicType.BOOLEAN_TYPE,
                        BasicType.BYTE_TYPE,
                        BasicType.SHORT_TYPE,
                        BasicType.INT_TYPE,
                        BasicType.LONG_TYPE,
                        BasicType.FLOAT_TYPE,
                        BasicType.DOUBLE_TYPE,
                        PrimitiveByteArrayType.INSTANCE,
                        LocalTimeType.LOCAL_DATE_TYPE,
                        new DecimalType(38, 18),
                        LocalTimeType.LOCAL_DATE_TIME_TYPE
                    });

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
                        new MapType<>(BasicType.STRING_TYPE, BasicType.SHORT_TYPE),
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

    private PulsarContainer pulsarContainer;
    private PulsarClient client;

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        pulsarContainer =
                PulsarContainerSupport.startPulsarContainer(
                        dockerClient,
                        PULSAR_IMAGE_NAME,
                        NETWORK,
                        PULSAR_HOST,
                        Duration.of(400, SECONDS));
        Awaitility.given()
                .ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertTrue(pulsarContainer.isRunning());
                            if (client == null) {
                                client =
                                        PulsarClient.builder()
                                                .serviceUrl(pulsarContainer.getPulsarBrokerUrl())
                                                .build();
                            }
                        });
    }

    @Override
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        pulsarContainer.close();
    }

    @TestTemplate
    @DisabledOnContainer(value = {TestContainerId.SPARK_2_4})
    public void testFakeSourceToPulsarAvroFormat(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/avro/fake_to_pulsar_avro.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        CatalogTable catalogTable =
                CatalogTableUtil.getCatalogTable("", "", "", "test", FAKE_SOURCE_ROW_TYPE);
        AvroDeserializationSchema avroDeserializationSchema =
                new AvroDeserializationSchema(catalogTable);
        List<SeaTunnelRow> rows = consumeAvroRows(FAKE_SOURCE_TOPIC, avroDeserializationSchema);

        Assertions.assertEquals(90, rows.size());
        rows.forEach(
                row -> {
                    Assertions.assertInstanceOf(Map.class, row.getField(0));
                    Assertions.assertInstanceOf(Integer[].class, row.getField(1));
                    Assertions.assertInstanceOf(String.class, row.getField(2));
                    Assertions.assertEquals("fake_source_avro", row.getField(2).toString());
                    Assertions.assertInstanceOf(Boolean.class, row.getField(3));
                    Assertions.assertInstanceOf(Byte.class, row.getField(4));
                    Assertions.assertInstanceOf(Short.class, row.getField(5));
                    Assertions.assertInstanceOf(Integer.class, row.getField(6));
                    Assertions.assertInstanceOf(Long.class, row.getField(7));
                    Assertions.assertInstanceOf(Float.class, row.getField(8));
                    Assertions.assertInstanceOf(Double.class, row.getField(9));
                    Assertions.assertInstanceOf(byte[].class, row.getField(10));
                    Assertions.assertInstanceOf(LocalDate.class, row.getField(11));
                    Assertions.assertInstanceOf(BigDecimal.class, row.getField(12));
                    Assertions.assertInstanceOf(LocalDateTime.class, row.getField(13));
                });
    }

    @TestTemplate
    @DisabledOnContainer(value = {TestContainerId.SPARK_2_4})
    public void testPulsarAvroToAssert(TestContainer container) throws Exception {
        produceAvroTestData(0, 100);
        Container.ExecResult execResult = container.executeJob("/avro/pulsar_avro_to_assert.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    private void produceAvroTestData(int start, int end) throws Exception {
        AvroSerializationSchema serializationSchema =
                new AvroSerializationSchema(SEATUNNEL_ROW_TYPE);
        try (Producer<byte[]> producer =
                client.newProducer(Schema.BYTES).topic(SOURCE_TOPIC).create()) {
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
                                    LocalDate.of(2024, 1, 1),
                                    LocalDateTime.of(2024, 1, 1, 12, 59, 23)
                                });
                producer.send(serializationSchema.serialize(row));
            }
        }
    }

    private List<SeaTunnelRow> consumeAvroRows(
            String topic, AvroDeserializationSchema deserializationSchema) {
        List<SeaTunnelRow> rows = new ArrayList<>();
        try (Consumer<byte[]> consumer =
                client.newConsumer(Schema.BYTES)
                        .topic(topic)
                        .subscriptionName("PulsarFormatSubTest" + new Random().nextInt())
                        .subscriptionType(SubscriptionType.Exclusive)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe()) {
            for (int i = 0; i < 90; i++) {
                Message<byte[]> message = consumer.receive(30, TimeUnit.SECONDS);
                if (message == null) {
                    break;
                }
                rows.add(deserializationSchema.deserialize(message.getData()));
                consumer.acknowledge(message.getMessageId());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to consume pulsar avro data", e);
        }
        return rows;
    }
}
