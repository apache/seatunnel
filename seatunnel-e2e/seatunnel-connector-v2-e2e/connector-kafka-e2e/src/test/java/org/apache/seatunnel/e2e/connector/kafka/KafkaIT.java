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

package org.apache.seatunnel.e2e.connector.kafka;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.MessageFormat;
import org.apache.seatunnel.connectors.seatunnel.kafka.serialize.DefaultSeaTunnelRowSerializer;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.format.avro.AvroDeserializationSchema;
import org.apache.seatunnel.format.protobuf.ProtobufDeserializationSchema;
import org.apache.seatunnel.format.text.TextSerializationSchema;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.testcontainers.shaded.org.awaitility.Awaitility.given;

@Slf4j
public class KafkaIT extends TestSuiteBase implements TestResource {
    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:7.0.9";

    private static final String KAFKA_HOST = "kafkaCluster";

    private static final MessageFormat DEFAULT_FORMAT = MessageFormat.JSON;

    private static final String DEFAULT_FIELD_DELIMITER = ",";

    private KafkaProducer<byte[], byte[]> producer;

    private KafkaContainer kafkaContainer;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        kafkaContainer =
                new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(KAFKA_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(KAFKA_IMAGE_NAME)));
        Startables.deepStart(Stream.of(kafkaContainer)).join();
        log.info("Kafka container started");
        given().ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::initKafkaProducer);

        log.info("Write 100 records to topic test_topic_source");
        DefaultSeaTunnelRowSerializer serializer =
                DefaultSeaTunnelRowSerializer.create(
                        "test_topic_source",
                        SEATUNNEL_ROW_TYPE,
                        DEFAULT_FORMAT,
                        DEFAULT_FIELD_DELIMITER,
                        null);
        generateTestData(serializer::serializeRow, 0, 100);
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (producer != null) {
            producer.close();
        }
        if (kafkaContainer != null) {
            kafkaContainer.close();
        }
    }

    @TestTemplate
    public void testSinkKafka(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/kafka_sink_fake_to_kafka.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        String topicName = "test_topic";
        Map<String, String> data = getKafkaConsumerData(topicName);
        ObjectMapper objectMapper = new ObjectMapper();
        String key = data.keySet().iterator().next();
        ObjectNode objectNode = objectMapper.readValue(key, ObjectNode.class);
        Assertions.assertTrue(objectNode.has("c_map"));
        Assertions.assertTrue(objectNode.has("c_string"));
        Assertions.assertEquals(10, data.size());
    }

    @TestTemplate
    public void testTextFormatSinkKafka(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/textFormatIT/fake_source_to_text_sink_kafka.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        String topicName = "test_text_topic";
        Map<String, String> data = getKafkaConsumerData(topicName);
        Assertions.assertEquals(10, data.size());
    }

    @TestTemplate
    public void testDefaultRandomSinkKafka(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/kafka_default_sink_fake_to_kafka.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        String topicName = "topic_default_sink_test";
        List<String> data = getKafkaConsumerListData(topicName);
        Assertions.assertEquals(10, data.size());
    }

    @TestTemplate
    public void testExtractTopicFunction(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/extractTopic_fake_to_kafka.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        String topicName = "test_extract_topic";
        Map<String, String> data = getKafkaConsumerData(topicName);
        ObjectMapper objectMapper = new ObjectMapper();
        String key = data.keySet().iterator().next();
        ObjectNode objectNode = objectMapper.readValue(key, ObjectNode.class);
        Assertions.assertTrue(objectNode.has("c_map"));
        Assertions.assertTrue(objectNode.has("c_string"));
        Assertions.assertEquals(10, data.size());
    }

    @TestTemplate
    public void testSourceKafkaTextToConsole(TestContainer container)
            throws IOException, InterruptedException {
        TextSerializationSchema serializer =
                TextSerializationSchema.builder()
                        .seaTunnelRowType(SEATUNNEL_ROW_TYPE)
                        .delimiter(",")
                        .build();
        generateTestData(
                row -> new ProducerRecord<>("test_topic_text", null, serializer.serialize(row)),
                0,
                100);
        Container.ExecResult execResult =
                container.executeJob("/textFormatIT/kafka_source_text_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @TestTemplate
    public void testSourceKafkaToAssertWithMaxPollRecords1(TestContainer container)
            throws IOException, InterruptedException {
        TextSerializationSchema serializer =
                TextSerializationSchema.builder()
                        .seaTunnelRowType(SEATUNNEL_ROW_TYPE)
                        .delimiter(",")
                        .build();
        generateTestData(
                row ->
                        new ProducerRecord<>(
                                "test_topic_text_max_poll_records_1",
                                null,
                                serializer.serialize(row)),
                0,
                100);
        Container.ExecResult execResult =
                container.executeJob("/kafka/kafka_source_to_assert_with_max_poll_records_1.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @TestTemplate
    public void testSourceKafkaTextToConsoleAssertCatalogTable(TestContainer container)
            throws IOException, InterruptedException {
        TextSerializationSchema serializer =
                TextSerializationSchema.builder()
                        .seaTunnelRowType(SEATUNNEL_ROW_TYPE)
                        .delimiter(",")
                        .build();
        generateTestData(
                row -> new ProducerRecord<>("test_topic_text", null, serializer.serialize(row)),
                0,
                100);
        Container.ExecResult execResult =
                container.executeJob("/textFormatIT/kafka_source_text_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @TestTemplate
    public void testSourceKafkaTopicWithMultipleDotConsoleAssertCatalogTable(
            TestContainer container) throws IOException, InterruptedException {
        TextSerializationSchema serializer =
                TextSerializationSchema.builder()
                        .seaTunnelRowType(SEATUNNEL_ROW_TYPE)
                        .delimiter(",")
                        .build();
        generateTestData(
                row ->
                        new ProducerRecord<>(
                                "test.multiple.point.topic.json", null, serializer.serialize(row)),
                0,
                10);
        Container.ExecResult execResult =
                container.executeJob(
                        "/textFormatIT/kafka_source_topic_multiple_point_text_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @TestTemplate
    public void testSourceKafkaJsonToConsole(TestContainer container)
            throws IOException, InterruptedException {
        DefaultSeaTunnelRowSerializer serializer =
                DefaultSeaTunnelRowSerializer.create(
                        "test_topic_json",
                        SEATUNNEL_ROW_TYPE,
                        DEFAULT_FORMAT,
                        DEFAULT_FIELD_DELIMITER,
                        null);
        generateTestData(row -> serializer.serializeRow(row), 0, 100);
        Container.ExecResult execResult =
                container.executeJob("/jsonFormatIT/kafka_source_json_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @TestTemplate
    public void testSourceKafkaJsonFormatErrorHandleWaySkipToConsole(TestContainer container)
            throws IOException, InterruptedException {
        DefaultSeaTunnelRowSerializer serializer =
                DefaultSeaTunnelRowSerializer.create(
                        "test_topic_error_message",
                        SEATUNNEL_ROW_TYPE,
                        DEFAULT_FORMAT,
                        DEFAULT_FIELD_DELIMITER,
                        null);
        generateTestData(serializer::serializeRow, 0, 10);
        Container.ExecResult execResult =
                container.executeJob(
                        "/kafka/kafkasource_format_error_handle_way_skip_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @TestTemplate
    public void testSourceKafkaJsonFormatErrorHandleWayFailToConsole(TestContainer container)
            throws IOException, InterruptedException {
        DefaultSeaTunnelRowSerializer serializer =
                DefaultSeaTunnelRowSerializer.create(
                        "test_topic_error_message",
                        SEATUNNEL_ROW_TYPE,
                        DEFAULT_FORMAT,
                        DEFAULT_FIELD_DELIMITER,
                        null);
        generateTestData(serializer::serializeRow, 0, 100);
        Container.ExecResult execResult =
                container.executeJob(
                        "/kafka/kafkasource_format_error_handle_way_fail_to_console.conf");
        Assertions.assertEquals(1, execResult.getExitCode(), execResult.getStderr());
    }

    @TestTemplate
    public void testSourceKafka(TestContainer container) throws IOException, InterruptedException {
        testKafkaLatestToConsole(container);
        testKafkaEarliestToConsole(container);
        testKafkaSpecificOffsetsToConsole(container);
        testKafkaTimestampToConsole(container);
    }

    @TestTemplate
    public void testSourceKafkaStartConfig(TestContainer container)
            throws IOException, InterruptedException {
        DefaultSeaTunnelRowSerializer serializer =
                DefaultSeaTunnelRowSerializer.create(
                        "test_topic_group",
                        SEATUNNEL_ROW_TYPE,
                        DEFAULT_FORMAT,
                        DEFAULT_FIELD_DELIMITER,
                        null);
        generateTestData(row -> serializer.serializeRow(row), 100, 150);
        testKafkaGroupOffsetsToConsole(container);
    }

    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "flink and spark won't commit offset when batch job finished")
    @TestTemplate
    public void testSourceKafkaStartConfigWithCommitOffset(TestContainer container)
            throws Exception {
        DefaultSeaTunnelRowSerializer serializer =
                DefaultSeaTunnelRowSerializer.create(
                        "test_topic_group_with_commit_offset",
                        SEATUNNEL_ROW_TYPE,
                        DEFAULT_FORMAT,
                        DEFAULT_FIELD_DELIMITER,
                        null);
        generateTestData(row -> serializer.serializeRow(row), 0, 100);
        testKafkaGroupOffsetsToConsoleWithCommitOffset(container);
    }

    @TestTemplate
    @DisabledOnContainer(value = {TestContainerId.SPARK_2_4})
    public void testFakeSourceToKafkaAvroFormat(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/avro/fake_source_to_kafka_avro_format.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        String[] subField = {
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
        };
        SeaTunnelDataType<?>[] subFieldTypes = {
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
        };
        SeaTunnelRowType subRow = new SeaTunnelRowType(subField, subFieldTypes);
        String[] fieldNames = {
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
            "c_timestamp",
            "c_row"
        };
        SeaTunnelDataType<?>[] fieldTypes = {
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
            LocalTimeType.LOCAL_DATE_TIME_TYPE,
            subRow
        };
        SeaTunnelRowType fake_source_row_type = new SeaTunnelRowType(fieldNames, fieldTypes);
        CatalogTable catalogTable =
                CatalogTableUtil.getCatalogTable("", "", "", "test", fake_source_row_type);
        AvroDeserializationSchema avroDeserializationSchema =
                new AvroDeserializationSchema(catalogTable);
        List<SeaTunnelRow> kafkaSTRow =
                getKafkaSTRow(
                        "test_avro_topic_fake_source",
                        value -> {
                            try {
                                return avroDeserializationSchema.deserialize(value);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
        Assertions.assertEquals(90, kafkaSTRow.size());
        kafkaSTRow.forEach(
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
                    Assertions.assertInstanceOf(SeaTunnelRow.class, row.getField(14));
                });
    }

    @TestTemplate
    @DisabledOnContainer(value = {TestContainerId.SPARK_2_4})
    public void testKafkaAvroToAssert(TestContainer container)
            throws IOException, InterruptedException {
        DefaultSeaTunnelRowSerializer serializer =
                DefaultSeaTunnelRowSerializer.create(
                        "test_avro_topic",
                        SEATUNNEL_ROW_TYPE,
                        MessageFormat.AVRO,
                        DEFAULT_FIELD_DELIMITER,
                        null);
        int start = 0;
        int end = 100;
        generateTestData(row -> serializer.serializeRow(row), start, end);
        Container.ExecResult execResult = container.executeJob("/avro/kafka_avro_to_assert.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        CatalogTable catalogTable =
                CatalogTableUtil.getCatalogTable("", "", "", "test", SEATUNNEL_ROW_TYPE);

        AvroDeserializationSchema avroDeserializationSchema =
                new AvroDeserializationSchema(catalogTable);
        List<SeaTunnelRow> kafkaSTRow =
                getKafkaSTRow(
                        "test_avro_topic",
                        value -> {
                            try {
                                return avroDeserializationSchema.deserialize(value);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
        Assertions.assertEquals(100, kafkaSTRow.size());
        kafkaSTRow.forEach(
                row -> {
                    Assertions.assertTrue(
                            (long) row.getField(0) >= start && (long) row.getField(0) < end);
                    Assertions.assertEquals(
                            Collections.singletonMap("key", Short.parseShort("1")),
                            (Map<String, Short>) row.getField(1));
                    Assertions.assertArrayEquals(
                            new Byte[] {Byte.parseByte("1")}, (Byte[]) row.getField(2));
                    Assertions.assertEquals("string", row.getField(3).toString());
                    Assertions.assertEquals(false, row.getField(4));
                    Assertions.assertEquals(Byte.parseByte("1"), row.getField(5));
                    Assertions.assertEquals(Short.parseShort("1"), row.getField(6));
                    Assertions.assertEquals(Integer.parseInt("1"), row.getField(7));
                    Assertions.assertEquals(Long.parseLong("1"), row.getField(8));
                    Assertions.assertEquals(Float.parseFloat("1.1"), row.getField(9));
                    Assertions.assertEquals(Double.parseDouble("1.1"), row.getField(10));
                    Assertions.assertEquals(BigDecimal.valueOf(11, 1), row.getField(11));
                    Assertions.assertArrayEquals("test".getBytes(), (byte[]) row.getField(12));
                    Assertions.assertEquals(LocalDate.of(2024, 1, 1), row.getField(13));
                    Assertions.assertEquals(
                            LocalDateTime.of(2024, 1, 1, 12, 59, 23), row.getField(14));
                });
    }

    @TestTemplate
    public void testFakeSourceToKafkaProtobufFormat(TestContainer container)
            throws IOException, InterruptedException, URISyntaxException {

        // Execute the job and verify the exit code
        Container.ExecResult execResult =
                container.executeJob("/protobuf/fake_to_kafka_protobuf.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        // Define the SeaTunnelRowType for the address field
        SeaTunnelRowType addressType =
                new SeaTunnelRowType(
                        new String[] {"city", "state", "street"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.STRING_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
                        });

        // Define the SeaTunnelRowType for the main schema
        SeaTunnelRowType seaTunnelRowType =
                new SeaTunnelRowType(
                        new String[] {
                            "c_int32",
                            "c_int64",
                            "c_float",
                            "c_double",
                            "c_bool",
                            "c_string",
                            "c_bytes",
                            "Address",
                            "attributes",
                            "phone_numbers"
                        },
                        new SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.FLOAT_TYPE,
                            BasicType.DOUBLE_TYPE,
                            BasicType.BOOLEAN_TYPE,
                            BasicType.STRING_TYPE,
                            PrimitiveByteArrayType.INSTANCE,
                            addressType,
                            new MapType<>(BasicType.STRING_TYPE, BasicType.FLOAT_TYPE),
                            ArrayType.STRING_ARRAY_TYPE
                        });

        // Parse the configuration file
        String path = getTestConfigFile("/protobuf/fake_to_kafka_protobuf.conf");
        Config config = ConfigFactory.parseFile(new File(path));
        Config sinkConfig = config.getConfigList("sink").get(0);

        // Prepare the schema properties
        Map<String, String> schemaProperties = new HashMap<>();
        schemaProperties.put(
                "protobuf_message_name", sinkConfig.getString("protobuf_message_name"));
        schemaProperties.put("protobuf_schema", sinkConfig.getString("protobuf_schema"));

        // Build the table schema based on SeaTunnelRowType
        TableSchema schema =
                TableSchema.builder()
                        .columns(
                                Arrays.asList(
                                        IntStream.range(0, seaTunnelRowType.getTotalFields())
                                                .mapToObj(
                                                        i ->
                                                                PhysicalColumn.of(
                                                                        seaTunnelRowType
                                                                                .getFieldName(i),
                                                                        seaTunnelRowType
                                                                                .getFieldType(i),
                                                                        0,
                                                                        true,
                                                                        null,
                                                                        null))
                                                .toArray(PhysicalColumn[]::new)))
                        .build();

        // Create the catalog table
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("", "", "", "test"),
                        schema,
                        schemaProperties,
                        Collections.emptyList(),
                        "It is converted from RowType and only has column information.");

        // Initialize the Protobuf deserialization schema
        ProtobufDeserializationSchema deserializationSchema =
                new ProtobufDeserializationSchema(catalogTable);

        // Retrieve and verify Kafka rows
        List<SeaTunnelRow> kafkaRows =
                getKafkaSTRow(
                        "test_protobuf_topic_fake_source",
                        value -> {
                            try {
                                return deserializationSchema.deserialize(value);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });

        Assertions.assertEquals(16, kafkaRows.size());

        // Validate the contents of each row
        kafkaRows.forEach(
                row -> {
                    Assertions.assertInstanceOf(Integer.class, row.getField(0));
                    Assertions.assertInstanceOf(Long.class, row.getField(1));
                    Assertions.assertInstanceOf(Float.class, row.getField(2));
                    Assertions.assertInstanceOf(Double.class, row.getField(3));
                    Assertions.assertInstanceOf(Boolean.class, row.getField(4));
                    Assertions.assertInstanceOf(String.class, row.getField(5));
                    Assertions.assertInstanceOf(byte[].class, row.getField(6));
                    Assertions.assertInstanceOf(SeaTunnelRow.class, row.getField(7));
                    Assertions.assertInstanceOf(Map.class, row.getField(8));
                    Assertions.assertInstanceOf(String[].class, row.getField(9));
                });
    }

    @TestTemplate
    public void testKafkaProtobufToAssert(TestContainer container)
            throws IOException, InterruptedException, URISyntaxException {

        String confFile = "/protobuf/kafka_protobuf_to_assert.conf";
        String path = getTestConfigFile(confFile);
        Config config = ConfigFactory.parseFile(new File(path));
        Config sinkConfig = config.getConfigList("source").get(0);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(sinkConfig);
        SeaTunnelRowType seaTunnelRowType = buildSeaTunnelRowType();

        // Prepare schema properties
        Map<String, String> schemaProperties = new HashMap<>();
        schemaProperties.put(
                "protobuf_message_name", sinkConfig.getString("protobuf_message_name"));
        schemaProperties.put("protobuf_schema", sinkConfig.getString("protobuf_schema"));

        // Build the table schema
        TableSchema schema =
                TableSchema.builder()
                        .columns(
                                Arrays.asList(
                                        IntStream.range(0, seaTunnelRowType.getTotalFields())
                                                .mapToObj(
                                                        i ->
                                                                PhysicalColumn.of(
                                                                        seaTunnelRowType
                                                                                .getFieldName(i),
                                                                        seaTunnelRowType
                                                                                .getFieldType(i),
                                                                        0,
                                                                        true,
                                                                        null,
                                                                        null))
                                                .toArray(PhysicalColumn[]::new)))
                        .build();

        // Create catalog table
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("", "", "", "test"),
                        schema,
                        schemaProperties,
                        Collections.emptyList(),
                        "It is converted from RowType and only has column information.");

        // Initialize the Protobuf deserialization schema
        ProtobufDeserializationSchema deserializationSchema =
                new ProtobufDeserializationSchema(catalogTable);

        DefaultSeaTunnelRowSerializer serializer =
                getDefaultSeaTunnelRowSerializer(
                        "test_protobuf_topic_fake_source", seaTunnelRowType, readonlyConfig);

        sendData(serializer);

        // Execute the job and validate
        Container.ExecResult execResult = container.executeJob(confFile);
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        // Retrieve and verify Kafka rows
        List<SeaTunnelRow> kafkaSTRow =
                getKafkaSTRow(
                        "test_protobuf_topic_fake_source",
                        value -> {
                            try {
                                return deserializationSchema.deserialize(value);
                            } catch (IOException e) {
                                throw new RuntimeException("Error deserializing Kafka message", e);
                            }
                        });

        // Prepare expected values for assertions
        SeaTunnelRow expectedAddress = new SeaTunnelRow(3);
        expectedAddress.setField(0, "city_value");
        expectedAddress.setField(1, "state_value");
        expectedAddress.setField(2, "street_value");

        Map<String, Float> expectedAttributesMap = new HashMap<>();
        expectedAttributesMap.put("k1", 0.1F);
        expectedAttributesMap.put("k2", 2.3F);

        String[] expectedPhoneNumbers = {"1", "2"};

        // Assertions
        Assertions.assertEquals(20, kafkaSTRow.size());
        kafkaSTRow.forEach(
                row -> {
                    Assertions.assertAll(
                            "Verify row fields",
                            () -> Assertions.assertEquals(123, (int) row.getField(0)),
                            () -> Assertions.assertEquals(123123123123L, (long) row.getField(1)),
                            () -> Assertions.assertEquals(0.123f, (float) row.getField(2)),
                            () -> Assertions.assertEquals(0.123d, (double) row.getField(3)),
                            () -> Assertions.assertFalse((boolean) row.getField(4)),
                            () -> Assertions.assertEquals("test data", row.getField(5).toString()),
                            () ->
                                    Assertions.assertArrayEquals(
                                            new byte[] {1, 2, 3}, (byte[]) row.getField(6)),
                            () -> Assertions.assertEquals(expectedAddress, row.getField(7)),
                            () -> Assertions.assertEquals(expectedAttributesMap, row.getField(8)),
                            () ->
                                    Assertions.assertArrayEquals(
                                            expectedPhoneNumbers, (String[]) row.getField(9)));
                });
    }

    @TestTemplate
    @DisabledOnContainer(
            type = EngineType.SPARK,
            value = {})
    public void testKafkaToKafkaExactlyOnceOnStreaming(TestContainer container)
            throws InterruptedException {
        String producerTopic = "kafka_topic_exactly_once_1";
        String consumerTopic = "kafka_topic_exactly_once_2";
        String sourceData = "Seatunnel Exactly Once Example";
        for (int i = 0; i < 10; i++) {
            ProducerRecord<byte[], byte[]> record =
                    new ProducerRecord<>(producerTopic, null, sourceData.getBytes());
            producer.send(record);
            producer.flush();
        }
        Long endOffset = 0l;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConsumerConfig())) {
            consumer.subscribe(Arrays.asList(producerTopic));
            Map<TopicPartition, Long> offsets =
                    consumer.endOffsets(Arrays.asList(new TopicPartition(producerTopic, 0)));
            endOffset = offsets.entrySet().iterator().next().getValue();
        }
        // async execute
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/kafka/kafka_to_kafka_exactly_once_streaming.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        // wait for data written to kafka
        Long finalEndOffset = endOffset;
        given().pollDelay(30, TimeUnit.SECONDS)
                .pollInterval(5, TimeUnit.SECONDS)
                .await()
                .atMost(5, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        checkData(consumerTopic, finalEndOffset, sourceData)));
    }

    @TestTemplate
    public void testKafkaToKafkaExactlyOnceOnBatch(TestContainer container)
            throws InterruptedException, IOException {
        String producerTopic = "kafka_topic_exactly_once_1";
        String consumerTopic = "kafka_topic_exactly_once_2";
        String sourceData = "Seatunnel Exactly Once Example";
        for (int i = 0; i < 10; i++) {
            ProducerRecord<byte[], byte[]> record =
                    new ProducerRecord<>(producerTopic, null, sourceData.getBytes());
            producer.send(record);
            producer.flush();
        }
        Long endOffset;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConsumerConfig())) {
            consumer.subscribe(Arrays.asList(producerTopic));
            Map<TopicPartition, Long> offsets =
                    consumer.endOffsets(Arrays.asList(new TopicPartition(producerTopic, 0)));
            endOffset = offsets.entrySet().iterator().next().getValue();
        }
        Container.ExecResult execResult =
                container.executeJob("/kafka/kafka_to_kafka_exactly_once_batch.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        // wait for data written to kafka
        Assertions.assertTrue(checkData(consumerTopic, endOffset, sourceData));
    }

    // Compare the values of data fields obtained from consumers
    private boolean checkData(String topicName, long endOffset, String data) {
        List<String> listData = getKafkaConsumerListData(topicName, endOffset);
        if (listData.isEmpty() || listData.size() != endOffset) {
            log.error(
                    "testKafkaToKafkaExactlyOnce get data size is not expect,get consumer data size {}",
                    listData.size());
            return false;
        }
        for (String value : listData) {
            if (!data.equals(value)) {
                log.error("testKafkaToKafkaExactlyOnce get data value is not expect");
                return false;
            }
        }
        return true;
    }

    private @NotNull DefaultSeaTunnelRowSerializer getDefaultSeaTunnelRowSerializer(
            String topic, SeaTunnelRowType seaTunnelRowType, ReadonlyConfig readonlyConfig) {
        // Create serializer
        DefaultSeaTunnelRowSerializer serializer =
                DefaultSeaTunnelRowSerializer.create(
                        topic,
                        seaTunnelRowType,
                        MessageFormat.PROTOBUF,
                        DEFAULT_FIELD_DELIMITER,
                        readonlyConfig);
        return serializer;
    }

    private void sendData(DefaultSeaTunnelRowSerializer serializer) {
        // Produce records to Kafka
        IntStream.range(0, 20)
                .forEach(
                        i -> {
                            try {
                                SeaTunnelRow originalRow = buildSeaTunnelRow();
                                ProducerRecord<byte[], byte[]> producerRecord =
                                        serializer.serializeRow(originalRow);
                                producer.send(producerRecord).get();
                            } catch (InterruptedException | ExecutionException e) {
                                throw new RuntimeException("Error sending Kafka message", e);
                            }
                        });

        producer.flush();
    }

    @TestTemplate
    public void testKafkaProtobufForTransformToAssert(TestContainer container)
            throws IOException, InterruptedException, URISyntaxException {

        String confFile = "/protobuf/kafka_protobuf_transform_to_assert.conf";
        String path = getTestConfigFile(confFile);
        Config config = ConfigFactory.parseFile(new File(path));
        Config sinkConfig = config.getConfigList("source").get(0);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(sinkConfig);
        SeaTunnelRowType seaTunnelRowType = buildSeaTunnelRowType();

        // Create serializer
        DefaultSeaTunnelRowSerializer serializer =
                getDefaultSeaTunnelRowSerializer(
                        "test_protobuf_topic_transform_fake_source",
                        seaTunnelRowType,
                        readonlyConfig);

        // Produce records to Kafka
        sendData(serializer);

        // Execute the job and validate
        Container.ExecResult execResult = container.executeJob(confFile);
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        try (KafkaConsumer<byte[], byte[]> consumer =
                new KafkaConsumer<>(kafkaByteConsumerConfig())) {
            consumer.subscribe(Arrays.asList("verify_protobuf_transform"));
            Map<TopicPartition, Long> offsets =
                    consumer.endOffsets(
                            Arrays.asList(new TopicPartition("verify_protobuf_transform", 0)));
            Long endOffset = offsets.entrySet().iterator().next().getValue();
            Long lastProcessedOffset = -1L;

            do {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (lastProcessedOffset < record.offset()) {
                        String data = new String(record.value(), "UTF-8");
                        ObjectNode jsonNodes = JsonUtils.parseObject(data);
                        Assertions.assertEquals(jsonNodes.size(), 2);
                        Assertions.assertEquals(jsonNodes.get("city").asText(), "city_value");
                        Assertions.assertEquals(jsonNodes.get("c_string").asText(), "test data");
                    }
                    lastProcessedOffset = record.offset();
                }
            } while (lastProcessedOffset < endOffset - 1);
        }
    }

    public static String getTestConfigFile(String configFile)
            throws FileNotFoundException, URISyntaxException {
        URL resource = KafkaIT.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }

    public void testKafkaLatestToConsole(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/kafka/kafkasource_latest_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    public void testKafkaEarliestToConsole(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/kafka/kafkasource_earliest_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    public void testKafkaSpecificOffsetsToConsole(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/kafka/kafkasource_specific_offsets_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    public void testKafkaGroupOffsetsToConsole(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/kafka/kafkasource_group_offset_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    public void testKafkaGroupOffsetsToConsoleWithCommitOffset(TestContainer container)
            throws IOException, InterruptedException, ExecutionException {
        Container.ExecResult execResult =
                container.executeJob(
                        "/kafka/kafkasource_group_offset_to_console_with_commit_offset.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        String consumerGroup = "SeaTunnel-Consumer-Group";
        TopicPartition topicPartition =
                new TopicPartition("test_topic_group_with_commit_offset", 0);
        try (AdminClient adminClient = createKafkaAdmin()) {
            ListConsumerGroupOffsetsOptions options =
                    new ListConsumerGroupOffsetsOptions()
                            .topicPartitions(Arrays.asList(topicPartition));
            Map<TopicPartition, Long> topicOffset =
                    adminClient
                            .listConsumerGroupOffsets(consumerGroup, options)
                            .partitionsToOffsetAndMetadata()
                            .thenApply(
                                    result -> {
                                        Map<TopicPartition, Long> offsets = new HashMap<>();
                                        result.forEach(
                                                (tp, oam) -> {
                                                    if (oam != null) {
                                                        offsets.put(tp, oam.offset());
                                                    }
                                                });
                                        return offsets;
                                    })
                            .get();
            Assertions.assertEquals(100L, topicOffset.get(topicPartition));
        }
    }

    public void testKafkaTimestampToConsole(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/kafka/kafkasource_timestamp_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    private AdminClient createKafkaAdmin() {
        Properties props = new Properties();
        String bootstrapServers = kafkaContainer.getBootstrapServers();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(props);
    }

    private void initKafkaProducer() {
        Properties props = new Properties();
        String bootstrapServers = kafkaContainer.getBootstrapServers();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producer = new KafkaProducer<>(props);
    }

    private Properties kafkaConsumerConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "seatunnel-kafka-sink-group");
        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                OffsetResetStrategy.EARLIEST.toString().toLowerCase());
        // exactly once semantics must set config read_commit
        props.put(
                ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                IsolationLevel.READ_COMMITTED.name().toLowerCase());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    private Properties kafkaByteConsumerConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "seatunnel-kafka-sink-group");
        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                OffsetResetStrategy.EARLIEST.toString().toLowerCase());
        props.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        return props;
    }

    private void generateTestData(ProducerRecordConverter converter, int start, int end) {
        try {
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
                ProducerRecord<byte[], byte[]> producerRecord = converter.convert(row);
                producer.send(producerRecord).get();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        producer.flush();
    }

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

    private Map<String, String> getKafkaConsumerData(String topicName) {
        Map<String, String> data = new HashMap<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConsumerConfig())) {
            consumer.subscribe(Arrays.asList(topicName));
            Map<TopicPartition, Long> offsets =
                    consumer.endOffsets(Arrays.asList(new TopicPartition(topicName, 0)));
            Long endOffset = offsets.entrySet().iterator().next().getValue();
            Long lastProcessedOffset = -1L;

            do {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    if (lastProcessedOffset < record.offset()) {
                        data.put(record.key(), record.value());
                    }
                    lastProcessedOffset = record.offset();
                }
            } while (lastProcessedOffset < endOffset - 1);
        }
        return data;
    }

    private List<String> getKafkaConsumerListData(String topicName) {
        List<String> data = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConsumerConfig())) {
            consumer.subscribe(Arrays.asList(topicName));
            Map<TopicPartition, Long> offsets =
                    consumer.endOffsets(Arrays.asList(new TopicPartition(topicName, 0)));
            Long endOffset = offsets.entrySet().iterator().next().getValue();
            Long lastProcessedOffset = -1L;

            do {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    if (lastProcessedOffset < record.offset()) {
                        data.add(record.value());
                    }
                    lastProcessedOffset = record.offset();
                }
            } while (lastProcessedOffset < endOffset - 1);
        }
        return data;
    }

    private List<String> getKafkaConsumerListData(String topicName, long endOffset) {
        List<String> data = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConsumerConfig())) {
            consumer.subscribe(Arrays.asList(topicName));
            Long lastProcessedOffset = -1L;
            do {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    if (lastProcessedOffset < record.offset()) {
                        data.add(record.value());
                    }
                    lastProcessedOffset = record.offset();
                }
            } while (lastProcessedOffset < endOffset - 1);
        }
        return data;
    }

    private List<SeaTunnelRow> getKafkaSTRow(String topicName, ConsumerRecordConverter converter) {
        List<SeaTunnelRow> data = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer =
                new KafkaConsumer<>(kafkaByteConsumerConfig())) {
            consumer.subscribe(Arrays.asList(topicName));
            Map<TopicPartition, Long> offsets =
                    consumer.endOffsets(Arrays.asList(new TopicPartition(topicName, 0)));
            Long endOffset = offsets.entrySet().iterator().next().getValue();
            Long lastProcessedOffset = -1L;

            do {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (lastProcessedOffset < record.offset()) {
                        data.add(converter.convert(record.value()));
                    }
                    lastProcessedOffset = record.offset();
                }
            } while (lastProcessedOffset < endOffset - 1);
        }
        return data;
    }

    interface ProducerRecordConverter {
        ProducerRecord<byte[], byte[]> convert(SeaTunnelRow row);
    }

    interface ConsumerRecordConverter {
        SeaTunnelRow convert(byte[] value);
    }

    private SeaTunnelRow buildSeaTunnelRow() {
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(10);

        Map<String, Float> attributesMap = new HashMap<>();
        attributesMap.put("k1", 0.1F);
        attributesMap.put("k2", 2.3F);

        String[] phoneNumbers = {"1", "2"};
        byte[] byteVal = {1, 2, 3};

        SeaTunnelRow address = new SeaTunnelRow(3);
        address.setField(0, "city_value");
        address.setField(1, "state_value");
        address.setField(2, "street_value");

        seaTunnelRow.setField(0, 123);
        seaTunnelRow.setField(1, 123123123123L);
        seaTunnelRow.setField(2, 0.123f);
        seaTunnelRow.setField(3, 0.123d);
        seaTunnelRow.setField(4, false);
        seaTunnelRow.setField(5, "test data");
        seaTunnelRow.setField(6, byteVal);
        seaTunnelRow.setField(7, address);
        seaTunnelRow.setField(8, attributesMap);
        seaTunnelRow.setField(9, phoneNumbers);

        return seaTunnelRow;
    }

    private SeaTunnelRowType buildSeaTunnelRowType() {
        SeaTunnelRowType addressType =
                new SeaTunnelRowType(
                        new String[] {"city", "state", "street"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.STRING_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
                        });

        return new SeaTunnelRowType(
                new String[] {
                    "c_int32",
                    "c_int64",
                    "c_float",
                    "c_double",
                    "c_bool",
                    "c_string",
                    "c_bytes",
                    "Address",
                    "attributes",
                    "phone_numbers"
                },
                new SeaTunnelDataType<?>[] {
                    BasicType.INT_TYPE,
                    BasicType.LONG_TYPE,
                    BasicType.FLOAT_TYPE,
                    BasicType.DOUBLE_TYPE,
                    BasicType.BOOLEAN_TYPE,
                    BasicType.STRING_TYPE,
                    PrimitiveByteArrayType.INSTANCE,
                    addressType,
                    new MapType<>(BasicType.STRING_TYPE, BasicType.FLOAT_TYPE),
                    ArrayType.STRING_ARRAY_TYPE
                });
    }
}
