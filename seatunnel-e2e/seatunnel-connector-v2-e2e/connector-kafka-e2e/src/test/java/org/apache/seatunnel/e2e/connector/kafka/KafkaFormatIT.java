/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.connector.kafka;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresJdbcRowConverter;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.given;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Spark engine will lose the row kind of record")
public class KafkaFormatIT extends TestSuiteBase implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaFormatIT.class);

    // ---------------------------MaxWell Format Parameter---------------------------------------
    private static final String MAXWELL_DATA_PATH = "/maxwell/maxwell_data.txt";
    private static final String MAXWELL_KAFKA_SOURCE_TOPIC = "maxwell-test-cdc_mds";
    private static final String MAXWELL_KAFKA_SINK_TOPIC = "test-maxwell-sink";

    // ---------------------------Ogg Format Parameter---------------------------------------
    private static final String OGG_DATA_PATH = "/ogg/ogg_data.txt";
    private static final String OGG_KAFKA_SOURCE_TOPIC = "test-ogg-source";
    private static final String OGG_KAFKA_SINK_TOPIC = "test-ogg-sink";

    // ---------------------------Canal Format Parameter---------------------------------------

    private static final String CANAL_KAFKA_SINK_TOPIC = "test-canal-sink";
    private static final String CANAL_DATA_PATH = "/canal/canal_data.txt";
    private static final String CANAL_KAFKA_SOURCE_TOPIC = "test-cdc_mds";

    // ---------------------------Compatible Format Parameter---------------------------------------
    private static final String COMPATIBLE_DATA_PATH = "/compatible/compatible_data.txt";
    private static final String COMPATIBLE_KAFKA_SOURCE_TOPIC = "jdbc_source_record";

    // ---------------------------Debezium Format Parameter  ---------------------------------------
    private static final String DEBEZIUM_KAFKA_SINK_TOPIC = "test-debezium-sink";
    private static final String DEBEZIUM_DATA_PATH = "/debezium/debezium_data.txt";
    private static final String DEBEZIUM_KAFKA_SOURCE_TOPIC = "dbserver1.debezium.products";

    private static final String PG_SINK_TABLE1 = "sink";
    private static final String PG_SINK_TABLE2 = "sink2";

    private static final Map<String, CatalogTable> sinkTables = new HashMap<>();

    static {
        sinkTables.put(
                PG_SINK_TABLE1,
                CatalogTableUtil.getCatalogTable(
                        PG_SINK_TABLE1,
                        new SeaTunnelRowType(
                                new String[] {"id", "name", "description", "weight"},
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE
                                })));

        sinkTables.put(
                PG_SINK_TABLE2,
                CatalogTableUtil.getCatalogTable(
                        PG_SINK_TABLE2,
                        new SeaTunnelRowType(
                                new String[] {
                                    "id",
                                    "f_binary",
                                    "f_blob",
                                    "f_long_varbinary",
                                    "f_longblob",
                                    "f_tinyblob",
                                    "f_varbinary",
                                    "f_smallint",
                                    "f_smallint_unsigned",
                                    "f_mediumint",
                                    "f_mediumint_unsigned",
                                    "f_int",
                                    "f_int_unsigned",
                                    "f_integer",
                                    "f_integer_unsigned",
                                    "f_bigint",
                                    "f_bigint_unsigned",
                                    "f_numeric",
                                    "f_decimal",
                                    "f_float",
                                    "f_double",
                                    "f_double_precision",
                                    "f_longtext",
                                    "f_mediumtext",
                                    "f_text",
                                    "f_tinytext",
                                    "f_varchar",
                                    "f_date",
                                    "f_datetime",
                                    "f_timestamp",
                                    "f_bit1",
                                    "f_bit64",
                                    "f_char",
                                    "f_enum",
                                    "f_mediumblob",
                                    "f_long_varchar",
                                    "f_real",
                                    "f_time",
                                    "f_tinyint",
                                    "f_tinyint_unsigned",
                                    "f_json",
                                    "f_year"
                                },
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE,
                                    PrimitiveByteArrayType.INSTANCE,
                                    PrimitiveByteArrayType.INSTANCE,
                                    PrimitiveByteArrayType.INSTANCE,
                                    PrimitiveByteArrayType.INSTANCE,
                                    PrimitiveByteArrayType.INSTANCE,
                                    PrimitiveByteArrayType.INSTANCE,
                                    BasicType.SHORT_TYPE,
                                    BasicType.INT_TYPE,
                                    BasicType.INT_TYPE,
                                    BasicType.INT_TYPE,
                                    BasicType.INT_TYPE,
                                    BasicType.INT_TYPE,
                                    BasicType.INT_TYPE,
                                    BasicType.LONG_TYPE,
                                    BasicType.LONG_TYPE,
                                    new DecimalType(10, 0),
                                    new DecimalType(10, 0),
                                    new DecimalType(10, 0),
                                    BasicType.FLOAT_TYPE,
                                    BasicType.DOUBLE_TYPE,
                                    BasicType.DOUBLE_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE,
                                    LocalTimeType.LOCAL_DATE_TYPE,
                                    LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                    LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                    BasicType.BOOLEAN_TYPE,
                                    BasicType.BYTE_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE,
                                    PrimitiveByteArrayType.INSTANCE,
                                    BasicType.STRING_TYPE,
                                    BasicType.DOUBLE_TYPE,
                                    LocalTimeType.LOCAL_TIME_TYPE,
                                    BasicType.BYTE_TYPE,
                                    BasicType.INT_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.INT_TYPE
                                })));
    }

    // Used to map local data paths to kafa topics that need to be written to kafka
    private static LinkedHashMap<String, String> LOCAL_DATA_TO_KAFKA_MAPPING;

    // Initialization maps local data and paths ready to be sent to kafka
    static {
        LOCAL_DATA_TO_KAFKA_MAPPING =
                new LinkedHashMap<String, String>() {
                    {
                        put(CANAL_DATA_PATH, CANAL_KAFKA_SOURCE_TOPIC);
                        put(OGG_DATA_PATH, OGG_KAFKA_SOURCE_TOPIC);
                        put(MAXWELL_DATA_PATH, MAXWELL_KAFKA_SOURCE_TOPIC);
                        put(COMPATIBLE_DATA_PATH, COMPATIBLE_KAFKA_SOURCE_TOPIC);
                        put(DEBEZIUM_DATA_PATH, DEBEZIUM_KAFKA_SOURCE_TOPIC);
                    }
                };
    }

    // ---------------------------Kafka Container---------------------------------------
    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:7.0.9";

    private static final String KAFKA_HOST = "kafka_e2e";

    private static KafkaContainer KAFKA_CONTAINER;

    private KafkaConsumer<String, String> kafkaConsumer;

    // --------------------------- Postgres Container-------------------------------------
    private static final String PG_IMAGE = "postgres:alpine3.16";

    private static final String PG_DRIVER_JAR =
            "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar";

    private static PostgreSQLContainer<?> POSTGRESQL_CONTAINER;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O "
                                        + PG_DRIVER_JAR);
                Assertions.assertEquals(0, extraCommands.getExitCode());
            };

    private void createKafkaContainer() {
        KAFKA_CONTAINER =
                new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(KAFKA_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(KAFKA_IMAGE_NAME)));
    }

    private void createPostgreSQLContainer() {
        POSTGRESQL_CONTAINER =
                new PostgreSQLContainer<>(DockerImageName.parse(PG_IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("postgresql")
                        .withExposedPorts(5432)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));
    }

    @BeforeAll
    @Override
    public void startUp() throws ClassNotFoundException, InterruptedException, IOException {

        LOG.info("The first stage: Starting Kafka containers...");
        createKafkaContainer();
        Startables.deepStart(Stream.of(KAFKA_CONTAINER)).join();
        LOG.info("Kafka Containers are started");

        LOG.info("The fourth stage: Starting PostgreSQL container...");
        createPostgreSQLContainer();
        Startables.deepStart(Stream.of(POSTGRESQL_CONTAINER)).join();
        Class.forName(POSTGRESQL_CONTAINER.getDriverClassName());
        LOG.info("postgresql Containers are started");

        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(this::initializeJdbcTable);

        given().ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::initKafkaConsumer);

        // local file local data send kafka
        given().ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(3, TimeUnit.MINUTES)
                .untilAsserted(this::initLocalDataToKafka);
        Thread.sleep(20 * 1000);
    }

    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "The multi-catalog does not currently support the Spark Flink engine")
    @TestTemplate
    public void testMultiFormatCheck(TestContainer container)
            throws IOException, InterruptedException {
        LOG.info(
                "====================== Multi Source Format Canal and Ogg Check  ======================");
        Container.ExecResult execCanalAndOggResultKafka =
                container.executeJob("/multiFormatIT/kafka_multi_source_to_pg.conf");
        Assertions.assertEquals(
                0,
                execCanalAndOggResultKafka.getExitCode(),
                execCanalAndOggResultKafka.getStderr());
        checkFormatCanalAndOgg();
    }

    @TestTemplate
    public void testFormatCanalCheck(TestContainer container)
            throws IOException, InterruptedException {
        LOG.info("====================== Check Canal======================");
        Container.ExecResult execCanalResultKafka =
                container.executeJob("/canalFormatIT/kafka_source_canal_to_kafka.conf");
        Assertions.assertEquals(
                0, execCanalResultKafka.getExitCode(), execCanalResultKafka.getStderr());
        Container.ExecResult execCanalResultToPgSql =
                container.executeJob("/canalFormatIT/kafka_source_canal_cdc_to_pgsql.conf");
        Assertions.assertEquals(
                0, execCanalResultToPgSql.getExitCode(), execCanalResultToPgSql.getStderr());
        // Check Canal
        checkCanalFormat();
    }

    @TestTemplate
    public void testFormatOggCheck(TestContainer container)
            throws IOException, InterruptedException {

        LOG.info("====================== Check Ogg======================");
        Container.ExecResult execOggResultKafka =
                container.executeJob("/oggFormatIT/kafka_source_ogg_to_kafka.conf");
        Assertions.assertEquals(
                0, execOggResultKafka.getExitCode(), execOggResultKafka.getStderr());
        // check ogg kafka to postgresql
        Container.ExecResult execOggResultToPgSql =
                container.executeJob("/oggFormatIT/kafka_source_ogg_to_pgsql.conf");
        Assertions.assertEquals(
                0, execOggResultToPgSql.getExitCode(), execOggResultToPgSql.getStderr());

        // Check Ogg
        checkOggFormat();
    }

    @TestTemplate
    public void testFormatDebeziumCheck(TestContainer container)
            throws IOException, InterruptedException {

        LOG.info("======================  Check Debezium ====================== ");
        Container.ExecResult execDebeziumResultKafka =
                container.executeJob("/debeziumFormatIT/kafkasource_debezium_to_kafka.conf");
        Assertions.assertEquals(
                0, execDebeziumResultKafka.getExitCode(), execDebeziumResultKafka.getStderr());

        Container.ExecResult execDebeziumResultToPgSql =
                container.executeJob("/debeziumFormatIT/kafkasource_debezium_cdc_to_pgsql.conf");
        Assertions.assertEquals(
                0, execDebeziumResultToPgSql.getExitCode(), execDebeziumResultToPgSql.getStderr());
        // Check debezium
        checkDebeziumFormat();
    }

    @TestTemplate
    public void testFormatCompatibleCheck(TestContainer container)
            throws IOException, InterruptedException {

        LOG.info("======================  Check Compatible ====================== ");
        Container.ExecResult execCompatibleResultToPgSql =
                container.executeJob("/compatibleFormatIT/kafkasource_jdbc_record_to_pgsql.conf");
        Assertions.assertEquals(
                0,
                execCompatibleResultToPgSql.getExitCode(),
                execCompatibleResultToPgSql.getStderr());

        // Check Compatible
        checkCompatibleFormat();
    }

    @TestTemplate
    public void testFormatMaxWellCheck(TestContainer container)
            throws IOException, InterruptedException {

        LOG.info("====================== Check MaxWell======================");
        // check MaxWell to Postgresql
        Container.ExecResult checkMaxWellResultToKafka =
                container.executeJob("/maxwellFormatIT/kafkasource_maxwell_to_kafka.conf");
        Assertions.assertEquals(
                0, checkMaxWellResultToKafka.getExitCode(), checkMaxWellResultToKafka.getStderr());

        Container.ExecResult checkDataResult =
                container.executeJob("/maxwellFormatIT/kafkasource_maxwell_cdc_to_pgsql.conf");
        Assertions.assertEquals(0, checkDataResult.getExitCode(), checkDataResult.getStderr());

        // Check MaxWell
        checkMaxWellFormat();
    }

    private void checkFormatCanalAndOgg() {
        List<List<Object>> postgreSinkTableList = getPostgreSinkTableList(PG_SINK_TABLE1);
        List<List<Object>> checkArraysResult =
                Stream.<List<Object>>of(
                                Arrays.asList(
                                        101,
                                        "scooter",
                                        "Small 2-wheel scooter",
                                        "3.140000104904175"),
                                Arrays.asList(
                                        102, "car battery", "12V car battery", "8.100000381469727"),
                                Arrays.asList(
                                        103,
                                        "12-pack drill bits",
                                        "12-pack of drill bits with sizes ranging from #40 to #3",
                                        "0.800000011920929"),
                                Arrays.asList(104, "hammer", "12oz carpenter's hammer", "0.75"),
                                Arrays.asList(105, "hammer", "14oz carpenter's hammer", "0.875"),
                                Arrays.asList(106, "hammer", "18oz carpenter hammer", "1"),
                                Arrays.asList(
                                        107, "rocks", "box of assorted rocks", "5.099999904632568"),
                                Arrays.asList(
                                        108,
                                        "jacket",
                                        "water resistent black wind breaker",
                                        "0.10000000149011612"),
                                Arrays.asList(
                                        109,
                                        "spare tire",
                                        "24 inch spare tire",
                                        "22.200000762939453"),
                                Arrays.asList(
                                        110,
                                        "jacket",
                                        "new water resistent white wind breaker",
                                        "0.5"),
                                Arrays.asList(1101, "scooter", "Small 2-wheel scooter", "4.56"),
                                Arrays.asList(1102, "car battery", "12V car battery", "8.1"),
                                Arrays.asList(
                                        1103,
                                        "12-pack drill bits",
                                        "12-pack of drill bits with sizes ranging from #40 to #3",
                                        "0.8"),
                                Arrays.asList(1104, "hammer", "12oz carpenter's hammer", "0.75"),
                                Arrays.asList(1105, "hammer", "14oz carpenter's hammer", "0.875"),
                                Arrays.asList(1106, "hammer", "16oz carpenter's hammer", "1.0"),
                                Arrays.asList(1107, "rocks", "box of assorted rocks", "7.88"),
                                Arrays.asList(
                                        1108,
                                        "jacket",
                                        "water resistent black wind breaker",
                                        "0.1"))
                        .collect(Collectors.toList());
        Assertions.assertIterableEquals(postgreSinkTableList, checkArraysResult);
    }

    private void checkCanalFormat() {
        List<String> expectedResult =
                Arrays.asList(
                        "{\"data\":{\"id\":1101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":\"3.14\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":1102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":\"8.1\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":1103,\"name\":\"12-pack drill bits\",\"description\":\"12-pack of drill bits with sizes ranging from #40 to #3\",\"weight\":\"0.8\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":1104,\"name\":\"hammer\",\"description\":\"12oz carpenter's hammer\",\"weight\":\"0.75\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":1105,\"name\":\"hammer\",\"description\":\"14oz carpenter's hammer\",\"weight\":\"0.875\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":1106,\"name\":\"hammer\",\"description\":\"16oz carpenter's hammer\",\"weight\":\"1.0\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":1107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":\"5.3\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":1108,\"name\":\"jacket\",\"description\":\"water resistent black wind breaker\",\"weight\":\"0.1\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":1109,\"name\":\"spare tire\",\"description\":\"24 inch spare tire\",\"weight\":\"22.2\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":1101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":\"3.14\"},\"type\":\"DELETE\"}",
                        "{\"data\":{\"id\":1101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":\"4.56\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":1107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":\"5.3\"},\"type\":\"DELETE\"}",
                        "{\"data\":{\"id\":1107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":\"7.88\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":1109,\"name\":\"spare tire\",\"description\":\"24 inch spare tire\",\"weight\":\"22.2\"},\"type\":\"DELETE\"}");

        ArrayList<String> result = new ArrayList<>();
        ArrayList<String> topics = new ArrayList<>();
        topics.add(CANAL_KAFKA_SINK_TOPIC);
        kafkaConsumer.subscribe(topics);
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            ConsumerRecords<String, String> consumerRecords =
                                    kafkaConsumer.poll(Duration.ofMillis(1000));
                            for (ConsumerRecord<String, String> record : consumerRecords) {
                                result.add(record.value());
                            }
                            Assertions.assertEquals(expectedResult, result);
                        });

        LOG.info("==================== start kafka canal format to pg check ====================");

        List<List<Object>> postgreSinkTableList = getPostgreSinkTableList(PG_SINK_TABLE1);

        List<List<Object>> expected =
                Stream.<List<Object>>of(
                                Arrays.asList(1101, "scooter", "Small 2-wheel scooter", "4.56"),
                                Arrays.asList(1102, "car battery", "12V car battery", "8.1"),
                                Arrays.asList(
                                        1103,
                                        "12-pack drill bits",
                                        "12-pack of drill bits with sizes ranging from #40 to #3",
                                        "0.8"),
                                Arrays.asList(1104, "hammer", "12oz carpenter's hammer", "0.75"),
                                Arrays.asList(1105, "hammer", "14oz carpenter's hammer", "0.875"),
                                Arrays.asList(1106, "hammer", "16oz carpenter's hammer", "1.0"),
                                Arrays.asList(1107, "rocks", "box of assorted rocks", "7.88"),
                                Arrays.asList(
                                        1108,
                                        "jacket",
                                        "water resistent black wind breaker",
                                        "0.1"))
                        .collect(Collectors.toList());
        Assertions.assertIterableEquals(expected, postgreSinkTableList);
    }

    private void checkMaxWellFormat() {
        List<String> expectedResult =
                Arrays.asList(
                        "{\"data\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":\"3.14\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":\"8.1\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":103,\"name\":\"12-pack drill bits\",\"description\":\"12-pack of drill bits with sizes ranging from #40 to #3\",\"weight\":\"0.8\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":104,\"name\":\"hammer\",\"description\":\"12oz carpenter's hammer\",\"weight\":\"0.75\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":105,\"name\":\"hammer\",\"description\":\"14oz carpenter's hammer\",\"weight\":\"0.875\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":106,\"name\":\"hammer\",\"description\":\"16oz carpenter's hammer\",\"weight\":\"1.0\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":\"5.3\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":108,\"name\":\"jacket\",\"description\":\"water resistent black wind breaker\",\"weight\":\"0.1\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":109,\"name\":\"spare tire\",\"description\":\"24 inch spare tire\",\"weight\":\"22.2\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":\"3.14\"},\"type\":\"DELETE\"}",
                        "{\"data\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":\"4.56\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":\"5.3\"},\"type\":\"DELETE\"}",
                        "{\"data\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":\"7.88\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":109,\"name\":\"spare tire\",\"description\":\"24 inch spare tire\",\"weight\":\"22.2\"},\"type\":\"DELETE\"}");

        ArrayList<String> result = new ArrayList<>();
        ArrayList<String> topics = new ArrayList<>();
        topics.add(MAXWELL_KAFKA_SINK_TOPIC);
        kafkaConsumer.subscribe(topics);
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            ConsumerRecords<String, String> consumerRecords =
                                    kafkaConsumer.poll(Duration.ofMillis(1000));
                            for (ConsumerRecord<String, String> record : consumerRecords) {
                                result.add(record.value());
                            }
                            Assertions.assertEquals(expectedResult, result);
                        });

        LOG.info(
                "==================== start kafka MaxWell format to pg check ====================");

        List<List<Object>> postgreSinkTableList = getPostgreSinkTableList(PG_SINK_TABLE1);

        List<List<Object>> expected =
                Stream.<List<Object>>of(
                                Arrays.asList(101, "scooter", "Small 2-wheel scooter", "4.56"),
                                Arrays.asList(102, "car battery", "12V car battery", "8.1"),
                                Arrays.asList(
                                        103,
                                        "12-pack drill bits",
                                        "12-pack of drill bits with sizes ranging from #40 to #3",
                                        "0.8"),
                                Arrays.asList(104, "hammer", "12oz carpenter's hammer", "0.75"),
                                Arrays.asList(105, "hammer", "14oz carpenter's hammer", "0.875"),
                                Arrays.asList(106, "hammer", "16oz carpenter's hammer", "1.0"),
                                Arrays.asList(107, "rocks", "box of assorted rocks", "7.88"),
                                Arrays.asList(
                                        108, "jacket", "water resistent black wind breaker", "0.1"))
                        .collect(Collectors.toList());
        Assertions.assertIterableEquals(expected, postgreSinkTableList);
    }

    private void checkOggFormat() {
        List<String> kafkaExpectedResult =
                Arrays.asList(
                        "{\"data\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":\"3.140000104904175\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":\"8.100000381469727\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":103,\"name\":\"12-pack drill bits\",\"description\":\"12-pack of drill bits with sizes ranging from #40 to #3\",\"weight\":\"0.800000011920929\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":104,\"name\":\"hammer\",\"description\":\"12oz carpenter's hammer\",\"weight\":\"0.75\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":105,\"name\":\"hammer\",\"description\":\"14oz carpenter's hammer\",\"weight\":\"0.875\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":106,\"name\":\"hammer\",\"description\":\"16oz carpenter's hammer\",\"weight\":\"1\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":\"5.300000190734863\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":108,\"name\":\"jacket\",\"description\":\"water resistent black wind breaker\",\"weight\":\"0.10000000149011612\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":109,\"name\":\"spare tire\",\"description\":\"24 inch spare tire\",\"weight\":\"22.200000762939453\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":106,\"name\":\"hammer\",\"description\":\"16oz carpenter's hammer\",\"weight\":\"1\"},\"type\":\"DELETE\"}",
                        "{\"data\":{\"id\":106,\"name\":\"hammer\",\"description\":\"18oz carpenter hammer\",\"weight\":\"1\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":\"5.300000190734863\"},\"type\":\"DELETE\"}",
                        "{\"data\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":\"5.099999904632568\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":110,\"name\":\"jacket\",\"description\":\"water resistent white wind breaker\",\"weight\":\"0.20000000298023224\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":\"5.179999828338623\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":110,\"name\":\"jacket\",\"description\":\"water resistent white wind breaker\",\"weight\":\"0.20000000298023224\"},\"type\":\"DELETE\"}",
                        "{\"data\":{\"id\":110,\"name\":\"jacket\",\"description\":\"new water resistent white wind breaker\",\"weight\":\"0.5\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":\"5.179999828338623\"},\"type\":\"DELETE\"}",
                        "{\"data\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":\"5.170000076293945\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":\"5.170000076293945\"},\"type\":\"DELETE\"}");

        ArrayList<String> checkKafkaConsumerResult = new ArrayList<>();
        ArrayList<String> topics = new ArrayList<>();
        topics.add(OGG_KAFKA_SINK_TOPIC);
        kafkaConsumer.subscribe(topics);
        // check ogg kafka to kafka
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            ConsumerRecords<String, String> consumerRecords =
                                    kafkaConsumer.poll(Duration.ofMillis(1000));
                            for (ConsumerRecord<String, String> record : consumerRecords) {
                                checkKafkaConsumerResult.add(record.value());
                            }
                            Assertions.assertEquals(kafkaExpectedResult, checkKafkaConsumerResult);
                        });

        LOG.info("==================== start kafka ogg format to pg check ====================");

        List<List<Object>> postgresqlEexpectedResult = getPostgreSinkTableList(PG_SINK_TABLE1);
        List<List<Object>> checkArraysResult =
                Stream.<List<Object>>of(
                                Arrays.asList(
                                        101,
                                        "scooter",
                                        "Small 2-wheel scooter",
                                        "3.140000104904175"),
                                Arrays.asList(
                                        102, "car battery", "12V car battery", "8.100000381469727"),
                                Arrays.asList(
                                        103,
                                        "12-pack drill bits",
                                        "12-pack of drill bits with sizes ranging from #40 to #3",
                                        "0.800000011920929"),
                                Arrays.asList(104, "hammer", "12oz carpenter's hammer", "0.75"),
                                Arrays.asList(105, "hammer", "14oz carpenter's hammer", "0.875"),
                                Arrays.asList(106, "hammer", "18oz carpenter hammer", "1"),
                                Arrays.asList(
                                        107, "rocks", "box of assorted rocks", "5.099999904632568"),
                                Arrays.asList(
                                        108,
                                        "jacket",
                                        "water resistent black wind breaker",
                                        "0.10000000149011612"),
                                Arrays.asList(
                                        109,
                                        "spare tire",
                                        "24 inch spare tire",
                                        "22.200000762939453"),
                                Arrays.asList(
                                        110,
                                        "jacket",
                                        "new water resistent white wind breaker",
                                        "0.5"))
                        .collect(Collectors.toList());
        Assertions.assertIterableEquals(postgresqlEexpectedResult, checkArraysResult);
    }

    private void checkDebeziumFormat() {
        ArrayList<String> result = new ArrayList<>();
        kafkaConsumer.subscribe(Lists.newArrayList(DEBEZIUM_KAFKA_SINK_TOPIC));
        Awaitility.await()
                .atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            ConsumerRecords<String, String> consumerRecords =
                                    kafkaConsumer.poll(Duration.ofMillis(1000));
                            for (ConsumerRecord<String, String> record : consumerRecords) {
                                result.add(record.value());
                            }
                            Assertions.assertEquals(3, result.size());
                        });
        LOG.info(
                "==================== start kafka debezium format to pg check ====================");
        List<List<Object>> actual = getPostgreSinkTableList(PG_SINK_TABLE2);
        List<List<Object>> expected =
                Stream.<List<Object>>of(
                                Arrays.asList(
                                        1,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        (short) 12345,
                                        54321,
                                        123456,
                                        654321,
                                        1234567,
                                        7654321,
                                        1234567,
                                        7654321L,
                                        123456789L,
                                        new BigDecimal(987654321),
                                        new BigDecimal(123),
                                        new BigDecimal(789),
                                        12.34f,
                                        56.78,
                                        90.12,
                                        "This is a long text field",
                                        "This is a medium text field",
                                        "This is a text field",
                                        "This is a tiny text field",
                                        "This is a varchar field",
                                        LocalDate.parse("2022-04-27"),
                                        LocalDateTime.parse("2022-04-27T14:30"),
                                        LocalDateTime.parse("2023-04-27T03:08:40"),
                                        true,
                                        (byte) 0,
                                        "C",
                                        "enum2",
                                        null,
                                        "This is a long varchar field",
                                        12.345,
                                        LocalTime.parse("14:30"),
                                        (byte) -128,
                                        255,
                                        "{\"key\": \"value\"}",
                                        2022),
                                Arrays.asList(
                                        2,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        (short) 12345,
                                        54321,
                                        123456,
                                        654321,
                                        1234567,
                                        7654321,
                                        1234567,
                                        7654321L,
                                        123456789L,
                                        new BigDecimal(987654321),
                                        new BigDecimal(123),
                                        new BigDecimal(789),
                                        12.34f,
                                        56.78,
                                        90.12,
                                        "This is a long text field",
                                        "This is a medium text field",
                                        "This is a text field",
                                        "This is a tiny text field",
                                        "This is a varchar field",
                                        LocalDate.parse("2022-04-27"),
                                        LocalDateTime.parse("2022-04-27T14:30"),
                                        LocalDateTime.parse("2023-04-27T03:08:40"),
                                        true,
                                        (byte) 0,
                                        "C",
                                        "enum2",
                                        null,
                                        "This is a long varchar field",
                                        112.345,
                                        LocalTime.parse("14:30"),
                                        (byte) -128,
                                        22,
                                        "{\"key\": \"value\"}",
                                        2013),
                                Arrays.asList(
                                        3,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        (short) 12345,
                                        54321,
                                        123456,
                                        654321,
                                        1234567,
                                        7654321,
                                        1234567,
                                        7654321L,
                                        123456789L,
                                        new BigDecimal(987654321),
                                        new BigDecimal(123),
                                        new BigDecimal(789),
                                        12.34f,
                                        56.78,
                                        90.12,
                                        "This is a long text field",
                                        "This is a medium text field",
                                        "This is a text field",
                                        "This is a tiny text field",
                                        "This is a varchar field",
                                        LocalDate.parse("2022-04-27"),
                                        LocalDateTime.parse("2022-04-27T14:30"),
                                        LocalDateTime.parse("2023-04-27T03:08:40"),
                                        true,
                                        (byte) 0,
                                        "C",
                                        "enum2",
                                        null,
                                        "This is a long varchar field",
                                        112.345,
                                        LocalTime.parse("14:30"),
                                        (byte) -128,
                                        22,
                                        "{\"key\": \"value\"}",
                                        2021))
                        .collect(Collectors.toList());

        // not compare bytes for now
        for (Integer i : Arrays.asList(1, 2, 3, 5, 6, 34)) {
            for (int j = 0; j < 3; j++) {
                actual.get(j).set(i, null);
                expected.get(j).set(i, null);
            }
        }
        Assertions.assertIterableEquals(expected, actual);
    }

    private void checkCompatibleFormat() {
        LOG.info(
                "==================== start kafka Compatible format to pg check ====================");
        List<List<Object>> actual = getPostgreSinkTableList(PG_SINK_TABLE1);
        List<List<Object>> expected =
                Stream.<List<Object>>of(
                                Arrays.asList(15, "test", "test", "20"),
                                Arrays.asList(16, "test-001", "test", "30"),
                                Arrays.asList(18, "sdc", "sdc", "sdc"))
                        .collect(Collectors.toList());
        Assertions.assertIterableEquals(expected, actual);
    }

    // Initialize the kafka Consumer
    private void initKafkaConsumer() {
        Properties prop = new Properties();
        String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "CONF");
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        kafkaConsumer = new KafkaConsumer<>(prop);
    }

    // Example Initialize the pg sink table
    private void initializeJdbcTable() {
        try (Connection connection =
                DriverManager.getConnection(
                        POSTGRESQL_CONTAINER.getJdbcUrl(),
                        POSTGRESQL_CONTAINER.getUsername(),
                        POSTGRESQL_CONTAINER.getPassword())) {
            Statement statement = connection.createStatement();
            String sink =
                    "create table if not exists sink(\n"
                            + "id INT NOT NULL PRIMARY KEY,\n"
                            + "name varchar(255),\n"
                            + "description varchar(255),\n"
                            + "weight varchar(255)"
                            + ")";
            String sink2 =
                    "CREATE TABLE if not exists sink2\n"
                            + "(\n"
                            + "    id                   SERIAL PRIMARY KEY,\n"
                            + "    f_binary             BYTEA,\n"
                            + "    f_blob               BYTEA,\n"
                            + "    f_long_varbinary     BYTEA,\n"
                            + "    f_longblob           BYTEA,\n"
                            + "    f_tinyblob           BYTEA,\n"
                            + "    f_varbinary          VARCHAR(100),\n"
                            + "    f_smallint           SMALLINT,\n"
                            + "    f_smallint_unsigned  INTEGER,\n"
                            + "    f_mediumint          INTEGER,\n"
                            + "    f_mediumint_unsigned INTEGER,\n"
                            + "    f_int                INTEGER,\n"
                            + "    f_int_unsigned       INTEGER,\n"
                            + "    f_integer            INTEGER,\n"
                            + "    f_integer_unsigned   INTEGER,\n"
                            + "    f_bigint             BIGINT,\n"
                            + "    f_bigint_unsigned    BIGINT,\n"
                            + "    f_numeric            DECIMAL,\n"
                            + "    f_decimal            DECIMAL,\n"
                            + "    f_float              REAL,\n"
                            + "    f_double             DOUBLE PRECISION,\n"
                            + "    f_double_precision   DOUBLE PRECISION,\n"
                            + "    f_longtext           TEXT,\n"
                            + "    f_mediumtext         TEXT,\n"
                            + "    f_text               TEXT,\n"
                            + "    f_tinytext           TEXT,\n"
                            + "    f_varchar            VARCHAR(100),\n"
                            + "    f_date               DATE,\n"
                            + "    f_datetime           TIMESTAMP,\n"
                            + "    f_timestamp          TIMESTAMP,\n"
                            + "    f_bit1               boolean,\n"
                            + "    f_bit64              SMALLINT,\n"
                            + "    f_char               CHAR,\n"
                            + "    f_enum               VARCHAR(10),\n"
                            + "    f_mediumblob         BYTEA,\n"
                            + "    f_long_varchar       TEXT,\n"
                            + "    f_real               REAL,\n"
                            + "    f_time               TIME,\n"
                            + "    f_tinyint            SMALLINT,\n"
                            + "    f_tinyint_unsigned   SMALLINT,\n"
                            + "    f_json               VARCHAR(100),\n"
                            + "    f_year               INTEGER\n"
                            + ");\n";
            statement.execute(sink);
            statement.execute(sink2);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing PostgreSql table failed!", e);
        }
    }

    // Initialize ogg data to kafka
    private void initLocalDataToKafka() {
        String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
        Properties props = new Properties();

        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (String localPath : LOCAL_DATA_TO_KAFKA_MAPPING.keySet()) {
            String kafkaTopic = LOCAL_DATA_TO_KAFKA_MAPPING.get(localPath);
            InputStream inputStream = KafkaFormatIT.class.getResourceAsStream(localPath);
            if (inputStream != null) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        ProducerRecord<String, String> record =
                                new ProducerRecord<>(kafkaTopic, null, line);
                        producer.send(record).get();
                    }
                } catch (IOException | InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
        producer.close();
    }

    // Get result data
    private List<List<Object>> getPostgreSinkTableList(String tableName) {
        List<List<Object>> actual = new ArrayList<>();
        try (Connection connection =
                DriverManager.getConnection(
                        POSTGRESQL_CONTAINER.getJdbcUrl(),
                        POSTGRESQL_CONTAINER.getUsername(),
                        POSTGRESQL_CONTAINER.getPassword())) {
            try (Statement statement = connection.createStatement();
                    ResultSet resultSet =
                            statement.executeQuery("select * from " + tableName + " order by id")) {
                PostgresJdbcRowConverter postgresJdbcRowConverter = new PostgresJdbcRowConverter();
                while (resultSet.next()) {
                    SeaTunnelRow row =
                            postgresJdbcRowConverter.toInternal(
                                    resultSet, sinkTables.get(tableName).getTableSchema());
                    actual.add(Arrays.asList(row.getFields()));
                }
            }
            // truncate e2e sink table
            try (Statement statement = connection.createStatement()) {
                statement.execute("truncate table " + tableName);
                LOG.info("truncate table sink");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return actual;
    }

    @Override
    public void tearDown() {
        if (KAFKA_CONTAINER != null) {
            KAFKA_CONTAINER.close();
        }
        if (POSTGRESQL_CONTAINER != null) {
            POSTGRESQL_CONTAINER.close();
        }
    }
}
