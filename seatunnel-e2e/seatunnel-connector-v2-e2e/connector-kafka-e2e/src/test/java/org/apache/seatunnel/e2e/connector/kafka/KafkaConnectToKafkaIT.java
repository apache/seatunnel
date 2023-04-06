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

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@Slf4j
public class KafkaConnectToKafkaIT extends TestSuiteBase implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectToKafkaIT.class);

    // kafka
    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:latest";

    private static final String KAFKA_DEBEZIUM_TOPIC = "debezium_source_record";
    private static final String KAFKA_JDBC_TOPIC = "jdbc_source_record";

    private static final int KAFKA_PORT = 9093;

    private static final String KAFKA_HOST = "kafkaCluster";

    private static KafkaContainer KAFKA_CONTAINER;

    private KafkaProducer<byte[], byte[]> kafkaProducer;

    // ----------------------------------------------------------------------------
    // mysql
    private static final String MYSQL_HOST = "mysql_e2e";

    private static final int MYSQL_PORT = 3306;

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "seatunnel", "mysqluser", "mysqlpw");

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        MySqlContainer mySqlContainer =
                new MySqlContainer(version)
                        .withConfigurationOverride("docker/server-gtids/my.cnf")
                        .withSetupSQL("docker/setup.sql")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_HOST)
                        .withDatabaseName("seatunnel")
                        .withUsername("st_user")
                        .withPassword("seatunnel")
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
        mySqlContainer.setPortBindings(
                com.google.common.collect.Lists.newArrayList(
                        String.format("%s:%s", MYSQL_PORT, MYSQL_PORT)));
        return mySqlContainer;
    }

    private void createKafkaContainer() {
        KAFKA_CONTAINER =
                new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(KAFKA_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(KAFKA_IMAGE_NAME)));
        KAFKA_CONTAINER.setPortBindings(
                com.google.common.collect.Lists.newArrayList(
                        String.format("%s:%s", KAFKA_PORT, KAFKA_PORT)));
    }

    @BeforeAll
    @Override
    public void startUp() throws ClassNotFoundException {

        LOG.info("The first stage: Starting Kafka containers...");
        createKafkaContainer();
        Startables.deepStart(Stream.of(KAFKA_CONTAINER)).join();
        LOG.info("Kafka Containers are started");

        LOG.info("The second stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Mysql Containers are started");

        given().ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::initKafkaProducer);

        log.info("Write 7 records to topic " + KAFKA_DEBEZIUM_TOPIC);
        generateConnectDebeziumRecord();
        log.info("Write 3 records to topic " + KAFKA_JDBC_TOPIC);
        generateConnectJdbcRecord();
        inventoryDatabase.createAndInitialize();
    }

    @TestTemplate
    public void testJdbcRecordKafkaToMysql(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult =
                container.executeJob("/kafkasource_jdbc_record_to_mysql.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        Set<List<Object>> actual = new HashSet<>();
        try (Connection connection =
                DriverManager.getConnection(
                        MYSQL_CONTAINER.getJdbcUrl(),
                        MYSQL_CONTAINER.getUsername(),
                        MYSQL_CONTAINER.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("select * from jdbc_sink");
                while (resultSet.next()) {
                    List<Object> row =
                            Arrays.asList(
                                    resultSet.getInt("id"),
                                    resultSet.getString("name"),
                                    resultSet.getString("description"),
                                    resultSet.getString("weight"));
                    actual.add(row);
                }
            }
        }
        Set<List<Object>> expected =
                Stream.<List<Object>>of(
                                Arrays.asList(15, "test", "test", "20"),
                                Arrays.asList(16, "test-001", "test", "30"),
                                Arrays.asList(18, "sdc", "sdc", "sdc"))
                        .collect(Collectors.toSet());
        Assertions.assertIterableEquals(expected, actual);
    }

    @TestTemplate
    public void testDebeziumRecordKafkaToMysql(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult =
                container.executeJob("/kafkasource_debezium_record_to_mysql.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        Set<List<Object>> actual = new HashSet<>();
        try (Connection connection =
                DriverManager.getConnection(
                        MYSQL_CONTAINER.getJdbcUrl(),
                        MYSQL_CONTAINER.getUsername(),
                        MYSQL_CONTAINER.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("select * from debezium_sink");
                while (resultSet.next()) {
                    List<Object> row =
                            Arrays.asList(
                                    resultSet.getInt("id"),
                                    resultSet.getString("name"),
                                    resultSet.getString("description"),
                                    resultSet.getString("weight"));
                    actual.add(row);
                }
            }
        }
        Set<List<Object>> expected =
                Stream.<List<Object>>of(
                                Arrays.asList(15, "test", "test", "20"),
                                Arrays.asList(16, "test-001", "test", "30"),
                                Arrays.asList(18, "sdc", "sdc", "sdc"))
                        .collect(Collectors.toSet());
        Assertions.assertIterableEquals(expected, actual);
    }

    public void generateConnectDebeziumRecord() {
        String[] debeziumSourceRecords = {
            // Read id=15,16
            "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"string\",\"optional\":true,\"field\":\"description\"},{\"type\":\"string\",\"optional\":true,\"field\":\"weight\"}],\"optional\":true,\"name\":\"kafkaconnect_debezium_record.test_database_001.seatunnel_test_cdc.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"string\",\"optional\":true,\"field\":\"description\"},{\"type\":\"string\",\"optional\":true,\"field\":\"weight\"}],\"optional\":true,\"name\":\"kafkaconnect_debezium_record.test_database_001.seatunnel_test_cdc.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false,incremental\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"kafkaconnect_debezium_record.test_database_001.seatunnel_test_cdc.Envelope\"},\"payload\":{\"before\":null,\"after\":{\"id\":15,\"name\":\"test\",\"description\":\"test\",\"weight\":\"20\"},\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"debezium-mysql-source\",\"ts_ms\":1680838002000,\"snapshot\":\"true\",\"db\":\"test_database_001\",\"sequence\":null,\"table\":\"seatunnel_test_cdc\",\"server_id\":0,\"gtid\":null,\"file\":\"mysql-bin.007009\",\"pos\":343976613,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"r\",\"ts_ms\":1680838002399,\"transaction\":null}}",
            "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"string\",\"optional\":true,\"field\":\"description\"},{\"type\":\"string\",\"optional\":true,\"field\":\"weight\"}],\"optional\":true,\"name\":\"kafkaconnect_debezium_record.test_database_001.seatunnel_test_cdc.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"string\",\"optional\":true,\"field\":\"description\"},{\"type\":\"string\",\"optional\":true,\"field\":\"weight\"}],\"optional\":true,\"name\":\"kafkaconnect_debezium_record.test_database_001.seatunnel_test_cdc.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false,incremental\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"kafkaconnect_debezium_record.test_database_001.seatunnel_test_cdc.Envelope\"},\"payload\":{\"before\":null,\"after\":{\"id\":16,\"name\":\"test-001\",\"description\":\"xoaoyi 001\",\"weight\":\"30\"},\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"debezium-mysql-source\",\"ts_ms\":1680838002000,\"snapshot\":\"last\",\"db\":\"test_database_001\",\"sequence\":null,\"table\":\"seatunnel_test_cdc\",\"server_id\":0,\"gtid\":null,\"file\":\"mysql-bin.007009\",\"pos\":343976613,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"r\",\"ts_ms\":1680838002402,\"transaction\":null}}",
            // Create id=17
            "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"string\",\"optional\":true,\"field\":\"description\"},{\"type\":\"string\",\"optional\":true,\"field\":\"weight\"}],\"optional\":true,\"name\":\"kafkaconnect_debezium_record.test_database_001.seatunnel_test_cdc.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"string\",\"optional\":true,\"field\":\"description\"},{\"type\":\"string\",\"optional\":true,\"field\":\"weight\"}],\"optional\":true,\"name\":\"kafkaconnect_debezium_record.test_database_001.seatunnel_test_cdc.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false,incremental\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"kafkaconnect_debezium_record.test_database_001.seatunnel_test_cdc.Envelope\"},\"payload\":{\"before\":null,\"after\":{\"id\":17,\"name\":null,\"description\":null,\"weight\":null},\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"debezium-mysql-source\",\"ts_ms\":1680839484000,\"snapshot\":\"false\",\"db\":\"test_database_001\",\"sequence\":null,\"table\":\"seatunnel_test_cdc\",\"server_id\":1075402662,\"gtid\":\"d9f55c47-4d67-11ed-9b11-0c42a1da5d36:51548820\",\"file\":\"mysql-bin.007010\",\"pos\":88497283,\"row\":0,\"thread\":3948562,\"query\":null},\"op\":\"c\",\"ts_ms\":1680839484160,\"transaction\":null}}",
            // Update id =17
            "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"string\",\"optional\":true,\"field\":\"description\"},{\"type\":\"string\",\"optional\":true,\"field\":\"weight\"}],\"optional\":true,\"name\":\"kafkaconnect_debezium_record.test_database_001.seatunnel_test_cdc.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"string\",\"optional\":true,\"field\":\"description\"},{\"type\":\"string\",\"optional\":true,\"field\":\"weight\"}],\"optional\":true,\"name\":\"kafkaconnect_debezium_record.test_database_001.seatunnel_test_cdc.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false,incremental\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"kafkaconnect_debezium_record.test_database_001.seatunnel_test_cdc.Envelope\"},\"payload\":{\"before\":{\"id\":17,\"name\":null,\"description\":null,\"weight\":null},\"after\":{\"id\":17,\"name\":\"adsd\",\"description\":\"asca\",\"weight\":\"50\"},\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"debezium-mysql-source\",\"ts_ms\":1680839490000,\"snapshot\":\"false\",\"db\":\"test_database_001\",\"sequence\":null,\"table\":\"seatunnel_test_cdc\",\"server_id\":1075402662,\"gtid\":\"d9f55c47-4d67-11ed-9b11-0c42a1da5d36:51548869\",\"file\":\"mysql-bin.007010\",\"pos\":89426600,\"row\":0,\"thread\":3948562,\"query\":null},\"op\":\"u\",\"ts_ms\":1680839490454,\"transaction\":null}}",
            // Create id=18
            "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"string\",\"optional\":true,\"field\":\"description\"},{\"type\":\"string\",\"optional\":true,\"field\":\"weight\"}],\"optional\":true,\"name\":\"kafkaconnect_debezium_record.test_database_001.seatunnel_test_cdc.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"string\",\"optional\":true,\"field\":\"description\"},{\"type\":\"string\",\"optional\":true,\"field\":\"weight\"}],\"optional\":true,\"name\":\"kafkaconnect_debezium_record.test_database_001.seatunnel_test_cdc.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false,incremental\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"kafkaconnect_debezium_record.test_database_001.seatunnel_test_cdc.Envelope\"},\"payload\":{\"before\":null,\"after\":{\"id\":18,\"name\":null,\"description\":null,\"weight\":null},\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"debezium-mysql-source\",\"ts_ms\":1680851305000,\"snapshot\":\"false\",\"db\":\"test_database_001\",\"sequence\":null,\"table\":\"seatunnel_test_cdc\",\"server_id\":1075402662,\"gtid\":\"d9f55c47-4d67-11ed-9b11-0c42a1da5d36:51702047\",\"file\":\"mysql-bin.007014\",\"pos\":113909238,\"row\":0,\"thread\":3957850,\"query\":null},\"op\":\"c\",\"ts_ms\":1680851304973,\"transaction\":null}}",
            // Update id=18
            "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"string\",\"optional\":true,\"field\":\"description\"},{\"type\":\"string\",\"optional\":true,\"field\":\"weight\"}],\"optional\":true,\"name\":\"kafkaconnect_debezium_record.test_database_001.seatunnel_test_cdc.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"string\",\"optional\":true,\"field\":\"description\"},{\"type\":\"string\",\"optional\":true,\"field\":\"weight\"}],\"optional\":true,\"name\":\"kafkaconnect_debezium_record.test_database_001.seatunnel_test_cdc.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false,incremental\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"kafkaconnect_debezium_record.test_database_001.seatunnel_test_cdc.Envelope\"},\"payload\":{\"before\":{\"id\":18,\"name\":null,\"description\":null,\"weight\":null},\"after\":{\"id\":18,\"name\":\"sdc\",\"description\":\"sdc\",\"weight\":\"sdc\"},\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"debezium-mysql-source\",\"ts_ms\":1680851309000,\"snapshot\":\"false\",\"db\":\"test_database_001\",\"sequence\":null,\"table\":\"seatunnel_test_cdc\",\"server_id\":1075402662,\"gtid\":\"d9f55c47-4d67-11ed-9b11-0c42a1da5d36:51702084\",\"file\":\"mysql-bin.007014\",\"pos\":114651119,\"row\":0,\"thread\":3957850,\"query\":null},\"op\":\"u\",\"ts_ms\":1680851309193,\"transaction\":null}}",
            // delete id=17
            "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"string\",\"optional\":true,\"field\":\"description\"},{\"type\":\"string\",\"optional\":true,\"field\":\"weight\"}],\"optional\":true,\"name\":\"kafkaconnect_debezium_record.test_database_001.seatunnel_test_cdc.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"string\",\"optional\":true,\"field\":\"description\"},{\"type\":\"string\",\"optional\":true,\"field\":\"weight\"}],\"optional\":true,\"name\":\"kafkaconnect_debezium_record.test_database_001.seatunnel_test_cdc.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false,incremental\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"kafkaconnect_debezium_record.test_database_001.seatunnel_test_cdc.Envelope\"},\"payload\":{\"before\":{\"id\":17,\"name\":\"adsd\",\"description\":\"asca\",\"weight\":\"50\"},\"after\":null,\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\",\"name\":\"debezium-mysql-source\",\"ts_ms\":1680851317000,\"snapshot\":\"false\",\"db\":\"test_database_001\",\"sequence\":null,\"table\":\"seatunnel_test_cdc\",\"server_id\":1075402662,\"gtid\":\"d9f55c47-4d67-11ed-9b11-0c42a1da5d36:51702158\",\"file\":\"mysql-bin.007014\",\"pos\":115976082,\"row\":0,\"thread\":3957850,\"query\":null},\"op\":\"d\",\"ts_ms\":1680851317443,\"transaction\":null}}"
        };
        for (String value : debeziumSourceRecords) {
            kafkaProducer.send(new ProducerRecord<>(KAFKA_DEBEZIUM_TOPIC, value.getBytes()));
        }
    }

    public void generateConnectJdbcRecord() {
        String[] jdbcSourceRecords = {
            "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"string\",\"optional\":true,\"field\":\"description\"},{\"type\":\"string\",\"optional\":true,\"field\":\"weight\"}],\"optional\":false,\"name\":\"test_database_001.seatunnel_test_cdc\"},\"payload\":{\"id\":15,\"name\":\"test\",\"description\":\"test\",\"weight\":\"20\"}}",
            "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"string\",\"optional\":true,\"field\":\"description\"},{\"type\":\"string\",\"optional\":true,\"field\":\"weight\"}],\"optional\":false,\"name\":\"test_database_001.seatunnel_test_cdc\"},\"payload\":{\"id\":16,\"name\":\"test-001\",\"description\":\"xoaoyi 001\",\"weight\":\"30\"}}",
            "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"string\",\"optional\":true,\"field\":\"description\"},{\"type\":\"string\",\"optional\":true,\"field\":\"weight\"}],\"optional\":false,\"name\":\"test_database_001.seatunnel_test_cdc\"},\"payload\":{\"id\":18,\"name\":\"sdc\",\"description\":\"sdc\",\"weight\":\"sdc\"}}"
        };
        for (String value : jdbcSourceRecords) {
            kafkaProducer.send(new ProducerRecord<>(KAFKA_JDBC_TOPIC, value.getBytes()));
        }
    }

    private void initKafkaProducer() {
        Properties props = new Properties();
        String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        kafkaProducer = new KafkaProducer<>(props);
    }

    @Override
    public void tearDown() {
        MYSQL_CONTAINER.close();
        KAFKA_CONTAINER.close();
    }
}
