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

import static org.awaitility.Awaitility.given;

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

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
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@DisabledOnContainer(value = {}, type = {EngineType.FLINK, EngineType.SPARK})
public class CannalToKafakIT extends TestSuiteBase implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(CannalToKafakIT.class);

    private static GenericContainer<?> CANAL_CONTAINER;

    private static final String CANAL_DOCKER_IMAGE = "chinayin/canal:1.1.6";

    private static final String CANAL_HOST = "canal_e2e";

    private static final int CANAL_PORT = 11111;

    //----------------------------------------------------------------------------
    // kafka
    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:latest";

    private static final String KAFKA_TOPIC = "test-canal-sink";

    private static final int KAFKA_PORT = 9093;

    private static final String KAFKA_HOST = "kafkaCluster";

    private static KafkaContainer KAFKA_CONTAINER;

    private KafkaConsumer<String, String> kafkaConsumer;

    //----------------------------------------------------------------------------
    // mysql
    private static final String MYSQL_HOST = "mysql_e2e";

    private static final int MYSQL_PORT = 3306;

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase inventoryDatabase =
        new UniqueDatabase(MYSQL_CONTAINER, "canal", "mysqluser", "mysqlpw");

    //----------------------------------------------------------------------------
    // postgres
    private static final String PG_IMAGE = "postgres:alpine3.16";
    private static final String PG_DRIVER_JAR = "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar";
    private PostgreSQLContainer<?> postgreSQLContainer;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory = container -> {
        Container.ExecResult extraCommands = container.execInContainer("bash", "-c", "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O " + PG_DRIVER_JAR);
        Assertions.assertEquals(0, extraCommands.getExitCode());
    };

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        MySqlContainer mySqlContainer = new MySqlContainer(version)
            .withConfigurationOverride("docker/server-gtids/my.cnf")
            .withSetupSQL("docker/setup.sql")
            .withNetwork(NETWORK)
            .withNetworkAliases(MYSQL_HOST)
            .withDatabaseName("canal")
            .withUsername("st_user")
            .withPassword("seatunnel")
            .withLogConsumer(new Slf4jLogConsumer(LOG));
        mySqlContainer.setPortBindings(com.google.common.collect.Lists.newArrayList(
            String.format("%s:%s", MYSQL_PORT, MYSQL_PORT)));
        return mySqlContainer;
    }

    private void createCanalContainer() {
        CANAL_CONTAINER = new GenericContainer<>(CANAL_DOCKER_IMAGE)
            .withCopyFileToContainer(MountableFile.forClasspathResource("canal/canal.properties"), "/app/server/conf/canal.properties")
            .withCopyFileToContainer(MountableFile.forClasspathResource("canal/instance.properties"), "/app/server/conf/example/instance.properties")
            .withNetwork(NETWORK)
            .withNetworkAliases(CANAL_HOST)
            .withCommand()
            .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(CANAL_DOCKER_IMAGE)));
        CANAL_CONTAINER.setPortBindings(com.google.common.collect.Lists.newArrayList(
            String.format("%s:%s", CANAL_PORT, CANAL_PORT)));
    }

    private void createKafkaContainer(){
        KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME))
            .withNetwork(NETWORK)
            .withNetworkAliases(KAFKA_HOST)
            .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(KAFKA_IMAGE_NAME)));
        KAFKA_CONTAINER.setPortBindings(com.google.common.collect.Lists.newArrayList(
            String.format("%s:%s", KAFKA_PORT, KAFKA_PORT)));
    }

    @BeforeAll
    @Override
    public void startUp() throws ClassNotFoundException {

        LOG.info("The third stage: Starting Kafka containers...");
        createKafkaContainer();
        Startables.deepStart(Stream.of(KAFKA_CONTAINER)).join();
        LOG.info("Containers are started");

        LOG.info("The first stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Containers are started");

        LOG.info("The first stage: Starting Canal containers...");
        createCanalContainer();
        Startables.deepStart(Stream.of(CANAL_CONTAINER)).join();
        LOG.info("Containers are started");

        postgreSQLContainer = new PostgreSQLContainer<>(DockerImageName.parse(PG_IMAGE))
                .withNetwork(NETWORK)
                .withNetworkAliases("postgresql")
                .withExposedPorts(5432)
                .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));
        Startables.deepStart(Stream.of(postgreSQLContainer)).join();
        LOG.info("PostgreSQL container started");
        Class.forName(postgreSQLContainer.getDriverClassName());
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
    }

    @TestTemplate
    public void testCannalToKafakCannalFormatAnalysis2(TestContainer container) throws IOException, InterruptedException, SQLException {
        inventoryDatabase.createAndInitialize();
        Container.ExecResult execResult = container.executeJob("/kafkasource_canal_cdc_to_pgsql.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        Set<List<Object>> actual = new HashSet<>();
        try (Connection connection = DriverManager.getConnection(postgreSQLContainer.getJdbcUrl(),
                postgreSQLContainer.getUsername(), postgreSQLContainer.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("select * from sink");
                while (resultSet.next()) {
                    List<Object> row = Arrays.asList(
                            resultSet.getInt("id"),
                            resultSet.getString("name"),
                            resultSet.getString("description"),
                            resultSet.getString("weight")
                    );
                    actual.add(row);
                }
            }
        }
        Set<List<Object>> expected = Stream.<List<Object>>of(
                        Arrays.asList(102, "car battery", "12V car battery", "8.1"),
                        Arrays.asList(103, "12-pack drill bits", "12-pack of drill bits with sizes ranging from #40 to #3", "0.8"),
                        Arrays.asList(104, "hammer", "12oz carpenter's hammer", "0.75"),
                        Arrays.asList(105, "hammer", "14oz carpenter's hammer", "0.875"),
                        Arrays.asList(106, "hammer", "16oz carpenter's hammer", "1.0"),
                        Arrays.asList(108, "jacket", "water resistent black wind breaker", "0.1"),
                        Arrays.asList(101, "scooter", "Small 2-wheel scooter", "4.56"),
                        Arrays.asList(107, "rocks", "box of assorted rocks", "7.88")
                        ).collect(Collectors.toSet());
        Assertions.assertIterableEquals(expected, actual);
    }

    private void initKafkaConsumer() {
        Properties prop = new Properties();
        String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "CONF");
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        kafkaConsumer = new KafkaConsumer<>(prop);
    }

    private void initializeJdbcTable() {
        try (Connection connection = DriverManager.getConnection(postgreSQLContainer.getJdbcUrl(),
                postgreSQLContainer.getUsername(), postgreSQLContainer.getPassword())) {
            Statement statement = connection.createStatement();
            String sink = "create table sink(\n" +
                    "id INT NOT NULL PRIMARY KEY,\n" +
                    "name varchar(255),\n" +
                    "description varchar(255),\n" +
                    "weight varchar(255)" +
                    ")";
            statement.execute(sink);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing PostgreSql table failed!", e);
        }
    }

    @Override
    public void tearDown(){
        MYSQL_CONTAINER.close();
        KAFKA_CONTAINER.close();
        CANAL_CONTAINER.close();
    }
}
