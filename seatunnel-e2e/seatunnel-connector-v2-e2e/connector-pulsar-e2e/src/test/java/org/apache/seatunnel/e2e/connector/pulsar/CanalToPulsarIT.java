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

package org.apache.seatunnel.e2e.connector.pulsar;

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

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

/**
 * canal server producer data to pulsar, st-cdc is consumer reference:
 * https://pulsar.apache.org/docs/2.11.x/io-canal-source/
 */
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "spark would ignore delete type")
public class CanalToPulsarIT extends TestSuiteBase implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(CanalToPulsarIT.class);

    // ----------------------------------------------------------------------------
    // mysql
    private static final String MYSQL_HOST = "mysql.e2e";

    private static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER = "st_user";
    public static final String MYSQL_PASSWORD = "seatunnel";

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V5_7);

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "canal", "mysqluser", "mysqlpw");

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        MySqlContainer mySqlContainer =
                new MySqlContainer(version)
                        .withConfigurationOverride("mysql/server-gtids/my.cnf")
                        .withSetupSQL("mysql/setup.sql")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_HOST)
                        .withDatabaseName("canal")
                        .withUsername(MYSQL_USER)
                        .withPassword(MYSQL_PASSWORD)
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
        mySqlContainer.withExposedPorts(MYSQL_PORT);
        return mySqlContainer;
    }

    // ----------------------------------------------------------------------------
    // postgres
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

    private void createPostgreSQLContainer() throws ClassNotFoundException {
        POSTGRESQL_CONTAINER =
                new PostgreSQLContainer<>(DockerImageName.parse(PG_IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("postgresql")
                        .withExposedPorts(5432)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));
    }

    private void initializeJdbcTable() {
        try (Connection connection =
                DriverManager.getConnection(
                        POSTGRESQL_CONTAINER.getJdbcUrl(),
                        POSTGRESQL_CONTAINER.getUsername(),
                        POSTGRESQL_CONTAINER.getPassword())) {
            Statement statement = connection.createStatement();
            String sink =
                    "create table sink(\n"
                            + "id INT NOT NULL PRIMARY KEY,\n"
                            + "name varchar(255),\n"
                            + "description varchar(255),\n"
                            + "weight varchar(255)"
                            + ")";
            statement.execute(sink);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing PostgreSql table failed!", e);
        }
    }

    // ----------------------------------------------------------------------------
    // canal
    private static GenericContainer<?> CANAL_CONTAINER;

    private static final String CANAL_DOCKER_IMAGE = "canal/canal-server:v1.1.2";

    private static final String CANAL_HOST = "canal.e2e";

    private void createCanalContainer() {
        CANAL_CONTAINER =
                new GenericContainer<>(CANAL_DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(CANAL_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(CANAL_DOCKER_IMAGE)));

        CANAL_CONTAINER
                .withEnv("canal.auto.scan", "false")
                .withEnv("canal.destinations", "test")
                .withEnv(
                        "canal.instance.master.address",
                        String.format("%s:%s", MYSQL_HOST, MYSQL_PORT))
                .withEnv("canal.instance.dbUsername", MYSQL_USER)
                .withEnv("canal.instance.dbPassword", MYSQL_PASSWORD)
                .withEnv("canal.instance.connectionCharset", "UTF-8")
                .withEnv("canal.instance.tsdb.enable", "true")
                .withEnv("canal.instance.gtidon", "false");
    }

    // ----------------------------------------------------------------------------
    // pulsar container
    // download canal connector is so slowly,make it with canal connector from apache/pulsar
    private static final String PULSAR_IMAGE_NAME = "laglangyue/pulsar_canal:2.3.1";

    private static final String PULSAR_HOST = "pulsar.e2e";
    private static final String TOPIC = "test-cdc_mds";

    private static final Integer PULSAR_BROKER_PORT = 6650;
    private static final Integer PULSAR_BROKER_HTTP_PORT = 8080;

    private static GenericContainer<?> PULSAR_CONTAINER;

    private void createPulsarContainer() {
        PULSAR_CONTAINER =
                new GenericContainer<>(DockerImageName.parse(PULSAR_IMAGE_NAME))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(PULSAR_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(PULSAR_IMAGE_NAME)));
        PULSAR_CONTAINER.withExposedPorts(PULSAR_BROKER_PORT, PULSAR_BROKER_HTTP_PORT);

        // canal connectors config
        PULSAR_CONTAINER.withCopyFileToContainer(
                MountableFile.forClasspathResource("pulsar/canal-mysql-source-config.yaml"),
                "/pulsar/conf/");
        // start connectors cmd
        PULSAR_CONTAINER.withCopyFileToContainer(
                MountableFile.forClasspathResource("pulsar/start_canal_connector.sh"), "/pulsar/");
        // wait for pulsar started
        List<WaitStrategy> waitStrategies = new ArrayList<>();
        waitStrategies.add(Wait.forLogMessage(".*pulsar entered RUNNING state.*", 1));
        waitStrategies.add(Wait.forLogMessage(".*canal entered RUNNING state.*", 1));
        final WaitAllStrategy compoundedWaitStrategy = new WaitAllStrategy();
        waitStrategies.forEach(compoundedWaitStrategy::withStrategy);
        PULSAR_CONTAINER.waitingFor(compoundedWaitStrategy);
    }

    private void waitForTopicCreated() throws PulsarClientException {
        try (PulsarAdmin pulsarAdmin =
                PulsarAdmin.builder()
                        .serviceHttpUrl(
                                String.format(
                                        "http://%s:%s",
                                        PULSAR_CONTAINER.getHost(),
                                        PULSAR_CONTAINER.getMappedPort(PULSAR_BROKER_HTTP_PORT)))
                        .build()) {
            while (true) {
                try {
                    List<String> topics = pulsarAdmin.topics().getList("public/default");
                    if (topics.stream().anyMatch(t -> StringUtils.contains(t, TOPIC))) {
                        break;
                    }
                    Thread.sleep(5000);
                } catch (PulsarAdminException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @BeforeAll
    @Override
    public void startUp() throws ClassNotFoundException, InterruptedException {
        LOG.info("The second stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Mysql Containers are started");

        LOG.info("The third stage: Starting Canal containers...");
        createCanalContainer();
        Startables.deepStart(Stream.of(CANAL_CONTAINER)).join();
        LOG.info("Canal Containers are started");

        LOG.info("Starting Pulsar containers...");
        createPulsarContainer();
        Startables.deepStart(Stream.of(PULSAR_CONTAINER)).join();
        LOG.info("Pulsar Containers are started");
        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(5, TimeUnit.MINUTES)
                .untilAsserted(this::waitForTopicCreated);
        // before ddl, the pulsar_canal connector should be started
        inventoryDatabase.createAndInitialize();
        // wait pulsar get data from canal server
        Thread.sleep(10 * 1000);
        LOG.info("The fourth stage: Starting PostgresSQL container...");
        createPostgreSQLContainer();
        Startables.deepStart(Stream.of(POSTGRESQL_CONTAINER)).join();
        Class.forName(POSTGRESQL_CONTAINER.getDriverClassName());
        LOG.info("postgresql Containers are started");
        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(5, TimeUnit.MINUTES)
                .untilAsserted(this::initializeJdbcTable);
    }

    @Override
    public void tearDown() {
        MYSQL_CONTAINER.close();
        CANAL_CONTAINER.close();
        PULSAR_CONTAINER.close();
    }

    @TestTemplate
    void testCanalFormatMessages(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult = container.executeJob("/cdc_canal_pulsar_to_pg.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        List<List<Object>> actual = new ArrayList<>();
        try (Connection connection =
                DriverManager.getConnection(
                        POSTGRESQL_CONTAINER.getJdbcUrl(),
                        POSTGRESQL_CONTAINER.getUsername(),
                        POSTGRESQL_CONTAINER.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("SELECT * FROM sink ORDER BY id");
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
        List<List<Object>> expected =
                Lists.newArrayList(
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
                        Arrays.asList(108, "jacket", "water resistent black wind breaker", "0.1"));
        Assertions.assertIterableEquals(expected, actual);

        try (Connection connection =
                DriverManager.getConnection(
                        POSTGRESQL_CONTAINER.getJdbcUrl(),
                        POSTGRESQL_CONTAINER.getUsername(),
                        POSTGRESQL_CONTAINER.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("truncate table sink");
                LOG.info("testSinkCDCChangelog truncate table sink");
            }
        }
    }
}
