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

package org.apache.seatunnel.e2e.connector.hive;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.FLINK, EngineType.SPARK},
        disabledReason = "")
public class HiveSaveModeIT extends TestSuiteBase implements TestResource {

    private static final String HIVE_IMAGE = "apache/hive:3.1.3";
    private static final int THRIFT_PORT = 9083;
    private static final int JDBC_PORT = 10000;
    private GenericContainer metastore;
    private GenericContainer hive2;
    private String jdbcUrl;
    private String hmsUrl;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                // The jar of hive-exec
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "sh",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Hive/lib && cd /tmp/seatunnel/plugins/Hive/lib "
                                        + "&& wget https://repo1.maven.org/maven2/org/apache/hive/hive-exec/3.1.3/hive-exec-3.1.3.jar "
                                        + "&& wget https://repo1.maven.org/maven2/org/apache/hive/hive-service/3.1.3/hive-service-3.1.3.jar "
                                        + "&& wget https://repo1.maven.org/maven2/org/apache/hive/hive-jdbc/3.1.3/hive-jdbc-3.1.3.jar "
                                        + "&& wget https://repo1.maven.org/maven2/org/apache/thrift/libfb303/0.9.3/libfb303-0.9.3.jar");
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        metastore =
                new GenericContainer<>(HIVE_IMAGE)
                        .withExposedPorts(THRIFT_PORT)
                        .withNetwork(NETWORK)
                        .withNetworkAliases("metastore")
                        .withEnv("SERVICE_NAME", "metastore")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(HIVE_IMAGE)));
        metastore.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", THRIFT_PORT, THRIFT_PORT)));
        Startables.deepStart(Stream.of(metastore)).join();

        hive2 =
                new GenericContainer<>(HIVE_IMAGE)
                        .withExposedPorts(JDBC_PORT)
                        .withNetwork(NETWORK)
                        .withNetworkAliases("hiveserver2")
                        .withEnv("SERVICE_NAME", "hiveserver2")
                        .withEnv(
                                "SERVICE_OPTS",
                                "-Dhive.metastore.uris=thrift://"
                                        + InetAddress.getLocalHost().getHostAddress()
                                        + ":9083")
                        .withEnv("IS_RESUME", "true")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(HIVE_IMAGE)))
                        .waitingFor(
                                Wait.forListeningPort()
                                        .withStartupTimeout(Duration.ofSeconds(180)));
        hive2.setPortBindings(Lists.newArrayList(String.format("%s:%s", JDBC_PORT, JDBC_PORT)));
        Startables.deepStart(Stream.of(hive2)).join();
        createTable(
                InetAddress.getLocalHost().getHostAddress(),
                "10000",
                "default",
                "hive_jdbc_example2");
        changeConnectionURLConf("src/test/resources/savemode/fake_to_hive_create_table.conf");
        changeConnectionURLConf("src/test/resources/savemode/fake_to_hive_re_create_table.conf");
        changeConnectionURLConf("src/test/resources/savemode/fake_to_hive_table_not_exist.conf");
        changeConnectionURLConf("src/test/resources/savemode/hive1_to_assert.conf");
        changeConnectionURLConf("src/test/resources/savemode/hive2_to_assert.conf");
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (metastore != null) {
            metastore.close();
        }
        if (hive2 != null) {
            hive2.close();
        }
    }

    private void createTable(String host, String port, String db, String tableName)
            throws SQLException {
        String jdbcUrl = "jdbc:hive2://" + host + ":" + port + "/" + db;
        String ddl =
                "CREATE TABLE "
                        + tableName
                        + "("
                        + "    int_column              INT,"
                        + "    integer_column          INTEGER,"
                        + "    bigint_column           BIGINT,"
                        + "    smallint_column         SMALLINT,"
                        + "    tinyint_column          TINYINT,"
                        + "    double_column           DOUBLE,"
                        + "    double_PRECISION_column DOUBLE PRECISION,"
                        + "    float_column            FLOAT,"
                        + "    string_column           STRING,"
                        + "    char_column             CHAR(10),"
                        + "    varchar_column          VARCHAR(20),"
                        + "    boolean_column          BOOLEAN,"
                        + "    date_column             DATE,"
                        + "    timestamp_column        TIMESTAMP,"
                        + "    decimal_column          DECIMAL(10, 2),"
                        + "    numeric_column          NUMERIC(10, 2)"
                        + ")";
        Connection connection = DriverManager.getConnection(jdbcUrl);
        Statement statement = connection.createStatement();
        statement.execute(ddl);
    }

    private void changeConnectionURLConf(String resourceFilePath) throws UnknownHostException {
        jdbcUrl = "jdbc:hive2://" + InetAddress.getLocalHost().getHostAddress() + ":10000/default";
        hmsUrl = "thrift://" + InetAddress.getLocalHost().getHostAddress() + ":9083";
        Path path = Paths.get(resourceFilePath);
        try {
            List<String> lines = Files.readAllLines(path);
            List<String> newLines =
                    lines.stream()
                            .map(
                                    line -> {
                                        if (line.contains("hive_jdbc_url")) {
                                            return "    hive_jdbc_url = " + "\"" + jdbcUrl + "\"";
                                        }
                                        if (line.contains("metastore_uri")) {
                                            return "    metastore_uri = " + "\"" + hmsUrl + "\"";
                                        }
                                        return line;
                                    })
                            .collect(Collectors.toList());
            Files.write(path, newLines);
            log.info("Conf has been updated successfully.");
        } catch (IOException e) {
            throw new RuntimeException("Change conf error", e);
        }
    }

    @TestTemplate
    public void testCreateTable(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/savemode/fake_to_hive_create_table.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Container.ExecResult checkJobRes = container.executeJob("/savemode/hive1_to_assert.conf");
        Assertions.assertEquals(0, checkJobRes.getExitCode());
    }

    @TestTemplate
    public void testReCreateTable(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/savemode/fake_to_hive_re_create_table.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Container.ExecResult checkJobRes = container.executeJob("/savemode/hive2_to_assert.conf");
        Assertions.assertEquals(0, checkJobRes.getExitCode());
    }

    @TestTemplate
    public void testTableExist(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/savemode/fake_to_hive_table_not_exist.conf");
        Assertions.assertNotEquals(0, execResult.getExitCode());
        Assertions.assertTrue(execResult.getStderr().contains("The sink table not exist"));
    }
}
