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
import java.sql.*;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.FLINK, EngineType.SPARK},
        disabledReason = "")
public class HiveOverwriteIT extends TestSuiteBase implements TestResource {

    private static final String HIVE_IMAGE = "apache/hive:3.1.3";
    private static final int THRIFT_PORT = 9083;
    private static final int JDBC_PORT = 10010;
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
                "10010",
                "default",
                "hive_overwrite_example");

        changeConnectionURLConf("src/test/resources/overwrite/fake_to_hive_1_on_hdfs.conf");
        changeConnectionURLConf("src/test/resources/overwrite/fake_to_hive_overwrite_on_hdfs.conf");
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
                        + "    name              STRING,"
                        + "    age          INT,"
                        + "    score           DOUBLE,"
                        + "    c_date         DATE"
                        + ")";
        Connection connection = DriverManager.getConnection(jdbcUrl);
        Statement statement = connection.createStatement();
        statement.execute(ddl);
        log.info("create table {} successful. Jdbc url: {}", tableName, jdbcUrl);
    }

    private void selectTable(String host, String port, String db, String tableName)
            throws SQLException {
        String jdbcUrl = "jdbc:hive2://" + host + ":" + port + "/" + db;
        String ddl = "SELECT * FROM " + tableName ;
        Connection connection = DriverManager.getConnection(jdbcUrl);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(ddl);
        // Process the result set
        while (resultSet.next()) {
            // Assuming you want to log the first column value
            String firstColumnValue = resultSet.getString(1);
            log.info("select result is row: {}", firstColumnValue);
        }
    }

    private void changeConnectionURLConf(String resourceFilePath) throws UnknownHostException {
        jdbcUrl = "jdbc:hive2://" + InetAddress.getLocalHost().getHostAddress() + ":10010/default";
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
    public void testFakeSinkHiveOverwriteOnHDFS(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult1 = container.executeJob("/overwrite/fake_to_hive_1_on_hdfs.conf");
        selectTable(InetAddress.getLocalHost().getHostAddress(),
                "10010",
                "default",
                "hive_overwrite_example");
        log.info("execResult1: {}" , execResult1.toString());
        Assertions.assertEquals(0, execResult1.getExitCode());
        Container.ExecResult execResult2 = container.executeJob("/overwrite/fake_to_hive_overwrite_on_hdfs.conf");
        log.info("execResult2: {}", execResult2.toString());
        Container.ExecResult checkJobRes =
                container.executeJob("/overwrite/fake_to_hive_overwrite_on_hdfs.conf");
        log.info("checkJobRes: {}", checkJobRes.toString());
//        Assertions.assertEquals("0", checkJobRes.getStdout());
    }
}
