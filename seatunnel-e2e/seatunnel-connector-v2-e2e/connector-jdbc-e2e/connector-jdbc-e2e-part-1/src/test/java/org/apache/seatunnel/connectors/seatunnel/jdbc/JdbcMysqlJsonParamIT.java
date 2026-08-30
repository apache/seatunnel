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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

/**
 * E2E test verifying that json params via cli are correctly parsed by the ParameterSplitter and
 * ConfigBuilder
 */
@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.FLINK, EngineType.SPARK})
public class JdbcMysqlJsonParamIT extends TestSuiteBase implements TestResource {

    private static final String MYSQL_IMAGE = "mysql:8.0";
    private static final String MYSQL_HOST = "mysql_json_param_e2e";
    private static final String MYSQL_DATABASE = "json_test";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "Abc!@#135_seatunnel";

    private static final List<String> MULTI_TABLE_DDL =
            Arrays.asList(
                    "drop table if exists ml_tags;\n",
                    "create table ml_tags("
                            + "user_id bigint,"
                            + "movie_id bigint,"
                            + "tag varchar(400),"
                            + "unix_time bigint"
                            + ");\n",
                    "INSERT INTO ml_tags(user_id, movie_id, tag, unix_time) VALUES"
                            + "(336, 1, 'pixar', 1139045764),"
                            + "(62, 2, 'fantasy', 1528843929),"
                            + "(289, 3, 'moldy', 1143424860);",
                    "drop table if exists ratings;\n",
                    "create table ratings("
                            + "user_id bigint,"
                            + "movie_id bigint,"
                            + "rating float,"
                            + "unix_time bigint"
                            + ");\n",
                    "INSERT INTO ratings(user_id, movie_id, rating, unix_time) VALUES"
                            + "(1, 1, 4.0, 964982703),"
                            + "(1, 3, 4.0, 964981247);");

    private static final String MYSQL_DRIVER_URL =
            "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";

    private MySQLContainer<?> mysqlContainer;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult result =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib"
                                        + " && cd /tmp/seatunnel/plugins/Jdbc/lib"
                                        + " && wget -q "
                                        + MYSQL_DRIVER_URL);

                ContainerUtil.copyConnectorJarToContainer(
                        container,
                        "/jdbc_mysql_json_params.conf",
                        "seatunnel-connectors-v2",
                        "connector-jdbc-",
                        "seatunnel",
                        "/tmp/seatunnel");

                container.copyFileToContainer(
                        MountableFile.forClasspathResource("jdbc_mysql_json_params.conf"),
                        "/tmp/jdbc_mysql_json_params.conf");

                Assertions.assertEquals(
                        0,
                        result.getExitCode(),
                        "Failed to download MySQL driver: " + result.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        mysqlContainer =
                new MySQLContainer<>(DockerImageName.parse(MYSQL_IMAGE))
                        .withDatabaseName(MYSQL_DATABASE)
                        .withUsername(MYSQL_USER)
                        .withPassword(MYSQL_PASSWORD)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MYSQL_IMAGE)));

        Startables.deepStart(Stream.of(mysqlContainer)).join();

        given().ignoreExceptions()
                .await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> initMysqlData());
        log.info("MySQL container started and test data initialised.");
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (mysqlContainer != null) {
            mysqlContainer.close();
        }
    }

    /**
     * 1: Verify MySQL properties take effect via CLI JSON params.
     *
     * <p>2: Verify nested JSON params (array in JSON, JSON in array) can be read from MySQL JSON
     * column via CLI.
     *
     * @param container
     * @throws IOException
     * @throws InterruptedException
     */
    @TestTemplate
    public void testJsonParams(TestContainer container) throws IOException, InterruptedException {
        List<String> variables = new ArrayList<>();
        variables.add("-c /tmp/jdbc_mysql_json_params.conf");
        variables.add("-i mysql_host=" + MYSQL_HOST);
        variables.add("-i mysql_port=3306");
        variables.add("-i mysql_db=" + MYSQL_DATABASE);
        variables.add(
                "-i mysql_props='{"
                        + "\"useSSL\":\"false\","
                        + "\"allowPublicKeyRetrieval\":\"true\"}'");

        variables.add("-i mysql_password=" + MYSQL_PASSWORD);

        variables.add(
                "-i table_list=['{\"table_path\":\"json_test.ml_*\",\"use_regex\":\"true\"}','{\"table_path\":\"json_test.ratings\"}']");
        variables.add(
                "-i table_filter='{\"plugin_input\":\"mysql_source\",\"plugin_output\":\"table_filter\",\"include_fields\":[movie_id,unix_time]}'");

        Container.ExecResult result =
                container.executeBaseCommand(variables.toArray(new String[0]));

        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "plain json or nested json or nested array value from -i variables assertion failed:\n"
                        + result.getStderr());
    }

    private void initMysqlData() throws Exception {
        String jdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true&allowMultiQueries=true",
                        mysqlContainer.getHost(),
                        mysqlContainer.getFirstMappedPort(),
                        MYSQL_DATABASE);
        try (Connection conn = DriverManager.getConnection(jdbcUrl, MYSQL_USER, MYSQL_PASSWORD);
                Statement stmt = conn.createStatement()) {

            MULTI_TABLE_DDL.forEach(
                    sql -> {
                        try {
                            stmt.execute(sql);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }
}
