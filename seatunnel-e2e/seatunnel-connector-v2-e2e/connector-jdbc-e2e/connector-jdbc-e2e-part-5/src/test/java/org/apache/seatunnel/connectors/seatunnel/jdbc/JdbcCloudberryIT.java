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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

public class JdbcCloudberryIT extends AbstractJdbcIT {
    private static final String CLOUDBERRY_IMAGE = "lhrbest/cbdb:1.5.4";
    private static final String CLOUDBERRY_CONTAINER_HOST = "cbdb";
    private static final String CLOUDBERRY_DATABASE = "postgres";

    private static final String CLOUDBERRY_SCHEMA = "public";
    private static final String CLOUDBERRY_SOURCE = "source";
    private static final String CLOUDBERRY_SINK = "sink";

    private static final String CLOUDBERRY_USERNAME = "gpadmin";
    private static final String CLOUDBERRY_PASSWORD = "gpadmin";
    private static final int CLOUDBERRY_CONTAINER_PORT = 5432;

    private static final String CLOUDBERRY_URL = "jdbc:postgresql://" + HOST + ":%s/%s";

    private static final String DRIVER_CLASS = "org.postgresql.Driver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_cloudberry_source_and_sink.conf");

    private static final String CREATE_SQL =
            "CREATE TABLE %s (\n" + "age INT NOT NULL,\n" + "name VARCHAR(255) NOT NULL\n" + ")";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl =
                String.format(CLOUDBERRY_URL, CLOUDBERRY_CONTAINER_PORT, CLOUDBERRY_DATABASE);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(CLOUDBERRY_SCHEMA, CLOUDBERRY_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(CLOUDBERRY_IMAGE)
                .networkAliases(CLOUDBERRY_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(CLOUDBERRY_CONTAINER_PORT)
                .localPort(CLOUDBERRY_CONTAINER_PORT)
                .jdbcTemplate(CLOUDBERRY_URL)
                .jdbcUrl(jdbcUrl)
                .userName(CLOUDBERRY_USERNAME)
                .password(CLOUDBERRY_PASSWORD)
                .database(CLOUDBERRY_SCHEMA)
                .sourceTable(CLOUDBERRY_SOURCE)
                .sinkTable(CLOUDBERRY_SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .tablePathFullName(CLOUDBERRY_SOURCE)
                .useSaveModeCreateTable(false)
                .build();
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[] {
                    "age", "name",
                };

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                i, "f_" + i,
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    GenericContainer<?> initContainer() {
        DockerImageName imageName = DockerImageName.parse(CLOUDBERRY_IMAGE);
        GenericContainer<?> container =
                new GenericContainer<>(imageName)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(CLOUDBERRY_CONTAINER_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(CLOUDBERRY_IMAGE)))
                        .withCommand("/usr/sbin/init") // Ensure container starts correctly
                        .withPrivilegedMode(true); // Set privileged mode
        // Mount cgroup volume
        container.addFileSystemBind("/sys/fs/cgroup", "/sys/fs/cgroup", BindMode.READ_ONLY);
        container.setPortBindings(
                Lists.newArrayList(
                        String.format(
                                "%s:%s", CLOUDBERRY_CONTAINER_PORT, CLOUDBERRY_CONTAINER_PORT)));
        return container;
    }

    @Override
    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }

    @Override
    public void clearTable(String schema, String table) {
        // do nothing.
    }

    @Override
    protected void beforeStartUP() {
        log.info("Setting up Apache Cloudberry...");
        try {
            // Wait for container to start
            Thread.sleep(5000);
            // Switch to gpadmin user and start database
            Container.ExecResult execResult =
                    dbServer.execInContainer("bash", "-c", "su - gpadmin -c 'gpstart -a'");
            log.info("gpstart result: {}", execResult.getStdout());
            // Set gpadmin password
            execResult =
                    dbServer.execInContainer(
                            "bash",
                            "-c",
                            "su - gpadmin -c \"psql -c \\\"ALTER USER gpadmin WITH PASSWORD 'gpadmin';\\\"\"");
            log.info("Set password result: {}", execResult.getStdout());
            // Confirm database is started
            execResult =
                    dbServer.execInContainer(
                            "bash", "-c", "su - gpadmin -c 'psql -c \"SELECT version();\"'");
            log.info("Apache Cloudberry version: {}", execResult.getStdout());

        } catch (InterruptedException | IOException e) {
            log.error("Failed to initialize Apache Cloudberry", e);
            throw new RuntimeException("Failed to initialize Apache Cloudberry", e);
        }
    }

    @BeforeAll
    @Override
    public void startUp() {
        dbServer = initContainer().withImagePullPolicy(PullPolicy.alwaysPull());
        Startables.deepStart(Stream.of(dbServer)).join();
        jdbcCase = getJdbcCase();
        beforeStartUP();
        // Increase retry count and timeout, CloudberryDB might need more time to start
        given().ignoreExceptions()
                .await()
                .atMost(600, TimeUnit.SECONDS) // Increase waiting time
                .pollInterval(10, TimeUnit.SECONDS) // Set polling interval
                .untilAsserted(() -> this.initializeJdbcConnection(jdbcCase.getJdbcUrl()));
        createSchemaIfNeeded();
        createNeededTables();
        insertTestData();
        initCatalog();
    }
}
