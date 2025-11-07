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
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.apache.commons.lang3.tuple.Pair;

import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** This class is used to test the Generic dialect with MariaDB. */
public class JdbcMariaDBIT extends AbstractJdbcIT {
    private static final String MARIADB_CONTAINER_HOST = "mariadb-e2e";
    private static final int MARIADB_PORT = 3306;
    private static final String MARIADB_IMAGE =
            "mariadb:11.6.2-ubi9"; // Use the appropriate version
    private static final String MARIADB_DRIVER = "org.mariadb.jdbc.Driver";
    private static final String MARIADB_URL = "jdbc:mariadb://" + HOST + ":%s/%s";
    private static final String MARIADB_DATABASE_NAME = "seatunnel";
    private static final String MARIADB_USER = "mariadb_user"; // Replace with your username
    private static final String MARIADB_PASSWORD = "mariadb_password"; // Replace with your password

    private static final String MARIADB_SOURCE = "source";
    private static final String MARIADB_SINK = "sink";
    private static final String CATALOG_DATABASE = "catalog_database";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList(
                    "/jdbc_mariadb_source_and_sink.conf",
                    "/jdbc_mariadb_source_using_table_path.conf");

    private static final String CREATE_SQL =
            "CREATE TABLE IF NOT EXISTS %s\n"
                    + "(\n"
                    + "    `c_int`                  INT                  DEFAULT NULL,\n"
                    + "    `c_varchar`              varchar(255)         DEFAULT NULL,\n"
                    + "    `c_text`                 text                 DEFAULT NULL,\n"
                    + "    `c_float`                float                DEFAULT NULL,\n"
                    + "    `c_double`               double               DEFAULT NULL,\n"
                    + "    `c_date`                 date                 DEFAULT NULL,\n"
                    + "    `c_datetime`             datetime             DEFAULT NULL,\n"
                    + "    `c_timestamp`            timestamp            DEFAULT NULL\n"
                    + ");";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(MARIADB_URL, MARIADB_PORT, MARIADB_DATABASE_NAME);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(MARIADB_DATABASE_NAME, MARIADB_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(MARIADB_IMAGE)
                .networkAliases(MARIADB_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(MARIADB_DRIVER)
                .host(HOST)
                .port(MARIADB_PORT)
                .localPort(MARIADB_PORT)
                .jdbcTemplate(MARIADB_URL)
                .jdbcUrl(jdbcUrl)
                .userName(MARIADB_USER)
                .password(MARIADB_PASSWORD)
                .database(MARIADB_DATABASE_NAME)
                .sourceTable(MARIADB_SOURCE)
                .sinkTable(MARIADB_SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .catalogDatabase(CATALOG_DATABASE)
                .catalogTable(MARIADB_SINK)
                .tablePathFullName(MARIADB_DATABASE_NAME + "." + MARIADB_SOURCE)
                .build();
    }

    @Override
    protected void checkResult(
            String executeKey, TestContainer container, Container.ExecResult execResult) {
        String[] fieldNames =
                new String[] {
                    "c_int",
                    "c_varchar",
                    "c_text",
                    "c_float",
                    "c_double",
                    "c_date",
                    "c_datetime",
                    "c_timestamp"
                };
        defaultCompare(executeKey, fieldNames, "c_int");
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/3.5.1/mariadb-java-client-3.5.1.jar"; // Use the appropriate version
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[] {
                    "c_int",
                    "c_varchar",
                    "c_text",
                    "c_float",
                    "c_double",
                    "c_date",
                    "c_datetime",
                    "c_timestamp"
                };

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String varcharValue = String.format("varchar_value_%d", i);
            String textValue = String.format("text_value_%d", i);
            float floatValue = 1.1f;
            double doubleValue = 1.1;
            LocalDate localDate = LocalDate.now();
            LocalDateTime localDateTime = LocalDateTime.now();

            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                i, // int
                                varcharValue, // varchar
                                textValue, // text
                                floatValue, // float
                                doubleValue, // double
                                Date.valueOf(localDate), // date
                                Timestamp.valueOf(localDateTime), // datetime
                                new Timestamp(System.currentTimeMillis()) // timestamp
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    GenericContainer<?> initContainer() {
        DockerImageName imageName = DockerImageName.parse(MARIADB_IMAGE);
        GenericContainer<?> container =
                new MariaDBContainer(imageName)
                        .withUsername(MARIADB_USER)
                        .withPassword(MARIADB_PASSWORD)
                        .withDatabaseName(MARIADB_DATABASE_NAME)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MARIADB_CONTAINER_HOST)
                        .withExposedPorts(MARIADB_PORT)
                        .waitingFor(Wait.forHealthcheck())
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MARIADB_IMAGE)));
        container.setPortBindings(
                Lists.newArrayList(String.format("%d:%d", MARIADB_PORT, MARIADB_PORT)));
        return container;
    }
}
