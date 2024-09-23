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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.commons.lang3.tuple.Pair;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcGreenplumIT extends AbstractJdbcIT {

    private static final String GREENPLUM_IMAGE = "datagrip/greenplum:6.8";
    private static final String GREENPLUM_CONTAINER_HOST = "flink_e2e_greenplum";
    private static final String GREENPLUM_DATABASE = "testdb";

    private static final String GREENPLUM_SCHEMA = "public";
    private static final String GREENPLUM_SOURCE = "source";
    private static final String GREENPLUM_SINK = "sink";

    private static final String GREENPLUM_USERNAME = "tester";
    private static final String GREENPLUM_PASSWORD = "pivotal";
    private static final int GREENPLUM_CONTAINER_PORT = 5432;
    private static final String GREENPLUM_URL = "jdbc:postgresql://" + HOST + ":%s/%s";

    private static final String DRIVER_CLASS = "org.postgresql.Driver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_greenplum_source_and_sink.conf");

    private static final String CREATE_SQL =
            "CREATE TABLE %s (\n" + "age INT NOT NULL,\n" + "name VARCHAR(255) NOT NULL\n" + ")";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(GREENPLUM_URL, GREENPLUM_CONTAINER_PORT, GREENPLUM_DATABASE);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(GREENPLUM_SCHEMA, GREENPLUM_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(GREENPLUM_IMAGE)
                .networkAliases(GREENPLUM_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(GREENPLUM_CONTAINER_PORT)
                .localPort(GREENPLUM_CONTAINER_PORT)
                .jdbcTemplate(GREENPLUM_URL)
                .jdbcUrl(jdbcUrl)
                .userName(GREENPLUM_USERNAME)
                .password(GREENPLUM_PASSWORD)
                .database(GREENPLUM_SCHEMA)
                .sourceTable(GREENPLUM_SOURCE)
                .sinkTable(GREENPLUM_SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .tablePathFullName(GREENPLUM_SOURCE)
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
        DockerImageName imageName = DockerImageName.parse(GREENPLUM_IMAGE);

        GenericContainer<?> container =
                new GenericContainer<>(imageName)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(GREENPLUM_CONTAINER_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(GREENPLUM_IMAGE)));

        container.setPortBindings(
                Lists.newArrayList(
                        String.format(
                                "%s:%s", GREENPLUM_CONTAINER_PORT, GREENPLUM_CONTAINER_PORT)));
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
}
