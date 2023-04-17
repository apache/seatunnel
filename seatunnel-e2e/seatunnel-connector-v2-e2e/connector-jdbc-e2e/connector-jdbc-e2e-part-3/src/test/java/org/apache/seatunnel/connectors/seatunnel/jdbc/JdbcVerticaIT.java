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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.commons.lang3.tuple.Pair;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcVerticaIT extends AbstractJdbcIT {

    private static final String VERTICA_IMAGE = "vertica/vertica-ce:latest";
    private static final String VERTICA_CONTAINER_HOST = "e2e_vertica";

    private static final String VERTICA_DATABASE = "VMart";
    private static final String VERTICA_SCHEMA = "public";
    private static final String VERTICA_SOURCE = "e2e_table_source";
    private static final String VERTICA_SINK = "e2e_table_sink";
    private static final String VERTICA_USERNAME = "DBADMIN";
    private static final String VERTICA_PASSWORD = "";
    private static final int VERTICA_PORT = 5433;
    private static final String VERTICA_URL = "jdbc:vertica://" + HOST + ":%s/%s";

    private static final String DRIVER_CLASS = "com.vertica.jdbc.Driver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_vertica_source_and_sink.conf");
    private static final String CREATE_SQL =
            "create table if not exists %s\n"
                    + "(\n"
                    + "   id int,\n"
                    + "   name varchar,\n"
                    + "   age int\n"
                    + ");";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(VERTICA_URL, VERTICA_PORT, VERTICA_DATABASE);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(VERTICA_SCHEMA, VERTICA_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(VERTICA_IMAGE)
                .networkAliases(VERTICA_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(VERTICA_PORT)
                .localPort(VERTICA_PORT)
                .jdbcTemplate(VERTICA_URL)
                .jdbcUrl(jdbcUrl)
                .userName(VERTICA_USERNAME)
                .password(VERTICA_PASSWORD)
                .database(VERTICA_SCHEMA)
                .sourceTable(VERTICA_SOURCE)
                .sinkTable(VERTICA_SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .build();
    }

    @Override
    void compareResult() {}

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/vertica/jdbc/vertica-jdbc/12.0.3-0/vertica-jdbc-12.0.3-0.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames = new String[] {"id", "name", "age"};

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                i, // INT
                                String.format("f1_%s", i), // VARCHAR
                                i
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    protected GenericContainer<?> initContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(VERTICA_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(VERTICA_CONTAINER_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(VERTICA_IMAGE)));
        container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", VERTICA_PORT, VERTICA_PORT)));

        return container;
    }

    @Override
    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }
}
