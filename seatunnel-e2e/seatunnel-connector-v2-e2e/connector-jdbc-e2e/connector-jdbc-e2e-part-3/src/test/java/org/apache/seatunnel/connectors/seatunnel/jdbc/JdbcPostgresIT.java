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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcPostgresIT extends AbstractJdbcIT {

    private static final String PG_IMAGE = "postgres:14-alpine";
    private static final String PG_CONTAINER_HOST = "seatunnel_e2e_pg";
    private static final String PG_DATABASE = "test";

    private static final String PG_SCHEMA = "public";
    private static final String PG_SOURCE = "source";
    private static final String PG_SINK = "sink";

    private static final String PG_USERNAME = "root";
    private static final String PG_PASSWORD = "test";
    private static final int PG_CONTAINER_PORT = 5432;

    private static final int PG_PORT = 5432;
    private static final String PG_URL = "jdbc:postgresql://" + HOST + ":%s/%s";

    private static final String DRIVER_CLASS = "org.postgresql.Driver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList(
                    "/jdbc_postgres_source_and_sink.conf",
                    "/jdbc_postgres_source_and_sink_parallel.conf",
                    "/jdbc_postgres_source_and_sink_parallel_upper_lower.conf",
                    "/jdbc_postgres_source_and_sink_xa.conf");

    private static final String CREATE_SQL =
            "CREATE TABLE %s (\n" + "age INT NOT NULL,\n" + "name VARCHAR(255) NOT NULL\n" + ")";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(PG_URL, PG_PORT, PG_DATABASE);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(PG_SCHEMA, PG_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(PG_IMAGE)
                .networkAliases(PG_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(PG_CONTAINER_PORT)
                .localPort(PG_PORT)
                .jdbcTemplate(PG_URL)
                .jdbcUrl(jdbcUrl)
                .userName(PG_USERNAME)
                .password(PG_PASSWORD)
                .database(PG_SCHEMA)
                .sourceTable(PG_SOURCE)
                .sinkTable(PG_SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .build();
    }

    @Override
    void compareResult() throws SQLException, IOException {}

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
        DockerImageName imageName = DockerImageName.parse(PG_IMAGE);

        GenericContainer<?> container =
                new PostgreSQLContainer<>(imageName)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(PG_CONTAINER_HOST)
                        .withUsername(PG_USERNAME)
                        .withPassword(PG_PASSWORD)
                        .withCommand("postgres -c max_prepared_transactions=100")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));

        container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", PG_PORT, PG_CONTAINER_PORT)));
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
