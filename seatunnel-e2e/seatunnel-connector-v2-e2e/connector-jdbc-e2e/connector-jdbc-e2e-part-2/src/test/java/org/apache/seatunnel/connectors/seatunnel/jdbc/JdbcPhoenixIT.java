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
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.apache.commons.lang3.tuple.Pair;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JdbcPhoenixIT extends AbstractJdbcIT {
    private static final String PHOENIX_IMAGE = "iteblog/hbase-phoenix-docker:1.0";
    private static final String PHOENIX_CONTAINER_HOST = "seatunnel_e2e_phoenix";
    private static final String PHOENIX_DATABASE = "test";

    private static final String PHOENIX_SOURCE = "SOURCE";

    private static final String PHOENIX_SINK = "SINK";

    private static final int PHOENIX_CONTAINER_PORT = 8765;
    private static final String PHOENIX_URL =
            "jdbc:phoenix:thin:url=http://" + HOST + ":%s;serialization=PROTOBUF";

    private static final String DRIVER_CLASS = "org.apache.phoenix.queryserver.client.Driver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_phoenix_source_and_sink.conf");

    private static final String CREATE_SQL =
            "CREATE TABLE %s (\n" + "age INTEGER PRIMARY KEY,\n" + "name VARCHAR(255)\n" + ")";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(PHOENIX_URL, PHOENIX_CONTAINER_PORT);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(PHOENIX_DATABASE, PHOENIX_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(PHOENIX_IMAGE)
                .networkAliases(PHOENIX_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(PHOENIX_CONTAINER_PORT)
                .localPort(PHOENIX_CONTAINER_PORT)
                .jdbcTemplate(PHOENIX_URL)
                .jdbcUrl(jdbcUrl)
                .database(PHOENIX_DATABASE)
                .sourceTable(PHOENIX_SOURCE)
                .sinkTable(PHOENIX_SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .build();
    }

    @Override
    public String insertTable(String schema, String table, String... fields) {
        String columns = String.join(", ", fields);
        String placeholders = Arrays.stream(fields).map(f -> "?").collect(Collectors.joining(", "));

        return "UPSERT INTO "
                + buildTableInfoWithSchema(schema, table)
                + " ("
                + columns
                + " )"
                + " VALUES ("
                + placeholders
                + ")";
    }

    @Override
    public void clearTable(String schema, String table) {
        try (Statement statement = connection.createStatement()) {
            String truncate =
                    String.format(
                            "delete from %s where 1=1", buildTableInfoWithSchema(schema, table));
            statement.execute(truncate);
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException exception) {
                throw new SeaTunnelRuntimeException(JdbcITErrorCode.CLEAR_TABLE_FAILED, exception);
            }
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CLEAR_TABLE_FAILED, e);
        }
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/aliyun/phoenix/ali-phoenix-shaded-thin-client/5.2.5-HBase-2.x/ali-phoenix-shaded-thin-client-5.2.5-HBase-2.x.jar";
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
        DockerImageName imageName = DockerImageName.parse(PHOENIX_IMAGE);

        GenericContainer<?> container =
                new GenericContainer<>(imageName)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(PHOENIX_CONTAINER_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PHOENIX_IMAGE)));
        container.setPortBindings(
                Lists.newArrayList(
                        String.format("%s:%s", PHOENIX_CONTAINER_PORT, PHOENIX_CONTAINER_PORT)));
        return container;
    }

    @Override
    public String quoteIdentifier(String field) {
        return field;
    }
}
