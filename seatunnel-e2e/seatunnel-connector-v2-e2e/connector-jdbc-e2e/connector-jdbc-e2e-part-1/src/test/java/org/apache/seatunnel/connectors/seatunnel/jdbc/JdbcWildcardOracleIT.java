/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class JdbcWildcardOracleIT extends AbstractJdbcWildcardIT {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcWildcardOracleIT.class);
    private static final String DATABASE_TYPE = "oracle";
    private static final String ORACLE_IMAGE = "gvenzl/oracle-xe:21-slim";
    private static final String ORACLE_NETWORK_ALIASES = "e2e_oracleDb";
    private static final String DRIVER_CLASS = "oracle.jdbc.OracleDriver";
    private static final int ORACLE_PORT = 1521;
    private static final String ORACLE_URL = "jdbc:oracle:thin:@" + HOST + ":%s/%s";
    private static final String USERNAME = "TESTUSER";
    private static final String PASSWORD = "testPassword";
    private static final String SCHEMA = USERNAME;
    private static final String CREATE_TABLE_TEMPLATE =
            "CREATE TABLE %s (\"id\" INT NOT NULL, \"name\" VARCHAR2(255), \"desc\" VARCHAR2(255), PRIMARY KEY (\"id\"))";

    @Override
    JdbcWildcardCase getJdbcWildcardsCase() {
        Pair<String[], List<SeaTunnelRow>> testData = initTestData();
        String columns =
                Arrays.stream(testData.getLeft())
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(testData.getLeft()).map(f -> "?").collect(Collectors.joining(", "));
        String INSERT_DATA_TEMPLATE =
                "INSERT INTO %s (" + columns + ") VALUES (" + placeholders + ")";
        String jdbcUrl = String.format(ORACLE_URL, ORACLE_PORT, SCHEMA);
        return JdbcWildcardCase.builder()
                .databaseType(DATABASE_TYPE)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .userName(USERNAME)
                .password(PASSWORD)
                .port(ORACLE_PORT)
                .jdbcUrl(jdbcUrl)
                .configFile("/jdbc_wildcards_oracle_source_to_sink.conf")
                .sourceDatabase(SCHEMA)
                .sinkDatabase(SCHEMA)
                .createTableTemplate(CREATE_TABLE_TEMPLATE)
                .insertDataTableTemplate(INSERT_DATA_TEMPLATE)
                .sourceTable(Lists.newArrayList("SOURCE_TEST1", "SOURCE_TEST2"))
                .sinkTable(Lists.newArrayList("SINK_SOURCE_TEST1", "SINK_SOURCE_TEST2"))
                .testData(testData)
                .build();
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/12.2.0.1/ojdbc8-12.2.0.1.jar && wget https://repo1.maven.org/maven2/com/oracle/database/xml/xdb6/12.2.0.1/xdb6-12.2.0.1.jar && wget https://repo1.maven.org/maven2/com/oracle/database/xml/xmlparserv2/12.2.0.1/xmlparserv2-12.2.0.1.jar";
    }

    @Override
    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }

    @Override
    protected void dropTableIfExists() {
        try (Statement statement = connection.createStatement()) {
            for (String table : jdbcWildcardCase.getSinkTable()) {
                ResultSet resultSet =
                        statement.executeQuery(
                                String.format(
                                        "SELECT * FROM ALL_TABLES WHERE OWNER='%s' AND TABLE_NAME='%s'",
                                        jdbcWildcardCase.getSinkDatabase(), table));
                if (resultSet.next()) {
                    statement.execute(
                            String.format(
                                    "DROP TABLE %s",
                                    buildTableInfoWithSchema(
                                            jdbcWildcardCase.getSinkDatabase(), table)));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    GenericContainer<?> initContainer() {
        DockerImageName imageName = DockerImageName.parse(ORACLE_IMAGE);

        GenericContainer<?> container =
                new OracleContainer(imageName)
                        .withDatabaseName(SCHEMA)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("sql/oracle_init.sql"),
                                "/container-entrypoint-startdb.d/init.sql")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(ORACLE_NETWORK_ALIASES)
                        .withExposedPorts(ORACLE_PORT)
                        .withLogConsumer(new Slf4jLogConsumer(LOG));

        container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", ORACLE_PORT, ORACLE_PORT)));

        return container;
    }
}
