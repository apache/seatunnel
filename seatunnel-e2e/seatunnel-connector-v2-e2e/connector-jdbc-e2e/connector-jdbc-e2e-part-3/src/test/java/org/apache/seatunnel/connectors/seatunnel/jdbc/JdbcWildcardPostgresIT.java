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
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.utility.DockerImageName;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class JdbcWildcardPostgresIT extends AbstractJdbcWildcardIT {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcWildcardPostgresIT.class);
    private static final String DATABASE_TYPE = "postgres";
    private static final String PG_IMAGE = "postgres:latest";
    private static final String CONTAINER_HOST = "postgresql";
    private static final String PG_PASSWORD = "postgres";
    private static final String PG_USER = "postgres";
    private static final String PG_DATABASE = "postgres";
    private static final String PG_SOURCE_DATABASE = PG_DATABASE;
    private static final String PG_SINK_DATABASE = PG_DATABASE;
    private static final String PG_SCHEMA = "public";
    private static final Integer PG_PORT = 5432;
    private static final String PG_DRIVER = "org.postgresql.Driver";
    private static final String PG_JDBC_URL = "jdbc:postgresql://%s:%s/%s";
    private static final String CREATE_TABLE_TEMPLATE =
            "CREATE TABLE %s (\"id\" INT NOT NULL, \"name\" VARCHAR(255), \"desc\" VARCHAR(255), PRIMARY KEY (id))";

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
        return JdbcWildcardCase.builder()
                .databaseType(DATABASE_TYPE)
                .driverClass(PG_DRIVER)
                .host(HOST)
                .userName(PG_USER)
                .password(PG_PASSWORD)
                .port(PG_PORT)
                .jdbcUrl(String.format(PG_JDBC_URL, HOST, PG_PORT, PG_DATABASE))
                .configFile("/jdbc_wildcard_postgres_source_to_sink.conf")
                .sourceDatabase(PG_SOURCE_DATABASE)
                .sinkDatabase(PG_SINK_DATABASE)
                .createTableTemplate(CREATE_TABLE_TEMPLATE)
                .insertDataTableTemplate(INSERT_DATA_TEMPLATE)
                .sourceTable(Lists.newArrayList("test1", "test2"))
                .sinkTable(Lists.newArrayList("sink_test1", "sink_test2"))
                .testData(testData)
                .build();
    }

    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar";
    }

    @Override
    GenericContainer<?> initContainer() {
        PostgreSQLContainer<?> container =
                new PostgreSQLContainer<>(
                                DockerImageName.parse(PG_IMAGE)
                                        .asCompatibleSubstituteFor("postgres"))
                        .withImagePullPolicy(PullPolicy.defaultPolicy())
                        .withNetwork(TestSuiteBase.NETWORK)
                        .withNetworkAliases(CONTAINER_HOST)
                        .withDatabaseName(PG_DATABASE)
                        .withUsername(PG_USER)
                        .withPassword(PG_PASSWORD)
                        .withCommand("postgres -c max_prepared_transactions=100")
                        .withLogConsumer(new Slf4jLogConsumer(LOG));

        container.setPortBindings(Lists.newArrayList(String.format("%s:%s", PG_PORT, PG_PORT)));
        return container;
    }

    @Override
    protected String buildDatabaseWithSchema(String database) {
        return quoteIdentifier(database) + "." + quoteIdentifier(PG_SCHEMA);
    }

    @Override
    protected String buildTableInfoWithSchema(String database, String table) {
        if (StringUtils.isNotBlank(database)) {
            return quoteIdentifier(database)
                    + "."
                    + quoteIdentifier(PG_SCHEMA)
                    + "."
                    + quoteIdentifier(table);
        } else {
            return quoteIdentifier(table);
        }
    }
}
