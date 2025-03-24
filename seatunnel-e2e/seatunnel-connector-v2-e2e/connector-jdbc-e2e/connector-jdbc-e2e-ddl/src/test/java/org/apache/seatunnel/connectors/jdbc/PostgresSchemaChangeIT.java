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

package org.apache.seatunnel.connectors.jdbc;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

public class PostgresSchemaChangeIT extends AbstractSchemaChangeBaseIT {

    private static final String PG_IMAGE = "postgis/postgis";
    private static final String PG_DRIVER_JAR =
            "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar";
    private final int PG_PORT = 5432;
    private final String DATABASE_TYPE = "Postgres";
    private final String PG_USER = "postgres";
    private final String PG_PASSWORD = "postgres";
    private final String PG_SCHEMA = "public";
    private final String PG_JDBC_URL = "jdbc:postgresql://%s:%s/%s";
    private final String PG_DRIVER_CLASS = "org.postgresql.Driver";
    private final String schemaEvolutionCase_config =
            "/mysqlcdc_to_postgres_with_schema_change.conf";
    private final String schemaEvolutionCaseExactlyOnce_config =
            "/mysqlcdc_to_postgres_with_schema_change_exactly_once.conf";
    private final String QUERRY_COLUMNS =
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' ORDER by COLUMN_NAME";

    @Override
    protected SchemaChangeCase getSchemaChangeCase() {
        return SchemaChangeCase.builder()
                .jdbcUrl(PG_JDBC_URL)
                .username(PG_USER)
                .password(PG_PASSWORD)
                .driverUrl(PG_DRIVER_JAR)
                .port(PG_PORT)
                .driverClassName(PG_DRIVER_CLASS)
                .databaseName(SINK_DATABASE)
                .schemaName(PG_SCHEMA)
                .schemaEvolutionCase(schemaEvolutionCase_config)
                .sinkTable1(SINK_TABLE1)
                .openExactlyOnce(true)
                .schemaEvolutionCaseExactlyOnce(schemaEvolutionCaseExactlyOnce_config)
                .sinkTable2(SINK_TABLE2)
                .sinkQueryColumns(QUERRY_COLUMNS)
                .build();
    }

    @Override
    protected GenericContainer initSinkContainer() {
        PostgreSQLContainer container =
                new PostgreSQLContainer<>(
                                DockerImageName.parse(PG_IMAGE)
                                        .asCompatibleSubstituteFor("postgres"))
                        .withDatabaseName(SINK_DATABASE)
                        .withUsername(PG_USER)
                        .withPassword(PG_PASSWORD)
                        .withNetwork(TestSuiteBase.NETWORK)
                        .withNetworkAliases("postgresql")
                        .withCommand("postgres -c max_prepared_transactions=100")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));
        container.setPortBindings(Lists.newArrayList(String.format("%s:%s", PG_PORT, PG_PORT)));
        return container;
    }

    @Override
    protected String sinkDatabaseType() {
        return DATABASE_TYPE;
    }
}
