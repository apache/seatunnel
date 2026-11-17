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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlConnectionUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlSchema;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Verifies the `DESC` schema fallback of {@link MySqlSchema} against a real MySQL server.
 *
 * <p>{@link MySqlSchema#readTableSchemaByDesc} builds a synthetic {@code CREATE TABLE} statement
 * from the output of {@code DESC <table>} and feeds it to the Debezium DDL parser. For a table
 * without a primary key that statement used to end with an empty {@code PRIMARY KEY ()} clause,
 * which is not valid SQL: the parser rejects the statement, no schema change event is produced and
 * the table schema ends up missing (in a job this surfaces as {@code Can't obtain schema for table
 * <table>}).
 *
 * <p>This test is a standalone JUnit 5 test rather than a {@code TestSuiteBase} case: it only needs
 * a MySQL container, not the SeaTunnel distribution images.
 */
@Slf4j
public class MysqlSchemaDescIT {

    private static final String DATABASE_NAME = "mysql_schema_desc";
    private static final String USERNAME = "mysqluser";
    private static final String PASSWORD = "mysqlpw";

    private static final String NO_PK_TABLE = "no_pk_table";
    private static final String PK_TABLE = "pk_table";

    private static MySqlContainer mysqlContainer;
    private static Connection setupConnection;
    private static JdbcConnection jdbcConnection;
    private static MySqlSchema mySqlSchema;

    private static TableId noPkTableId;
    private static TableId pkTableId;

    @BeforeAll
    public static void startUp() throws Exception {
        mysqlContainer =
                new MySqlContainer(MySqlVersion.V8_0)
                        .withConfigurationOverride("docker/server-gtids/my.cnf")
                        .withSetupSQL("docker/setup.sql")
                        .withDatabaseName(DATABASE_NAME)
                        .withUsername(USERNAME)
                        .withPassword(PASSWORD)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger("mysql-schema-desc-it")));
        mysqlContainer.start();

        setupConnection =
                DriverManager.getConnection(mysqlContainer.getJdbcUrl(), USERNAME, PASSWORD);
        try (Statement statement = setupConnection.createStatement()) {
            // No primary key at all: the DESC fallback DDL must not contain a PRIMARY KEY clause.
            statement.execute(
                    String.format(
                            "CREATE TABLE `%s`.`%s` (`id` INT NOT NULL, `name` VARCHAR(64))",
                            DATABASE_NAME, NO_PK_TABLE));
            // Control case: the PRIMARY KEY clause must still be generated for a primary key table.
            statement.execute(
                    String.format(
                            "CREATE TABLE `%s`.`%s` (`id` INT NOT NULL, `name` VARCHAR(64), PRIMARY KEY (`id`))",
                            DATABASE_NAME, PK_TABLE));
        }

        MySqlSourceConfigFactory factory = new MySqlSourceConfigFactory();
        factory.hostname(mysqlContainer.getHost());
        factory.port(mysqlContainer.getDatabasePort());
        factory.username(USERNAME);
        factory.password(PASSWORD);
        factory.databaseList(DATABASE_NAME);
        factory.tableList(DATABASE_NAME + "." + NO_PK_TABLE, DATABASE_NAME + "." + PK_TABLE);
        MySqlSourceConfig sourceConfig = factory.create(0);

        noPkTableId = TableId.parse(DATABASE_NAME + "." + NO_PK_TABLE);
        pkTableId = TableId.parse(DATABASE_NAME + "." + PK_TABLE);

        Map<TableId, CatalogTable> tableMap = new HashMap<>();
        tableMap.put(noPkTableId, catalogTable(noPkTableId, false));
        tableMap.put(pkTableId, catalogTable(pkTableId, true));

        // The `DESC` fallback is what a real job hits when SHOW CREATE TABLE cannot be used, so
        // drive it through the same MySQL connection a job would use.
        jdbcConnection =
                MySqlConnectionUtils.createMySqlConnection(sourceConfig.getDbzConfiguration())
                        .connect();
        mySqlSchema = new MySqlSchema(sourceConfig, false, tableMap);
        log.info("MySQL schema DESC IT is ready, database {}", DATABASE_NAME);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        if (mySqlSchema != null) {
            mySqlSchema.close();
        }
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        if (setupConnection != null && !setupConnection.isClosed()) {
            setupConnection.close();
        }
        if (mysqlContainer != null) {
            mysqlContainer.stop();
        }
    }

    @Test
    public void testDescTableWithoutPrimaryKey() {
        TableChanges.TableChange tableChange =
                mySqlSchema.readTableSchemaByDesc(jdbcConnection, noPkTableId);

        Assertions.assertNotNull(tableChange, "no table change was produced for " + noPkTableId);
        Assertions.assertEquals(noPkTableId, tableChange.getId());
        Assertions.assertEquals(TableChanges.TableChangeType.CREATE, tableChange.getType());

        Table table = tableChange.getTable();
        Assertions.assertNotNull(table);
        Assertions.assertEquals(Arrays.asList("id", "name"), columnNames(table));
        Assertions.assertTrue(
                table.primaryKeyColumnNames().isEmpty(),
                "a table without a primary key must not report primary key columns, but got "
                        + table.primaryKeyColumnNames());
        Assertions.assertEquals("INT", table.columnWithName("id").typeName());
        Assertions.assertEquals("VARCHAR", table.columnWithName("name").typeName());
    }

    @Test
    public void testDescTableWithPrimaryKey() {
        TableChanges.TableChange tableChange =
                mySqlSchema.readTableSchemaByDesc(jdbcConnection, pkTableId);

        Assertions.assertNotNull(tableChange, "no table change was produced for " + pkTableId);
        Assertions.assertEquals(pkTableId, tableChange.getId());
        Assertions.assertEquals(TableChanges.TableChangeType.CREATE, tableChange.getType());

        Table table = tableChange.getTable();
        Assertions.assertNotNull(table);
        Assertions.assertEquals(Arrays.asList("id", "name"), columnNames(table));
        Assertions.assertEquals(Collections.singletonList("id"), table.primaryKeyColumnNames());
        Assertions.assertEquals("INT", table.columnWithName("id").typeName());
        Assertions.assertEquals("VARCHAR", table.columnWithName("name").typeName());
    }

    private static List<String> columnNames(Table table) {
        List<String> names = new ArrayList<>();
        for (Column column : table.columns()) {
            names.add(column.name());
        }
        return names;
    }

    private static CatalogTable catalogTable(TableId tableId, boolean withPrimaryKey) {
        TableSchema.Builder builder =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("id")
                                        .dataType(BasicType.INT_TYPE)
                                        .nullable(false)
                                        .build())
                        .column(
                                PhysicalColumn.builder()
                                        .name("name")
                                        .dataType(BasicType.STRING_TYPE)
                                        .nullable(true)
                                        .build());
        if (withPrimaryKey) {
            builder.primaryKey(PrimaryKey.of("pk", Collections.singletonList("id")));
        }
        return CatalogTable.of(
                TableIdentifier.of("mysql", TablePath.of(tableId.catalog(), tableId.table())),
                builder.build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }
}
