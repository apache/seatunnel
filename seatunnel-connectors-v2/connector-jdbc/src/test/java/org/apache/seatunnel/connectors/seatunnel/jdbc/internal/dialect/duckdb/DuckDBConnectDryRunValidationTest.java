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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.duckdb;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceFactory;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Validates the {@code --dry-run connect} hooks of {@link JdbcSourceFactory} and {@link
 * JdbcSinkFactory} against a real (embedded DuckDB) database: schema inference, connectivity,
 * target table existence gated by {@code schema_save_mode}, and upstream field compatibility.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DuckDBConnectDryRunValidationTest {

    private static final String SCHEMA_NAME = "main";
    private static final String SOURCE_TABLE_NAME = "dry_run_source";
    private static final String SINK_TABLE_NAME = "dry_run_sink";
    private static final String DB_FILE = "DuckDBConnectDryRunValidationTest.db";
    private static final String DRIVER = "org.duckdb.DuckDBDriver";

    private String jdbcUrl;

    @BeforeAll
    public void setUp() throws Exception {
        File dbFile = new File(DB_FILE);
        if (dbFile.exists()) {
            dbFile.delete();
        }
        jdbcUrl = "jdbc:duckdb:" + dbFile.getAbsolutePath();
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "CREATE TABLE \"%s\".\"%s\" (id INTEGER, name VARCHAR)",
                            SCHEMA_NAME, SOURCE_TABLE_NAME));
            statement.execute(
                    String.format(
                            "CREATE TABLE \"%s\".\"%s\" (id INTEGER, name VARCHAR)",
                            SCHEMA_NAME, SINK_TABLE_NAME));
        }
    }

    @AfterAll
    public void tearDown() {
        File dbFile = new File(DB_FILE);
        if (dbFile.exists()) {
            dbFile.delete();
        }
    }

    @Test
    public void testSourceInfersRealSchemaWithoutReadingRecords() throws Exception {
        JdbcSourceFactory factory = new JdbcSourceFactory();
        TableSourceFactoryContext context = sourceContext(SOURCE_TABLE_NAME);

        List<CatalogTable> catalogTables = factory.inferSchemaForDryRun(context);

        Assertions.assertEquals(1, catalogTables.size());
        SeaTunnelRowType rowType = catalogTables.get(0).getSeaTunnelRowType();
        Assertions.assertArrayEquals(new String[] {"id", "name"}, rowType.getFieldNames());
        // Connection validation must pass against the same live database.
        Assertions.assertDoesNotThrow(
                () -> factory.validateConnectionForDryRun(context, catalogTables));
    }

    @Test
    public void testSourceSchemaInferenceFailsForMissingTable() {
        JdbcSourceFactory factory = new JdbcSourceFactory();
        TableSourceFactoryContext context = sourceContext("no_such_table");

        Assertions.assertThrows(Exception.class, () -> factory.inferSchemaForDryRun(context));
    }

    @Test
    public void testSinkValidationPassesForExistingCompatibleTable() {
        JdbcSinkFactory factory = new JdbcSinkFactory();
        TableSinkFactoryContext context =
                sinkContext(
                        upstreamTable("id", "name"), sinkOptions(SINK_TABLE_NAME, new HashMap<>()));

        Assertions.assertDoesNotThrow(() -> factory.validateConnectionForDryRun(context));
    }

    @Test
    public void testSinkValidationFailsWhenUpstreamFieldMissingInTarget() {
        JdbcSinkFactory factory = new JdbcSinkFactory();
        TableSinkFactoryContext context =
                sinkContext(
                        upstreamTable("id", "name", "extra_col"),
                        sinkOptions(SINK_TABLE_NAME, new HashMap<>()));

        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () -> factory.validateConnectionForDryRun(context));
        Assertions.assertTrue(
                exception.getMessage().contains("missing upstream fields"),
                "Actual: " + exception.getMessage());
        Assertions.assertTrue(
                exception.getMessage().contains("extra_col"), "Actual: " + exception.getMessage());
    }

    @Test
    public void testSinkValidationFailsForMissingTableWithErrorSaveMode() {
        JdbcSinkFactory factory = new JdbcSinkFactory();
        Map<String, Object> extraOptions = new HashMap<>();
        extraOptions.put("schema_save_mode", "ERROR_WHEN_SCHEMA_NOT_EXIST");
        TableSinkFactoryContext context =
                sinkContext(upstreamTable("id", "name"), sinkOptions("absent_table", extraOptions));

        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () -> factory.validateConnectionForDryRun(context));
        Assertions.assertTrue(
                exception.getMessage().contains("does not exist"),
                "Actual: " + exception.getMessage());
    }

    @Test
    public void testSinkValidationPassesForMissingTableWithCreateSaveMode() {
        // The default schema_save_mode creates the table at runtime, so a missing table
        // must not fail the dry-run.
        JdbcSinkFactory factory = new JdbcSinkFactory();
        TableSinkFactoryContext context =
                sinkContext(
                        upstreamTable("id", "name"), sinkOptions("absent_table", new HashMap<>()));

        Assertions.assertDoesNotThrow(() -> factory.validateConnectionForDryRun(context));
    }

    @Test
    public void testSinkValidationWithCustomQueryChecksConnectivityOnly() {
        // A custom-query sink has no resolvable target table; only connectivity is validated.
        JdbcSinkFactory factory = new JdbcSinkFactory();
        Map<String, Object> options = new HashMap<>();
        options.put("url", jdbcUrl);
        options.put("driver", DRIVER);
        options.put("query", "INSERT INTO not_resolved VALUES (?, ?)");
        TableSinkFactoryContext context = sinkContext(upstreamTable("id", "name"), options);

        Assertions.assertDoesNotThrow(() -> factory.validateConnectionForDryRun(context));
    }

    private TableSourceFactoryContext sourceContext(String tableName) {
        Map<String, Object> options = new HashMap<>();
        options.put("url", jdbcUrl);
        options.put("driver", DRIVER);
        options.put("table_path", String.format("%s.%s", SCHEMA_NAME, tableName));
        return new TableSourceFactoryContext(
                ReadonlyConfig.fromMap(options), Thread.currentThread().getContextClassLoader());
    }

    private Map<String, Object> sinkOptions(String tableName, Map<String, Object> extraOptions) {
        Map<String, Object> options = new HashMap<>(extraOptions);
        options.put("url", jdbcUrl);
        options.put("driver", DRIVER);
        options.put("database", "default");
        options.put("table", String.format("%s.%s", SCHEMA_NAME, tableName));
        return options;
    }

    private TableSinkFactoryContext sinkContext(
            CatalogTable upstreamTable, Map<String, Object> options) {
        return new TableSinkFactoryContext(
                upstreamTable,
                ReadonlyConfig.fromMap(options),
                Thread.currentThread().getContextClassLoader());
    }

    private CatalogTable upstreamTable(String... fieldNames) {
        SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType<?>[fieldNames.length];
        fieldTypes[0] = BasicType.INT_TYPE;
        Arrays.fill(fieldTypes, 1, fieldNames.length, BasicType.STRING_TYPE);
        return CatalogTableUtil.getCatalogTable(
                "duckdb",
                "default",
                SCHEMA_NAME,
                "upstream_table",
                new SeaTunnelRowType(fieldNames, fieldTypes));
    }
}
