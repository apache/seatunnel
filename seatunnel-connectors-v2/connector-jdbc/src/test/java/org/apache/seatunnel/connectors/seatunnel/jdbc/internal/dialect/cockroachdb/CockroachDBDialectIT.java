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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.cockroachdb;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.FieldNamedPreparedStatement;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.ChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcCatalogUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Real PostgreSQL-driver and CockroachDB coverage; run with -DskipIT=false. */
class CockroachDBDialectIT {
    private static GenericContainer<?> database;
    private static String url;
    private final JdbcDialect dialect = new CockroachDBDialect();

    @BeforeAll
    static void startDatabase() throws Exception {
        database =
                new GenericContainer<>("cockroachdb/cockroach:v24.3.0")
                        .withCreateContainerCmdModifier(
                                cmd -> cmd.withEntrypoint("/cockroach/cockroach"))
                        .withCommand(
                                "start-single-node",
                                "--insecure",
                                "--store=type=mem,size=1GiB",
                                "--cache=64MiB",
                                "--max-sql-memory=128MiB")
                        .withExposedPorts(26257)
                        .waitingFor(
                                Wait.forLogMessage(".*node startup completed.*", 1)
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        database.start();
        url =
                "jdbc:postgresql://"
                        + database.getHost()
                        + ":"
                        + database.getMappedPort(26257)
                        + "/defaultdb?sslmode=disable";
        try (Connection connection = connect();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE source_rows (id INT8 PRIMARY KEY, key STRING, amount DECIMAL(12,2), active BOOL, payload BYTES, uid UUID)");
            statement.execute(
                    "INSERT INTO source_rows VALUES (1,'alpha',12.34,true,b'abc','00000000-0000-0000-0000-000000000001'), (2,'你好',-9.50,false,b'def','00000000-0000-0000-0000-000000000002'), (3,NULL,NULL,NULL,NULL,NULL), (4,'',0,true,b'','00000000-0000-0000-0000-000000000004')");
            statement.execute(
                    "CREATE TABLE upsert_rows (id INT8 PRIMARY KEY, email STRING UNIQUE, value STRING, preserved STRING DEFAULT 'default')");
            statement.execute("INSERT INTO upsert_rows VALUES (1,'a','before','keep')");
        }
    }

    @AfterAll
    static void stopDatabase() {
        if (database != null) {
            database.stop();
        }
    }

    private static Connection connect() throws SQLException {
        return DriverManager.getConnection(url, "root", "");
    }

    @Test
    void postgresHashFailsButCockroachHashIncludesEveryRowOnce() throws Exception {
        try (Connection connection = connect();
                Statement statement = connection.createStatement()) {
            SQLException error =
                    Assertions.assertThrows(
                            SQLException.class,
                            () ->
                                    statement.executeQuery(
                                            "SELECT "
                                                    + new PostgresDialect()
                                                            .hashModForField("key", 3)
                                                    + " FROM source_rows"));
            Assertions.assertEquals("42883", error.getSQLState());
        }
        for (String key : new String[] {"key", "uid"}) {
            Map<String, Object> values = new HashMap<>();
            values.put("url", url);
            values.put("driver", "org.postgresql.Driver");
            values.put("username", "root");
            values.put("password", "");
            values.put("dialect", "CockroachDB");
            values.put("query", "SELECT * FROM source_rows");
            values.put("partition_column", key);
            values.put("partition_num", 3);
            values.put("split.string-strategy", "hash");
            JdbcSourceConfig config = JdbcSourceConfig.of(ReadonlyConfig.fromMap(values));
            Map<TablePath, JdbcSourceTable> tables =
                    JdbcCatalogUtils.getTables(
                            config.getJdbcConnectionConfig(), config.getTableConfigList(), null);
            Assertions.assertEquals(1, tables.size());
            JdbcSourceTable table = tables.values().iterator().next();
            Set<Long> seen = new HashSet<>();
            try (ChunkSplitter splitter = ChunkSplitter.create(config)) {
                Collection<JdbcSourceSplit> splits = splitter.generateSplits(table);
                Assertions.assertEquals(3, splits.size());
                for (JdbcSourceSplit split : splits) {
                    try (PreparedStatement query =
                                    splitter.generateSplitStatement(
                                            split, table.getCatalogTable().getTableSchema());
                            ResultSet rows = query.executeQuery()) {
                        while (rows.next()) {
                            SeaTunnelRow row =
                                    dialect.getRowConverter()
                                            .toInternal(
                                                    rows, table.getCatalogTable().getTableSchema());
                            Assertions.assertTrue(
                                    seen.add((Long) row.getField(0)), "duplicate source row");
                        }
                    }
                }
            }
            Assertions.assertEquals(Set.of(1L, 2L, 3L, 4L), seen);
        }
    }

    @Test
    void discoversTableMetadataAndReadsNumericDynamicSplits() throws Exception {
        Map<String, Object> values = new HashMap<>();
        values.put("url", url);
        values.put("driver", "org.postgresql.Driver");
        values.put("username", "root");
        values.put("password", "");
        values.put("dialect", "CockroachDB");
        values.put("table_path", "defaultdb.public.source_rows");
        values.put("partition_column", "id");
        values.put("split.size", 2);
        JdbcSourceConfig config = JdbcSourceConfig.of(ReadonlyConfig.fromMap(values));
        JdbcSourceTable table =
                JdbcCatalogUtils.getTables(
                                config.getJdbcConnectionConfig(), config.getTableConfigList(), null)
                        .values()
                        .iterator()
                        .next();
        Assertions.assertEquals(
                "id",
                table.getCatalogTable().getTableSchema().getPrimaryKey().getColumnNames().get(0));
        Set<Long> seen = new HashSet<>();
        try (ChunkSplitter splitter = ChunkSplitter.create(config)) {
            Collection<JdbcSourceSplit> splits = splitter.generateSplits(table);
            Assertions.assertTrue(splits.size() > 1);
            for (JdbcSourceSplit split : splits) {
                try (PreparedStatement query =
                                splitter.generateSplitStatement(
                                        split, table.getCatalogTable().getTableSchema());
                        ResultSet rows = query.executeQuery()) {
                    while (rows.next()) {
                        SeaTunnelRow row =
                                dialect.getRowConverter()
                                        .toInternal(rows, table.getCatalogTable().getTableSchema());
                        Assertions.assertTrue(
                                seen.add((Long) row.getField(0)), "duplicate numeric split row");
                    }
                }
            }
        }
        Assertions.assertEquals(Set.of(1L, 2L, 3L, 4L), seen);
    }

    @Test
    void mapsTypesAndCountsTableAndFilteredQuery() throws Exception {
        try (Connection connection = connect()) {
            CatalogTable catalog =
                    CatalogUtils.getCatalogTable(
                            connection,
                            "SELECT * FROM source_rows",
                            dialect.getJdbcDialectTypeMapper());
            TableSchema schema = catalog.getTableSchema();
            Assertions.assertEquals(BasicType.LONG_TYPE, schema.getColumn("id").getDataType());
            Assertions.assertEquals(BasicType.STRING_TYPE, schema.getColumn("uid").getDataType());
            JdbcSourceTable table =
                    JdbcSourceTable.builder()
                            .tablePath(TablePath.of("defaultdb", "public", "source_rows"))
                            .catalogTable(catalog)
                            .build();
            Assertions.assertEquals(4L, dialect.approximateRowCntStatement(connection, table));
            table =
                    JdbcSourceTable.builder()
                            .query("SELECT * FROM source_rows WHERE id <= 2")
                            .catalogTable(catalog)
                            .build();
            Assertions.assertEquals(2L, dialect.approximateRowCntStatement(connection, table));
            try (PreparedStatement query =
                            dialect.creatPreparedStatement(
                                    connection, "SELECT * FROM source_rows ORDER BY id", 1);
                    ResultSet rows = query.executeQuery()) {
                Assertions.assertTrue(rows.next());
                SeaTunnelRow row = dialect.getRowConverter().toInternal(rows, schema);
                Assertions.assertEquals(new BigDecimal("12.34"), row.getField(2));
                Assertions.assertEquals(true, row.getField(3));
                Assertions.assertArrayEquals(new byte[] {97, 98, 99}, (byte[]) row.getField(4));
                Assertions.assertEquals("00000000-0000-0000-0000-000000000001", row.getField(5));
            }
        }
    }

    @Test
    void readsCursorWithFreshConnectionMetadataCache() throws Exception {
        TableSchema schema;
        try (Connection discovery = connect()) {
            schema =
                    CatalogUtils.getCatalogTable(
                                    discovery,
                                    "SELECT * FROM source_rows",
                                    dialect.getJdbcDialectTypeMapper())
                            .getTableSchema();
        }
        assertFreshCursor(schema);
        TableSchema explicitSchema =
                TableSchema.builder()
                        .columns(
                                schema.getColumns().stream()
                                        .map(
                                                column ->
                                                        (Column)
                                                                PhysicalColumn.builder()
                                                                        .name(column.getName())
                                                                        .dataType(
                                                                                column
                                                                                        .getDataType())
                                                                        .build())
                                        .collect(Collectors.toList()))
                        .build();
        assertFreshCursor(explicitSchema);
    }

    private void assertFreshCursor(TableSchema schema) throws Exception {
        // The source reader uses a different connection from schema discovery. A cached
        // PostgreSQL metadata lookup would hide additional queries made while a cursor is open.
        try (Connection reader = connect();
                PreparedStatement query =
                        dialect.creatPreparedStatement(
                                reader, "SELECT * FROM source_rows ORDER BY id", 1);
                ResultSet rows = query.executeQuery()) {
            int count = 0;
            while (rows.next()) {
                SeaTunnelRow row = dialect.getRowConverter().toInternal(rows, schema);
                Assertions.assertEquals(++count, ((Long) row.getField(0)).intValue());
                if (count == 1) {
                    Assertions.assertEquals("alpha", row.getField(1));
                    Assertions.assertEquals(new BigDecimal("12.34"), row.getField(2));
                    Assertions.assertEquals(true, row.getField(3));
                    Assertions.assertArrayEquals(new byte[] {97, 98, 99}, (byte[]) row.getField(4));
                    Assertions.assertEquals(
                            "00000000-0000-0000-0000-000000000001", row.getField(5));
                } else if (count == 3) {
                    for (int field = 1; field < 6; field++) {
                        Assertions.assertNull(row.getField(field));
                    }
                }
            }
            Assertions.assertEquals(4, count);
        }
    }

    @Test
    void upsertHonorsSecondaryUniqueKeyAndPreservesOmittedColumn() throws Exception {
        try (Connection connection = connect()) {
            TableSchema schema =
                    CatalogUtils.getCatalogTable(
                                    connection,
                                    "SELECT id,email,value FROM upsert_rows",
                                    dialect.getJdbcDialectTypeMapper())
                            .getTableSchema();
            String sql =
                    dialect.getUpsertStatement(
                                    "defaultdb",
                                    "public.upsert_rows",
                                    schema.getFieldNames(),
                                    new String[] {"email"})
                            .get();
            try (PreparedStatement statement =
                    FieldNamedPreparedStatement.prepareStatement(
                            connection, sql, schema.getFieldNames())) {
                dialect.getRowConverter()
                        .toExternal(
                                schema,
                                null,
                                new SeaTunnelRow(new Object[] {2L, "a", "after"}),
                                statement);
                statement.executeUpdate();
                statement.executeUpdate();
            }
            try (Statement statement = connection.createStatement();
                    ResultSet rows = statement.executeQuery("SELECT * FROM upsert_rows")) {
                Assertions.assertTrue(rows.next());
                Assertions.assertEquals(2L, rows.getLong("id"));
                Assertions.assertEquals("after", rows.getString("value"));
                Assertions.assertEquals("keep", rows.getString("preserved"));
                Assertions.assertFalse(rows.next());
            }
            String invalidTarget =
                    dialect.getUpsertStatement(
                                    "defaultdb",
                                    "public.upsert_rows",
                                    schema.getFieldNames(),
                                    new String[] {"value"})
                            .get();
            try (PreparedStatement statement =
                    FieldNamedPreparedStatement.prepareStatement(
                            connection, invalidTarget, schema.getFieldNames())) {
                dialect.getRowConverter()
                        .toExternal(
                                schema,
                                null,
                                new SeaTunnelRow(new Object[] {3L, "b", "value"}),
                                statement);
                Assertions.assertThrows(SQLException.class, statement::executeUpdate);
            }
            Assertions.assertEquals(
                    PostgresDialect.class, JdbcDialectLoader.load(url, null, "").getClass());
        }
    }
}
