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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.utils;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.utils.SeaTunnelException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class PostgresPublishedColumnsTest {

    private static final List<String> COLUMNS =
            Arrays.asList("id", "price", "note", "stored_total", "virtual_total");

    @Test
    public void testGeneratedColumnsAreExcludedWhenPublicationDoesNotCoverTable() {
        Assertions.assertEquals(
                new LinkedHashSet<>(Arrays.asList("stored_total", "virtual_total")),
                PostgresPublishedColumns.excludedColumns(COLUMNS, null, generated(), false));
    }

    @Test
    public void testStoredColumnsAreKeptWhenPublicationPublishesThem() {
        Assertions.assertEquals(
                Collections.singleton("virtual_total"),
                PostgresPublishedColumns.excludedColumns(COLUMNS, null, generated(), true));
    }

    @Test
    public void testPublicationColumnListDecidesWhenTableIsPublished() {
        Assertions.assertEquals(
                new LinkedHashSet<>(Arrays.asList("note", "virtual_total")),
                PostgresPublishedColumns.excludedColumns(
                        COLUMNS,
                        new HashSet<>(Arrays.asList("id", "price", "stored_total")),
                        generated(),
                        true));
    }

    @Test
    public void testNothingIsExcludedWithoutGeneratedColumns() {
        Assertions.assertTrue(
                PostgresPublishedColumns.excludedColumns(
                                Arrays.asList("id", "price"), null, Collections.emptyMap(), false)
                        .isEmpty());
    }

    @Test
    public void testRetainColumnsRemovesExcludedColumnsAndTheirKeys() {
        CatalogTable table = table();

        CatalogTable retained =
                PostgresPublishedColumns.retainColumns(
                        table,
                        new LinkedHashSet<>(Arrays.asList("stored_total", "virtual_total")),
                        "dbz_publication");

        Assertions.assertEquals(
                Arrays.asList("id", "price", "note"),
                columnNames(retained.getTableSchema().getColumns()));
        Assertions.assertEquals(
                Collections.singletonList("id"),
                retained.getTableSchema().getPrimaryKey().getColumnNames());
        Assertions.assertTrue(retained.getTableSchema().getConstraintKeys().isEmpty());
        Assertions.assertEquals(table.getTableId(), retained.getTableId());
    }

    @Test
    public void testUnpublishedPrimaryKeyFailsFast() {
        CatalogTable table = table();

        SeaTunnelException error =
                Assertions.assertThrows(
                        SeaTunnelException.class,
                        () ->
                                PostgresPublishedColumns.retainColumns(
                                        table, Collections.singleton("id"), "orders_pub"));
        Assertions.assertTrue(error.getMessage().contains("orders_pub"));
        Assertions.assertTrue(error.getMessage().contains("public.orders"));
        Assertions.assertTrue(error.getMessage().contains("[id]"));
        Assertions.assertTrue(
                error.getMessage().contains("Include all required primary key columns"));
    }

    @Test
    public void testExistingPg15PublicationWithoutColumnListExcludesGeneratedColumns()
            throws SQLException {
        assertPublishedColumns(
                150000,
                "n",
                false,
                COLUMNS,
                Collections.emptyMap(),
                Arrays.asList("id", "price", "note"));
    }

    @Test
    public void testPg18StoredPublicationWithoutColumnListKeepsOnlyStoredGeneratedColumns()
            throws SQLException {
        assertPublishedColumns(
                180000,
                "s",
                false,
                COLUMNS,
                Collections.emptyMap(),
                Arrays.asList("id", "price", "note", "stored_total"));
    }

    @Test
    public void testExistingExplicitListOverridesGeneratedColumnSetting() throws SQLException {
        assertPublishedColumns(
                180000,
                "s",
                true,
                Arrays.asList("id", "price"),
                Collections.emptyMap(),
                Arrays.asList("id", "price"));
    }

    @Test
    public void testFilteredModeIgnoresExistingColumnList() throws SQLException {
        assertPublishedColumns(
                150000,
                "n",
                true,
                Arrays.asList("id", "price"),
                Collections.singletonMap("publication.autocreate.mode", "filtered"),
                Arrays.asList("id", "price", "note"));
    }

    @Test
    public void testMissingPublicationExcludesGeneratedColumns() throws SQLException {
        assertPublishedColumns(
                150000,
                null,
                false,
                COLUMNS,
                Collections.emptyMap(),
                Arrays.asList("id", "price", "note"));
    }

    @Test
    public void testPg14DoesNotQueryColumnLists() throws SQLException {
        assertPublishedColumns(
                140000,
                "n",
                true,
                Arrays.asList("price"),
                Collections.emptyMap(),
                Arrays.asList("id", "price", "note"));
    }

    @Test
    public void testBeforePg12DoesNotQueryGeneratedColumns() throws SQLException {
        assertPublishedColumns(
                110000, "n", true, Arrays.asList("price"), Collections.emptyMap(), COLUMNS);
    }

    @Test
    public void testUnpublishedPrimaryKeyIsNotSwallowedByMetadataFallback() throws SQLException {
        Assertions.assertThrows(
                SeaTunnelException.class,
                () ->
                        assertPublishedColumns(
                                150000,
                                "n",
                                true,
                                Arrays.asList("price"),
                                Collections.emptyMap(),
                                COLUMNS));
    }

    @Test
    public void testMetadataFailureKeepsOriginalSchema() throws SQLException {
        SQLException failure = new SQLException("private connection details", "08001");
        try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class)) {
            driverManager
                    .when(
                            () ->
                                    DriverManager.getConnection(
                                            "jdbc:test:publication", "user", "secret"))
                    .thenThrow(failure);
            List<CatalogTable> tables = Collections.singletonList(table());
            Assertions.assertSame(
                    tables,
                    PostgresPublishedColumns.retainPublishedColumns(
                            config(Collections.emptyMap()), tables));
        }
    }

    @Test
    public void testPublicationNameDefaultsToDebeziumPublication() {
        Assertions.assertEquals(
                "dbz_publication", PostgresPublishedColumns.publicationName(new HashMap<>()));
        Assertions.assertEquals(
                "orders_pub",
                PostgresPublishedColumns.publicationName(
                        Collections.singletonMap("publication.name", "orders_pub")));
    }

    private static void assertPublishedColumns(
            int version,
            String pubgencols,
            boolean explicitList,
            List<String> published,
            Map<String, String> debezium,
            List<String> expected)
            throws SQLException {
        Connection connection = mock(Connection.class);
        when(connection.prepareStatement(anyString()))
                .thenAnswer(
                        invocation -> {
                            String sql = invocation.getArgument(0);
                            PreparedStatement statement = mock(PreparedStatement.class);
                            ResultSet rs = mock(ResultSet.class);
                            when(statement.executeQuery()).thenReturn(rs);
                            if (sql.contains("server_version_num")) {
                                when(rs.next()).thenReturn(true, false);
                                when(rs.getString(1)).thenReturn(String.valueOf(version));
                            } else if (sql.contains("FROM pg_publication WHERE")) {
                                when(rs.next()).thenReturn(pubgencols != null, false);
                                when(rs.getString(1)).thenReturn(pubgencols);
                            } else if (sql.contains("FROM pg_publication_tables")) {
                                Assertions.assertTrue(version >= 150000);
                                when(rs.next()).thenReturn(true, false);
                                when(rs.getString(1)).thenReturn("public");
                                when(rs.getString(2)).thenReturn("orders");
                                Array columns = mock(Array.class);
                                when(columns.getArray())
                                        .thenReturn(published.toArray(new String[0]));
                                when(rs.getArray(3)).thenReturn(columns);
                                when(rs.getBoolean(4)).thenReturn(explicitList);
                            } else if (sql.contains("FROM pg_attribute")) {
                                Assertions.assertTrue(version >= 120000);
                                when(rs.next()).thenReturn(true, true, false);
                                when(rs.getString(1)).thenReturn("public");
                                when(rs.getString(2)).thenReturn("orders");
                                when(rs.getString(3)).thenReturn("stored_total", "virtual_total");
                                when(rs.getString(4)).thenReturn("s", "v");
                            } else {
                                Assertions.fail("Unexpected catalog query: " + sql);
                            }
                            return statement;
                        });
        try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class)) {
            driverManager
                    .when(
                            () ->
                                    DriverManager.getConnection(
                                            "jdbc:test:publication", "user", "secret"))
                    .thenReturn(connection);
            List<CatalogTable> retained =
                    PostgresPublishedColumns.retainPublishedColumns(
                            config(debezium), Collections.singletonList(table()));
            Assertions.assertEquals(
                    expected, columnNames(retained.get(0).getTableSchema().getColumns()));
        }
    }

    private static ReadonlyConfig config(Map<String, String> debezium) {
        Map<String, Object> options = new HashMap<>();
        options.put("url", "jdbc:test:publication");
        options.put("username", "user");
        options.put("password", "secret");
        options.put("debezium", debezium);
        return ReadonlyConfig.fromConfig(ConfigFactory.parseMap(options));
    }

    private static Map<String, String> generated() {
        Map<String, String> generated = new HashMap<>();
        generated.put("stored_total", "s");
        generated.put("virtual_total", "v");
        return generated;
    }

    private static CatalogTable table() {
        TableSchema.Builder builder = TableSchema.builder();
        COLUMNS.forEach(name -> builder.column(column(name)));
        TableSchema schema =
                builder.primaryKey(PrimaryKey.of("pk", Collections.singletonList("id")))
                        .constraintKey(
                                ConstraintKey.of(
                                        ConstraintKey.ConstraintType.UNIQUE_KEY,
                                        "uk_total",
                                        Collections.singletonList(
                                                ConstraintKey.ConstraintKeyColumn.of(
                                                        "stored_total", null))))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("Postgres", "db", "public", "orders"),
                schema,
                new HashMap<>(),
                Collections.emptyList(),
                "");
    }

    private static Column column(String name) {
        return PhysicalColumn.of(name, BasicType.INT_TYPE, (Long) null, true, null, null);
    }

    private static List<String> columnNames(List<Column> columns) {
        return columns.stream().map(Column::getName).collect(Collectors.toList());
    }
}
