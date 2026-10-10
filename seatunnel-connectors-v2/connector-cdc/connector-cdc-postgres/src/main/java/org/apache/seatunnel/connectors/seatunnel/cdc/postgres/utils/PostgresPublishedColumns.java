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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;

import lombok.extern.slf4j.Slf4j;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * pgoutput only streams the columns a publication publishes: generated columns are never published
 * before PostgreSQL 18, virtual generated columns are never published, and publication column lists
 * can leave out any column. Columns that are not streamed must not be part of the CDC schema,
 * otherwise snapshot rows carry a value while streamed rows carry NULL.
 */
@Slf4j
public final class PostgresPublishedColumns {

    static final String DEFAULT_PUBLICATION_NAME = "dbz_publication";
    private static final String PUBLICATION_NAME_KEY = "publication.name";
    private static final String PUBLICATION_AUTOCREATE_MODE_KEY = "publication.autocreate.mode";

    private PostgresPublishedColumns() {}

    public static List<CatalogTable> retainPublishedColumns(
            ReadonlyConfig config, List<CatalogTable> tables) {
        Map<String, String> debeziumProperties =
                config.getOptional(SourceOptions.DEBEZIUM_PROPERTIES)
                        .orElse(Collections.emptyMap());
        String publication = publicationName(debeziumProperties);
        // In filtered mode Debezium rewrites an existing publication with
        // ALTER PUBLICATION ... SET TABLE, which drops any column lists.
        boolean useColumnLists =
                !"filtered"
                        .equalsIgnoreCase(
                                debeziumProperties
                                        .getOrDefault(PUBLICATION_AUTOCREATE_MODE_KEY, "")
                                        .trim());
        try (Connection connection =
                DriverManager.getConnection(
                        config.get(JdbcCommonOptions.URL),
                        config.get(JdbcSourceOptions.USERNAME),
                        config.get(JdbcSourceOptions.PASSWORD))) {
            int serverVersion = serverVersion(connection);
            if (serverVersion < 120000) {
                // no generated columns and no publication column lists before PostgreSQL 12
                return tables;
            }
            String publishGeneratedColumns =
                    publishGeneratedColumns(connection, serverVersion, publication);
            Map<String, Set<String>> publishedByTable =
                    useColumnLists && publishGeneratedColumns != null && serverVersion >= 150000
                            ? publishedColumnsByTable(connection, publication)
                            : Collections.emptyMap();
            Map<String, Map<String, String>> generatedByTable = generatedColumnsByTable(connection);
            boolean publishesStoredColumns = "s".equals(publishGeneratedColumns);

            List<CatalogTable> result = new ArrayList<>(tables.size());
            for (CatalogTable table : tables) {
                String key =
                        tableKey(
                                table.getTableId().getSchemaName(),
                                table.getTableId().getTableName());
                Set<String> excluded =
                        excludedColumns(
                                columnNames(table),
                                publishedByTable.get(key),
                                generatedByTable.getOrDefault(key, Collections.emptyMap()),
                                publishesStoredColumns);
                result.add(retainColumns(table, excluded, publication));
            }
            return result;
        } catch (Exception e) {
            log.error(
                    "Failed to read columns published by '{}'; keeping all table columns. "
                            + "Unpublished columns may be written as NULL during streaming or cause a "
                            + "schema-change failure. Check publication access and JDBC connectivity. "
                            + "Failure type: {}, SQL state: {}",
                    publication,
                    e.getClass().getSimpleName(),
                    e instanceof SQLException ? ((SQLException) e).getSQLState() : null);
            return tables;
        }
    }

    static String publicationName(Map<String, String> debeziumProperties) {
        String name = debeziumProperties.get(PUBLICATION_NAME_KEY);
        return name == null || name.trim().isEmpty() ? DEFAULT_PUBLICATION_NAME : name.trim();
    }

    /**
     * Returns the columns pgoutput will not stream for a table.
     *
     * @param publishedColumns columns listed by pg_publication_tables, or {@code null} when the
     *     publication does not cover the table yet or its column lists do not apply
     * @param generatedColumns generated columns of the table and their {@code attgenerated} kind
     */
    static Set<String> excludedColumns(
            List<String> tableColumns,
            Set<String> publishedColumns,
            Map<String, String> generatedColumns,
            boolean publishesStoredColumns) {
        Set<String> excluded = new LinkedHashSet<>();
        for (String column : tableColumns) {
            if (publishedColumns != null) {
                if (!publishedColumns.contains(column)) {
                    excluded.add(column);
                }
                continue;
            }
            String generated = generatedColumns.get(column);
            if (generated != null
                    && !generated.isEmpty()
                    && !(publishesStoredColumns && "s".equals(generated))) {
                excluded.add(column);
            }
        }
        return excluded;
    }

    static CatalogTable retainColumns(
            CatalogTable table, Set<String> excluded, String publication) {
        TableSchema schema = table.getTableSchema();
        PrimaryKey primaryKey = schema.getPrimaryKey();
        Set<String> removed = new LinkedHashSet<>(excluded);
        if (primaryKey != null) {
            removed.removeAll(primaryKey.getColumnNames());
        }
        if (removed.isEmpty()) {
            return table;
        }
        log.info(
                "Table {} columns {} are not published by '{}' and are excluded from the CDC schema",
                table.getTablePath(),
                removed,
                publication);
        List<Column> kept =
                schema.getColumns().stream()
                        .filter(column -> !removed.contains(column.getName()))
                        .collect(Collectors.toList());
        List<ConstraintKey> constraintKeys =
                schema.getConstraintKeys().stream()
                        .filter(
                                key ->
                                        key.getColumnNames().stream()
                                                .noneMatch(
                                                        keyColumn ->
                                                                removed.contains(
                                                                        keyColumn.getColumnName())))
                        .collect(Collectors.toList());
        TableSchema retained =
                TableSchema.builder()
                        .columns(kept)
                        .primaryKey(primaryKey)
                        .constraintKey(constraintKeys)
                        .build();
        return CatalogTable.of(
                table.getTableId(),
                retained,
                table.getOptions(),
                table.getPartitionKeys(),
                table.getComment(),
                table.getCatalogName(),
                table.getMetadataSchema());
    }

    private static List<String> columnNames(CatalogTable table) {
        return table.getTableSchema().getColumns().stream()
                .map(Column::getName)
                .collect(Collectors.toList());
    }

    private static String tableKey(String schema, String table) {
        return schema + "." + table;
    }

    private static int serverVersion(Connection connection) throws SQLException {
        try (PreparedStatement statement =
                        connection.prepareStatement(
                                "SELECT current_setting('server_version_num')");
                ResultSet rs = statement.executeQuery()) {
            rs.next();
            return Integer.parseInt(rs.getString(1));
        }
    }

    /**
     * Returns {@code pubgencols} of an existing publication ({@code "n"} before PostgreSQL 18), or
     * {@code null} when the publication does not exist yet.
     */
    private static String publishGeneratedColumns(
            Connection connection, int serverVersion, String publication) throws SQLException {
        String column = serverVersion >= 180000 ? "pubgencols" : "'n'";
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "SELECT " + column + " FROM pg_publication WHERE pubname = ?")) {
            statement.setString(1, publication);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    private static Map<String, Set<String>> publishedColumnsByTable(
            Connection connection, String publication) throws SQLException {
        Map<String, Set<String>> published = new HashMap<>();
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "SELECT schemaname, tablename, attnames FROM pg_publication_tables"
                                + " WHERE pubname = ?")) {
            statement.setString(1, publication);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    Array attnames = rs.getArray(3);
                    if (attnames != null) {
                        published.put(
                                tableKey(rs.getString(1), rs.getString(2)),
                                new HashSet<>(Arrays.asList((String[]) attnames.getArray())));
                    }
                }
            }
        }
        return published;
    }

    private static Map<String, Map<String, String>> generatedColumnsByTable(Connection connection)
            throws SQLException {
        Map<String, Map<String, String>> generated = new HashMap<>();
        try (PreparedStatement statement =
                        connection.prepareStatement(
                                "SELECT n.nspname, c.relname, a.attname, a.attgenerated"
                                        + " FROM pg_attribute a"
                                        + " JOIN pg_class c ON a.attrelid = c.oid"
                                        + " JOIN pg_namespace n ON c.relnamespace = n.oid"
                                        + " WHERE a.attnum > 0 AND NOT a.attisdropped"
                                        + " AND a.attgenerated <> ''");
                ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                generated
                        .computeIfAbsent(
                                tableKey(rs.getString(1), rs.getString(2)), k -> new HashMap<>())
                        .put(rs.getString(3), rs.getString(4));
            }
        }
        return generated;
    }
}
