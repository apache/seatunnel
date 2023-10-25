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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class CatalogUtils {
    public static String getFieldIde(String identifier, String fieldIde) {
        if (StringUtils.isBlank(fieldIde)) {
            return identifier;
        }
        switch (FieldIdeEnum.valueOf(fieldIde.toUpperCase())) {
            case LOWERCASE:
                return identifier.toLowerCase();
            case UPPERCASE:
                return identifier.toUpperCase();
            default:
                return identifier;
        }
    }

    public static String quoteIdentifier(String identifier, String fieldIde, String quote) {
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length - 1; i++) {
                sb.append(quote).append(parts[i]).append(quote).append(".");
            }
            return sb.append(quote)
                    .append(getFieldIde(parts[parts.length - 1], fieldIde))
                    .append(quote)
                    .toString();
        }

        return quote + getFieldIde(identifier, fieldIde) + quote;
    }

    public static String quoteIdentifier(String identifier, String fieldIde) {
        return getFieldIde(identifier, fieldIde);
    }

    public static String quoteTableIdentifier(String identifier, String fieldIde) {
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length - 1; i++) {
                sb.append(parts[i]).append(".");
            }
            return sb.append(getFieldIde(parts[parts.length - 1], fieldIde)).toString();
        }

        return getFieldIde(identifier, fieldIde);
    }

    public static Optional<PrimaryKey> getPrimaryKey(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        // According to the Javadoc of java.sql.DatabaseMetaData#getPrimaryKeys,
        // the returned primary key columns are ordered by COLUMN_NAME, not by KEY_SEQ.
        // We need to sort them based on the KEY_SEQ value.
        ResultSet rs =
                metaData.getPrimaryKeys(
                        tablePath.getDatabaseName(),
                        tablePath.getSchemaName(),
                        tablePath.getTableName());

        // seq -> column name
        List<Pair<Integer, String>> primaryKeyColumns = new ArrayList<>();
        String pkName = null;
        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            // all the PK_NAME should be the same
            pkName = rs.getString("PK_NAME");
            int keySeq = rs.getInt("KEY_SEQ");
            // KEY_SEQ is 1-based index
            primaryKeyColumns.add(Pair.of(keySeq, columnName));
        }
        // initialize size
        List<String> pkFields =
                primaryKeyColumns.stream()
                        .sorted(Comparator.comparingInt(Pair::getKey))
                        .map(Pair::getValue)
                        .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(pkFields)) {
            return Optional.empty();
        }
        return Optional.of(PrimaryKey.of(pkName, pkFields));
    }

    public static List<ConstraintKey> getConstraintKeys(
            DatabaseMetaData metadata, TablePath tablePath) throws SQLException {
        ResultSet resultSet =
                metadata.getIndexInfo(
                        tablePath.getDatabaseName(),
                        tablePath.getSchemaName(),
                        tablePath.getTableName(),
                        false,
                        false);
        // index name -> index
        Map<String, ConstraintKey> constraintKeyMap = new HashMap<>();
        while (resultSet.next()) {
            String columnName = resultSet.getString("COLUMN_NAME");
            if (columnName == null) {
                continue;
            }
            String indexName = resultSet.getString("INDEX_NAME");
            boolean noUnique = resultSet.getBoolean("NON_UNIQUE");

            ConstraintKey constraintKey =
                    constraintKeyMap.computeIfAbsent(
                            indexName,
                            s -> {
                                ConstraintKey.ConstraintType constraintType =
                                        ConstraintKey.ConstraintType.INDEX_KEY;
                                if (!noUnique) {
                                    constraintType = ConstraintKey.ConstraintType.UNIQUE_KEY;
                                }
                                return ConstraintKey.of(
                                        constraintType, indexName, new ArrayList<>());
                            });

            ConstraintKey.ColumnSortType sortType =
                    "A".equals(resultSet.getString("ASC_OR_DESC"))
                            ? ConstraintKey.ColumnSortType.ASC
                            : ConstraintKey.ColumnSortType.DESC;
            ConstraintKey.ConstraintKeyColumn constraintKeyColumn =
                    new ConstraintKey.ConstraintKeyColumn(columnName, sortType);
            constraintKey.getColumnNames().add(constraintKeyColumn);
        }
        return new ArrayList<>(constraintKeyMap.values());
    }

    public static TableSchema getTableSchema(DatabaseMetaData metadata, TablePath tablePath)
            throws SQLException {
        Optional<PrimaryKey> primaryKey = getPrimaryKey(metadata, tablePath);
        List<ConstraintKey> constraintKeys = getConstraintKeys(metadata, tablePath);
        List<Column> columns = JdbcColumnConverter.convert(metadata, tablePath);
        return TableSchema.builder()
                .primaryKey(primaryKey.orElse(null))
                .constraintKey(constraintKeys)
                .columns(columns)
                .build();
    }

    public static CatalogTable getCatalogTable(Connection connection, TablePath tablePath)
            throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        TableSchema tableSchema = getTableSchema(metadata, tablePath);
        String catalogName = "jdbc_catalog";
        return CatalogTable.of(
                TableIdentifier.of(
                        catalogName,
                        tablePath.getDatabaseName(),
                        tablePath.getSchemaName(),
                        tablePath.getTableName()),
                tableSchema,
                new HashMap<>(),
                new ArrayList<>(),
                "",
                catalogName);
    }

    public static CatalogTable getCatalogTable(ResultSetMetaData resultSetMetaData)
            throws SQLException {
        return getCatalogTable(
                resultSetMetaData,
                (BiFunction<ResultSetMetaData, Integer, Column>)
                        (metadata, index) -> {
                            try {
                                return JdbcColumnConverter.convert(metadata, index);
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    public static CatalogTable getCatalogTable(
            ResultSetMetaData metadata, JdbcDialectTypeMapper typeMapper) throws SQLException {
        return getCatalogTable(
                metadata,
                (BiFunction<ResultSetMetaData, Integer, Column>)
                        (resultSetMetaData, index) -> {
                            try {
                                return typeMapper.mappingColumn(resultSetMetaData, index);
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    public static CatalogTable getCatalogTable(
            ResultSetMetaData metadata,
            BiFunction<ResultSetMetaData, Integer, Column> columnConverter)
            throws SQLException {
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        for (int index = 1; index <= metadata.getColumnCount(); index++) {
            Column column = columnConverter.apply(metadata, index);
            schemaBuilder.column(column);
        }
        String catalogName = "jdbc_catalog";
        return CatalogTable.of(
                TableIdentifier.of(catalogName, "default", "default", "default"),
                schemaBuilder.build(),
                new HashMap<>(),
                new ArrayList<>(),
                "",
                catalogName);
    }

    public static CatalogTable getCatalogTable(
            Connection connection, String sqlQuery, JdbcDialectTypeMapper typeMapper)
            throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sqlQuery)) {
            return getCatalogTable(ps.getMetaData(), typeMapper);
        }
    }

    public static CatalogTable getCatalogTable(Connection connection, String sqlQuery)
            throws SQLException {
        ResultSetMetaData resultSetMetaData;
        try (PreparedStatement ps = connection.prepareStatement(sqlQuery)) {
            resultSetMetaData = ps.getMetaData();
            return getCatalogTable(resultSetMetaData);
        }
    }
}
