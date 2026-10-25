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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Maps PostgreSQL column metadata while retaining vector dimensions.
 *
 * <p>Only physical JDBC column provenance is trusted; query text is never used to guess the table
 * for expressions, joins or subqueries.
 */
@Slf4j
public class PostgresTypeMapper implements JdbcDialectTypeMapper {

    @Override
    public Column mappingColumn(BasicTypeDefine typeDefine) {
        return PostgresTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    public Column mappingColumn(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String columnName = metadata.getColumnLabel(colIndex);
        String nativeType = metadata.getColumnTypeName(colIndex);
        int isNullable = metadata.isNullable(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(nativeType)
                        .dataType(nativeType)
                        .sqlType(metadata.getColumnType(colIndex))
                        .nullable(isNullable == ResultSetMetaData.columnNullable)
                        .length((long) precision)
                        .precision((long) precision)
                        .scale(scale)
                        .build();
        return mappingColumn(typeDefine);
    }

    /**
     * Overrides the default mapping to resolve vector dimension from pg_attribute when the column
     * type is {@code vector}. ResultSetMetaData does not carry the dimension for pgvector columns,
     * so we read the column's {@code atttypmod}, which pgvector stores as the dimension directly.
     * SQL text is never parsed to infer a result column's physical table.
     */
    @Override
    public Column mappingColumn(
            ResultSetMetaData metadata, int colIndex, Connection connection, String sqlQuery)
            throws SQLException {
        String nativeType = metadata.getColumnTypeName(colIndex);

        if (PostgresTypeConverter.PG_VECTOR.equalsIgnoreCase(nativeType) && connection != null) {
            String[] tableInfo = resolveTableInfo(metadata, colIndex);
            if (tableInfo != null && tableInfo[1] != null && !tableInfo[1].isEmpty()) {
                // Use getColumnName (physical column name) for pg_attribute lookup,
                // as pg_attribute.attname stores the original column name, not the SELECT alias.
                String physicalColumnName = metadata.getColumnName(colIndex);
                String vectorType =
                        queryVectorTypeFromPgAttribute(
                                connection, tableInfo[0], tableInfo[1], physicalColumnName);
                if (vectorType != null) {
                    // Use getColumnLabel (SELECT alias or column name) for the output column name
                    String outputColumnName = metadata.getColumnLabel(colIndex);
                    BasicTypeDefine typeDefine =
                            BasicTypeDefine.builder()
                                    .name(outputColumnName)
                                    .columnType(vectorType)
                                    .dataType(nativeType)
                                    .sqlType(metadata.getColumnType(colIndex))
                                    .nullable(
                                            metadata.isNullable(colIndex)
                                                    == ResultSetMetaData.columnNullable)
                                    .length((long) metadata.getPrecision(colIndex))
                                    .precision((long) metadata.getPrecision(colIndex))
                                    .scale(metadata.getScale(colIndex))
                                    .build();
                    return mappingColumn(typeDefine);
                }
            }
        }

        return mappingColumn(metadata, colIndex);
    }

    /**
     * Resolves the physical schema and table from JDBC column metadata. SQL text is deliberately
     * not parsed: joins, subqueries and expressions do not reliably identify a result column's
     * physical table. Returns null when the driver cannot provide this provenance; callers then
     * retain the unconstrained vector type rather than guessing a dimension.
     */
    private String[] resolveTableInfo(ResultSetMetaData metadata, int colIndex) {
        // Try ResultSetMetaData first - the PostgreSQL JDBC driver often populates this.
        try {
            String tableName = metadata.getTableName(colIndex);
            if (tableName != null && !tableName.isEmpty()) {
                String schemaName = metadata.getSchemaName(colIndex);
                return new String[] {schemaName, tableName};
            }
        } catch (SQLException e) {
            log.debug("JDBC driver cannot resolve the physical table for column {}", colIndex, e);
        }

        // SQL text is not a reliable source of column provenance for joins or subqueries.
        // Retain an unconstrained vector when the driver cannot identify its physical table.
        log.warn(
                "Cannot resolve vector dimension because JDBC metadata has no physical table for column {}",
                colIndex);
        return null;
    }

    /**
     * Reads pgvector's dimension directly from pg_attribute. Building the known type name locally
     * avoids search_path-dependent qualification by format_type for extension-owned types.
     * Schema-scoped matching avoids tables with the same name in different schemas.
     */
    private String queryVectorTypeFromPgAttribute(
            Connection connection, String schemaName, String tableName, String columnName) {
        String sql =
                "SELECT a.atttypmod "
                        + "FROM pg_attribute a "
                        + "JOIN pg_class c ON a.attrelid = c.oid "
                        + "JOIN pg_namespace n ON c.relnamespace = n.oid "
                        + "WHERE c.relname = ? AND n.nspname = COALESCE(NULLIF(?, ''), current_schema()) AND a.attname = ? AND a.attnum > 0";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, tableName);
            ps.setString(2, schemaName);
            ps.setString(3, columnName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    int dimension = rs.getInt(1);
                    if (!rs.wasNull() && dimension > 0) {
                        return PostgresTypeConverter.PG_VECTOR + "(" + dimension + ")";
                    }
                }
            }
        } catch (SQLException e) {
            log.debug(
                    "Failed to query vector type from pg_attribute for {}.{}.{}",
                    schemaName,
                    tableName,
                    columnName,
                    e);
        }
        return null;
    }
}
