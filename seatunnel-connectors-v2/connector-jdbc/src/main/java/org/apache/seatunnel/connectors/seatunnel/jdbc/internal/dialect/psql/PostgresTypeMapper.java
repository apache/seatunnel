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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PostgresTypeMapper implements JdbcDialectTypeMapper {

    private static final Pattern FROM_TABLE_PATTERN =
            Pattern.compile("(?i)\\bFROM\\s+([\\w.\"`]+)");

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
     * so we query {@code format_type(atttypid, atttypmod)} to obtain the full type string (e.g.
     * "vector(3)").
     */
    @Override
    public Column mappingColumn(
            ResultSetMetaData metadata, int colIndex, Connection connection, String sqlQuery)
            throws SQLException {
        String nativeType = metadata.getColumnTypeName(colIndex);

        if (PostgresTypeConverter.PG_VECTOR.equalsIgnoreCase(nativeType) && connection != null) {
            String[] tableInfo = resolveTableInfo(metadata, colIndex, sqlQuery);
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
     * Resolves the schema name and table name by first checking ResultSetMetaData, then falling
     * back to parsing the SQL query. Returns a two-element String array: [schemaName, tableName].
     * schemaName may be null if it cannot be determined.
     */
    private String[] resolveTableInfo(ResultSetMetaData metadata, int colIndex, String sqlQuery) {
        // Try ResultSetMetaData first — the PostgreSQL JDBC driver often populates this
        try {
            String tableName = metadata.getTableName(colIndex);
            if (tableName != null && !tableName.isEmpty()) {
                String schemaName = metadata.getSchemaName(colIndex);
                return new String[] {schemaName, tableName};
            }
        } catch (SQLException ignored) {
            // driver may not support it
        }

        // Fall back to parsing FROM clause from the query
        if (sqlQuery != null) {
            Matcher matcher = FROM_TABLE_PATTERN.matcher(sqlQuery);
            if (matcher.find()) {
                String fullName = matcher.group(1);
                // Strip any quoting
                fullName = fullName.replace("\"", "").replace("`", "");
                int lastDot = fullName.lastIndexOf('.');
                if (lastDot >= 0) {
                    String schemaName = fullName.substring(0, lastDot);
                    String tableName = fullName.substring(lastDot + 1);
                    return new String[] {schemaName, tableName};
                } else {
                    return new String[] {null, fullName};
                }
            }
        }
        return null;
    }

    /**
     * Queries pg_attribute to get the full vector type string (e.g. "vector(3)") for a specific
     * column. Uses pg_namespace to ensure correct schema-scoped matching, avoiding ambiguity when
     * tables with the same name exist in different schemas.
     */
    private String queryVectorTypeFromPgAttribute(
            Connection connection, String schemaName, String tableName, String columnName) {
        String sql =
                "SELECT format_type(a.atttypid, a.atttypmod) "
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
                    String fullType = rs.getString(1);
                    if (fullType != null
                            && fullType.toLowerCase()
                                    .startsWith(PostgresTypeConverter.PG_VECTOR + "(")) {
                        return fullType;
                    }
                }
            }
        } catch (SQLException e) {
            LOG.debug(
                    "Failed to query vector type from pg_attribute for {}.{}.{}",
                    schemaName,
                    tableName,
                    columnName,
                    e);
        }
        return null;
    }
}
