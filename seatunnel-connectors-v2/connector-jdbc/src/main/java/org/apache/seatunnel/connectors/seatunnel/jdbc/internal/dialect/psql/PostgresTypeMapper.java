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
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

@Slf4j
public class PostgresTypeMapper implements JdbcDialectTypeMapper {

    private static final String VECTOR_DIM_QUERY =
            "SELECT a.atttypmod FROM pg_attribute a "
                    + "JOIN pg_class c ON a.attrelid = c.oid "
                    + "JOIN pg_namespace n ON c.relnamespace = n.oid "
                    + "WHERE n.nspname = ? AND c.relname = ? AND a.attname = ? AND a.attnum > 0";

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
     * Overrides the default to enrich vector columns with their dimension from pg_attribute. JDBC
     * ResultSetMetaData.getColumnTypeName() returns "vector" without dimension, and getPrecision()
     * does not carry the vector dimension. This method queries pg_attribute to extract the actual
     * dimension from the typmod so that auto-create-sink DDL preserves vector(N) correctly.
     */
    @Override
    public Column mappingColumn(ResultSetMetaData metadata, int colIndex, Connection connection)
            throws SQLException {
        Column column = mappingColumn(metadata, colIndex);

        if (column.getDataType() == VectorType.VECTOR_FLOAT_TYPE
                && (column.getScale() == null || column.getScale() <= 0)
                && connection != null) {
            String tableName = metadata.getTableName(colIndex);
            String schemaName = metadata.getSchemaName(colIndex);
            String columnName = metadata.getColumnLabel(colIndex);

            if (tableName != null && !tableName.isEmpty()) {
                int dim =
                        queryVectorDimension(
                                connection,
                                schemaName != null && !schemaName.isEmpty() ? schemaName : "public",
                                tableName,
                                columnName);
                if (dim > 0) {
                    return PhysicalColumn.builder()
                            .name(column.getName())
                            .dataType(column.getDataType())
                            .sourceType(PostgresTypeConverter.PG_VECTOR + "(" + dim + ")")
                            .scale(dim)
                            .nullable(column.isNullable())
                            .defaultValue(column.getDefaultValue())
                            .comment(column.getComment())
                            .build();
                }
            }
        }
        return column;
    }

    private int queryVectorDimension(
            Connection connection, String schemaName, String tableName, String columnName) {
        try (PreparedStatement ps = connection.prepareStatement(VECTOR_DIM_QUERY)) {
            ps.setString(1, schemaName);
            ps.setString(2, tableName);
            ps.setString(3, columnName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    int typmod = rs.getInt(1);
                    // pgvector stores dimension as atttypmod - VARHDRSZ (4)
                    if (typmod > 4) {
                        return typmod - 4;
                    }
                }
            }
        } catch (SQLException e) {
            log.warn(
                    "Failed to query vector dimension for {}.{}.{}",
                    schemaName,
                    tableName,
                    columnName,
                    e);
        }
        return 0;
    }
}
