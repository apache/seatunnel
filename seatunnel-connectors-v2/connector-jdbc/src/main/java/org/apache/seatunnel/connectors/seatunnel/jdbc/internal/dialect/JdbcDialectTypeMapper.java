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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.io.Serializable;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.sql.Types.BINARY;
import static java.sql.Types.BLOB;
import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NCLOB;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

/** Separate the jdbc meta-information type to SeaTunnelDataType into the interface. */
public interface JdbcDialectTypeMapper extends Serializable {

    /**
     * @deprecated instead by {@link #mappingColumn(BasicTypeDefine)}
     * @param metadata
     * @param colIndex
     * @return
     * @throws SQLException
     */
    @Deprecated
    default SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
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
        return mappingColumn(typeDefine).getDataType();
    }

    default Column mappingColumn(BasicTypeDefine typeDefine) {
        throw new UnsupportedOperationException();
    }

    default List<Column> mappingColumn(
            DatabaseMetaData metadata,
            String catalog,
            String schemaPattern,
            String tableNamePattern,
            String columnNamePattern)
            throws SQLException {
        List<Column> columns = new ArrayList<>();
        try (ResultSet rs =
                metadata.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String nativeType = rs.getString("TYPE_NAME");
                int sqlType = rs.getInt("DATA_TYPE");
                int columnSize = rs.getInt("COLUMN_SIZE");
                int decimalDigits = rs.getInt("DECIMAL_DIGITS");
                int nullable = rs.getInt("NULLABLE");
                String comment = rs.getString("REMARKS");

                BasicTypeDefine typeDefine =
                        BasicTypeDefine.builder()
                                .name(columnName)
                                .columnType(nativeType)
                                .dataType(nativeType)
                                .sqlType(sqlType)
                                .length((long) columnSize)
                                .precision((long) columnSize)
                                .scale(decimalDigits)
                                .nullable(nullable == DatabaseMetaData.columnNullable)
                                .comment(comment)
                                .build();
                columns.add(mappingColumn(typeDefine));
            }
        }
        return columns;
    }

    default List<Column> mappingColumn(ResultSetMetaData metadata) throws SQLException {
        List<Column> columns = new ArrayList<>();
        for (int index = 1; index <= metadata.getColumnCount(); index++) {
            Column column = mappingColumn(metadata, index);
            columns.add(column);
        }
        return columns;
    }

    default Column mappingColumn(ResultSetMetaData metadata, int colIndex) throws SQLException {
        /**
         * TODO The mapping method should be replaced by {@link #mappingColumn(BasicTypeDefine)}.
         */
        SeaTunnelDataType seaTunnelType = mapping(metadata, colIndex);

        String columnName = metadata.getColumnLabel(colIndex);
        int jdbcType = metadata.getColumnType(colIndex);
        String nativeType = metadata.getColumnTypeName(colIndex);
        int isNullable = metadata.isNullable(colIndex);
        int precision = metadata.getPrecision(colIndex);

        int columnLength = precision;
        long longColumnLength = precision;
        long bitLength = 0;
        switch (jdbcType) {
            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
            case BLOB:
                bitLength = precision * 8;
                break;
            case CHAR:
            case VARCHAR:
            case LONGVARCHAR:
            case NCHAR:
            case NVARCHAR:
            case LONGNVARCHAR:
            case CLOB:
            case NCLOB:
                columnLength = precision * 3;
                longColumnLength = precision * 3;
                break;
            default:
                break;
        }

        return PhysicalColumn.of(
                columnName,
                seaTunnelType,
                columnLength,
                isNullable != ResultSetMetaData.columnNoNulls,
                null,
                null,
                nativeType,
                false,
                false,
                bitLength,
                Collections.emptyMap(),
                longColumnLength);
    }
}
