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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlite;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

@Slf4j
public class SqliteTypeMapper implements JdbcDialectTypeMapper {

    private static final String OBJECT_CLASS = "java.lang.Object";
    private static final String CHARACTER_CLASS = "java.lang.Character";
    private static final String STRING_CLASS = "java.lang.String";
    private static final String INTEGER_CLASS = "java.lang.Integer";
    private static final String SHORT_CLASS = "java.lang.Short";
    private static final String LONG_CLASS = "java.lang.Long";
    private static final String FLOAT_CLASS = "java.lang.Float";
    private static final String DOUBLE_CLASS = "java.lang.Double";
    private static final String BOOLEAN_CLASS = "java.lang.Boolean";
    private static final String BYTE_CLASS = "java.lang.Byte";

    /**
     * because of sqlite's dynamic data type and affinity, use columnType(java.sql.Types) determine the SeaTunnel Data Types
     */
    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex) throws SQLException {
        int columnType = metadata.getColumnType(colIndex);
        String columnClassName = metadata.getColumnClassName(colIndex);
        // sqlite data type is dynamic and type affinity, see https://www.sqlite.org/datatype3.html
        // 1. if columnClassName is java.lang.Object, this column has no data, use columnType determine the SeaTunnel Data Types
        // 2. otherwise we just use columnClassName to determine the SeaTunnel Data Types
        if (columnClassName.equalsIgnoreCase(OBJECT_CLASS)) {   // case 1.
            switch (columnType) {
                case Types.CHAR:
                case Types.NCHAR:
                case Types.VARCHAR:
                case Types.NVARCHAR:
                case Types.LONGVARCHAR:
                case Types.LONGNVARCHAR:
                    return BasicType.STRING_TYPE;
                case Types.INTEGER:
                    return BasicType.INT_TYPE;
                case Types.TINYINT:
                case Types.SMALLINT:
                    return BasicType.SHORT_TYPE;
                case Types.BIGINT:
                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                case Types.TIME_WITH_TIMEZONE:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    return BasicType.LONG_TYPE;
                case Types.FLOAT:
                    return BasicType.FLOAT_TYPE;
                case Types.DOUBLE:
                case Types.REAL:
                case Types.NUMERIC:
                case Types.DECIMAL:
                    return BasicType.DOUBLE_TYPE;
                case Types.BOOLEAN:
                case Types.BIT:
                    return BasicType.BOOLEAN_TYPE;
                case Types.BLOB:
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    return PrimitiveByteArrayType.INSTANCE;
                default:
                    final String jdbcColumnName = metadata.getColumnName(colIndex);
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Doesn't support sql type '%s' on column '%s'  yet.",
                                    columnType, jdbcColumnName));
            }
        } else {    // case 2
            switch (columnClassName) {
                case INTEGER_CLASS:
                    return BasicType.INT_TYPE;
                case SHORT_CLASS:
                    return BasicType.SHORT_TYPE;
                case LONG_CLASS:
                    return BasicType.LONG_TYPE;
                case FLOAT_CLASS:
                    return BasicType.FLOAT_TYPE;
                case DOUBLE_CLASS:
                    return BasicType.DOUBLE_TYPE;
                case STRING_CLASS:
                case CHARACTER_CLASS:
                    return BasicType.STRING_TYPE;
                case BOOLEAN_CLASS:
                    return BasicType.BOOLEAN_TYPE;
                case BYTE_CLASS:
                    return BasicType.BYTE_TYPE;
                default:
                    final String jdbcColumnName = metadata.getColumnName(colIndex);
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Doesn't support sql type '%s' on column '%s'  yet.",
                                    columnType, jdbcColumnName));
            }
        }
    }
}
