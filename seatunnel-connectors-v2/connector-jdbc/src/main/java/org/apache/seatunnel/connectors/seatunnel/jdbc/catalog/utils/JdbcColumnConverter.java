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

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BIT;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NCLOB;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.REAL;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.TIME_WITH_TIMEZONE;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

/**
 * @deprecated instead by {@link
 *     org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper}
 */
@Deprecated
public class JdbcColumnConverter {

    public static List<Column> convert(DatabaseMetaData metadata, TablePath tablePath)
            throws SQLException {
        List<Column> columns = new ArrayList<>();

        try (ResultSet columnsResultSet =
                metadata.getColumns(
                        tablePath.getDatabaseName(),
                        tablePath.getSchemaName(),
                        tablePath.getTableName(),
                        null)) {

            while (columnsResultSet.next()) {
                String columnName = columnsResultSet.getString("COLUMN_NAME");
                int jdbcType = columnsResultSet.getInt("DATA_TYPE");
                String nativeType = columnsResultSet.getString("TYPE_NAME");
                int columnSize = columnsResultSet.getInt("COLUMN_SIZE");
                int decimalDigits = columnsResultSet.getInt("DECIMAL_DIGITS");
                int nullable = columnsResultSet.getInt("NULLABLE");
                String comment = columnsResultSet.getString("REMARKS");

                Column column =
                        convert(
                                columnName,
                                jdbcType,
                                nativeType,
                                nullable,
                                columnSize,
                                decimalDigits,
                                comment);
                columns.add(column);
            }
        }
        return columns;
    }

    public static Column convert(ResultSetMetaData metadata, int index) throws SQLException {
        String columnName = metadata.getColumnLabel(index);
        int jdbcType = metadata.getColumnType(index);
        String nativeType = metadata.getColumnTypeName(index);
        int isNullable = metadata.isNullable(index);
        int precision = metadata.getPrecision(index);
        int scale = metadata.getScale(index);
        return convert(columnName, jdbcType, nativeType, isNullable, precision, scale, null);
    }

    public static Column convert(
            String columnName,
            int jdbcType,
            String nativeType,
            int isNullable,
            int precision,
            int scale,
            String comment)
            throws SQLException {
        int columnLength = precision;
        long longColumnLength = precision;
        long bitLength = 0;
        SeaTunnelDataType seaTunnelType;

        switch (jdbcType) {
            case BOOLEAN:
                seaTunnelType = BasicType.BOOLEAN_TYPE;
                break;
            case BIT:
                if (precision == 1) {
                    seaTunnelType = BasicType.BOOLEAN_TYPE;
                } else {
                    seaTunnelType = PrimitiveByteArrayType.INSTANCE;
                }
                break;
            case TINYINT:
                seaTunnelType = BasicType.BYTE_TYPE;
                break;
            case SMALLINT:
                seaTunnelType = BasicType.SHORT_TYPE;
                break;
            case INTEGER:
                seaTunnelType = BasicType.INT_TYPE;
                break;
            case BIGINT:
                seaTunnelType = BasicType.LONG_TYPE;
                break;
            case FLOAT:
                seaTunnelType = BasicType.FLOAT_TYPE;
                break;
            case REAL:
                seaTunnelType = BasicType.DOUBLE_TYPE;
                break;
            case DOUBLE:
                seaTunnelType = BasicType.DOUBLE_TYPE;
                break;
            case NUMERIC:
            case DECIMAL:
                if (scale == 0) {
                    seaTunnelType = BasicType.LONG_TYPE;
                } else {
                    seaTunnelType = new DecimalType(precision, scale);
                }
                break;
            case CHAR:
            case VARCHAR:
            case LONGVARCHAR:
            case NCHAR:
            case NVARCHAR:
            case LONGNVARCHAR:
            case CLOB:
            case NCLOB:
                seaTunnelType = BasicType.STRING_TYPE;
                columnLength = precision * 3;
                longColumnLength = precision * 3;
                break;
            case DATE:
                seaTunnelType = LocalTimeType.LOCAL_DATE_TYPE;
                break;
            case TIME:
            case TIME_WITH_TIMEZONE:
                seaTunnelType = LocalTimeType.LOCAL_TIME_TYPE;
                break;
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIMEZONE:
                seaTunnelType = LocalTimeType.LOCAL_DATE_TIME_TYPE;
                break;
            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
            case BLOB:
                seaTunnelType = PrimitiveByteArrayType.INSTANCE;
                bitLength = precision * 8;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported JDBC type: " + jdbcType);
        }

        return PhysicalColumn.of(
                columnName,
                seaTunnelType,
                columnLength,
                isNullable != ResultSetMetaData.columnNoNulls,
                null,
                comment,
                nativeType,
                false,
                false,
                bitLength,
                Collections.emptyMap(),
                longColumnLength);
    }
}
