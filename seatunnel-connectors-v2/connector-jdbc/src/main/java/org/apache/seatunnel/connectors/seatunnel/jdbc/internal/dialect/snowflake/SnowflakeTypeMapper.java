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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.snowflake;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

@Slf4j
public class SnowflakeTypeMapper implements JdbcDialectTypeMapper {

    /* ============================ data types ===================== */
    private static final String SNOWFLAKE_NUMBER = "NUMBER";
    private static final String SNOWFLAKE_DECIMAL = "DECIMAL";
    private static final String SNOWFLAKE_NUMERIC = "NUMERIC";
    private static final String SNOWFLAKE_INT = "INT";
    private static final String SNOWFLAKE_INTEGER = "INTEGER";
    private static final String SNOWFLAKE_BIGINT = "BIGINT";
    private static final String SNOWFLAKE_SMALLINT = "SMALLINT";
    private static final String SNOWFLAKE_TINYINT = "TINYINT";
    private static final String SNOWFLAKE_BYTEINT = "BYTEINT";

    private static final String SNOWFLAKE_FLOAT = "FLOAT";
    private static final String SNOWFLAKE_FLOAT4 = "FLOAT4";
    private static final String SNOWFLAKE_FLOAT8 = "FLOAT8";
    private static final String SNOWFLAKE_DOUBLE = "DOUBLE";
    private static final String SNOWFLAKE_DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final String SNOWFLAKE_REAL = "REAL";

    private static final String SNOWFLAKE_VARCHAR = "VARCHAR";
    private static final String SNOWFLAKE_CHAR = "CHAR";
    private static final String SNOWFLAKE_CHARACTER = "CHARACTER";
    private static final String SNOWFLAKE_STRING = "STRING";
    private static final String SNOWFLAKE_TEXT = "TEXT";
    private static final String SNOWFLAKE_BINARY = "BINARY";
    private static final String SNOWFLAKE_VARBINARY = "VARBINARY";

    private static final String SNOWFLAKE_BOOLEAN = "BOOLEAN";

    private static final String SNOWFLAKE_DATE = "DATE";
    private static final String SNOWFLAKE_DATE_TIME = "DATE_TIME";
    private static final String SNOWFLAKE_TIME = "TIME";
    private static final String SNOWFLAKE_TIMESTAMP = "TIMESTAMP";
    private static final String SNOWFLAKE_TIMESTAMP_LTZ = "TIMESTAMPLTZ";
    private static final String SNOWFLAKE_TIMESTAMP_NTZ = "TIMESTAMPNTZ";
    private static final String SNOWFLAKE_TIMESTAMP_TZ = "TIMESTAMPTZ";

    private static final String SNOWFLAKE_GEOGRAPHY = "GEOGRAPHY";
    private static final String SNOWFLAKE_GEOMETRY = "GEOMETRY";

    private static final String SNOWFLAKE_VARIANT = "VARIANT";
    private static final String SNOWFLAKE_OBJECT = "OBJECT";

    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String snowflakeType = metadata.getColumnTypeName(colIndex).toUpperCase();
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (snowflakeType) {
            case SNOWFLAKE_SMALLINT:
            case SNOWFLAKE_TINYINT:
            case SNOWFLAKE_BYTEINT:
                return BasicType.SHORT_TYPE;
            case SNOWFLAKE_INTEGER:
            case SNOWFLAKE_INT:
                return BasicType.INT_TYPE;
            case SNOWFLAKE_BIGINT:
                return BasicType.LONG_TYPE;
            case SNOWFLAKE_DECIMAL:
            case SNOWFLAKE_NUMERIC:
            case SNOWFLAKE_NUMBER:
                return new DecimalType(precision, scale);
            case SNOWFLAKE_REAL:
            case SNOWFLAKE_FLOAT4:
                return BasicType.FLOAT_TYPE;
            case SNOWFLAKE_DOUBLE:
            case SNOWFLAKE_DOUBLE_PRECISION:
            case SNOWFLAKE_FLOAT8:
            case SNOWFLAKE_FLOAT:
                return BasicType.DOUBLE_TYPE;
            case SNOWFLAKE_BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case SNOWFLAKE_CHAR:
            case SNOWFLAKE_CHARACTER:
            case SNOWFLAKE_VARCHAR:
            case SNOWFLAKE_STRING:
            case SNOWFLAKE_TEXT:
            case SNOWFLAKE_VARIANT:
            case SNOWFLAKE_OBJECT:
                return BasicType.STRING_TYPE;
            case SNOWFLAKE_GEOGRAPHY:
            case SNOWFLAKE_GEOMETRY:
                int geoMetaType = metadata.getColumnType(colIndex);
                switch (geoMetaType) {
                    case Types.BINARY:
                        return PrimitiveByteArrayType.INSTANCE;
                    case Types.VARCHAR:
                    default:
                        return BasicType.STRING_TYPE;
                }
            case SNOWFLAKE_BINARY:
            case SNOWFLAKE_VARBINARY:
                return PrimitiveByteArrayType.INSTANCE;
            case SNOWFLAKE_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case SNOWFLAKE_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case SNOWFLAKE_DATE_TIME:
            case SNOWFLAKE_TIMESTAMP:
            case SNOWFLAKE_TIMESTAMP_LTZ:
            case SNOWFLAKE_TIMESTAMP_NTZ:
            case SNOWFLAKE_TIMESTAMP_TZ:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support SNOWFLAKE type '%s' on column '%s'  yet.",
                                snowflakeType, jdbcColumnName));
        }
    }
}
