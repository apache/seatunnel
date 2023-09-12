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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class DmdbTypeMapper implements JdbcDialectTypeMapper {

    // ============================data types=====================
    private static final String DM_BIT = "BIT";

    // ----------------------------number-------------------------
    private static final String DM_NUMERIC = "NUMERIC";
    private static final String DM_NUMBER = "NUMBER";
    private static final String DM_DECIMAL = "DECIMAL";
    /** same to DECIMAL */
    private static final String DM_DEC = "DEC";

    // ----------------------------int-----------------------------
    private static final String DM_INTEGER = "INTEGER";
    private static final String DM_INT = "INT";
    public static final String DM_PLS_INTEGER = "PLS_INTEGER";
    private static final String DM_BIGINT = "BIGINT";
    private static final String DM_TINYINT = "TINYINT";
    private static final String DM_BYTE = "BYTE";
    private static final String DM_SMALLINT = "SMALLINT";

    // dm float is double for Cpp.
    private static final String DM_FLOAT = "FLOAT";
    private static final String DM_DOUBLE = "DOUBLE";
    private static final String DM_DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final String DM_REAL = "REAL";

    // DM_CHAR DM_CHARACTER DM_VARCHAR DM_VARCHAR2 max is 32767
    private static final String DM_CHAR = "CHAR";
    private static final String DM_CHARACTER = "CHARACTER";
    private static final String DM_VARCHAR = "VARCHAR";
    private static final String DM_VARCHAR2 = "VARCHAR2";
    private static final String DM_LONGVARCHAR = "LONGVARCHAR";
    private static final String DM_CLOB = "CLOB";
    private static final String DM_TEXT = "TEXT";
    private static final String DM_LONG = "LONG";

    // ------------------------------time-------------------------
    private static final String DM_DATE = "DATE";
    private static final String DM_TIME = "TIME";
    private static final String DM_TIMESTAMP = "TIMESTAMP";
    private static final String DM_DATETIME = "DATETIME";

    // ---------------------------binary---------------------------
    private static final String DM_BINARY = "BINARY";
    private static final String DM_VARBINARY = "VARBINARY";

    // -------------------------time interval-----------------------
    private static final String DM_INTERVAL_YEAR_TO_MONTH = "INTERVAL YEAR TO MONTH";
    private static final String DM_INTERVAL_YEAR = "INTERVAL YEAR";
    private static final String DM_INTERVAL_MONTH = "INTERVAL MONTH";
    private static final String DM_INTERVAL_DAY = "INTERVAL DAY";
    private static final String DM_INTERVAL_DAY_TO_HOUR = "INTERVAL DAY TO HOUR";
    private static final String DM_INTERVAL_DAY_TO_MINUTE = "INTERVAL DAY TO MINUTE";
    private static final String DM_INTERVAL_DAY_TO_SECOND = "INTERVAL DAY TO SECOND";
    private static final String DM_INTERVAL_HOUR = "INTERVAL HOUR";
    private static final String DM_INTERVAL_HOUR_TO_MINUTE = "INTERVAL HOUR TO MINUTE";
    private static final String DM_INTERVAL_HOUR_TO_SECOND = "INTERVAL HOUR TO SECOND";
    private static final String DM_INTERVAL_MINUTE = "INTERVAL MINUTE";
    private static final String DM_INTERVAL_MINUTE_TO_SECOND = "INTERVAL MINUTE TO SECOND";
    private static final String DM_INTERVAL_SECOND = "INTERVAL SECOND";
    // time zone
    private static final String DM_TIME_WITH_TIME_ZONE = "TIME WITH TIME ZONE";
    private static final String DM_TIMESTAMP_WITH_TIME_ZONE = "TIMESTAMP WITH TIME ZONE";
    private static final String TIMESTAMP_WITH_LOCAL_TIME_ZONE = "TIMESTAMP WITH LOCAL TIME ZONE";

    // ------------------------------blob-------------------------
    public static final String DM_BLOB = "BLOB";
    public static final String DM_BFILE = "BFILE";
    public static final String DM_IMAGE = "IMAGE";
    public static final String DM_LONGVARBINARY = "LONGVARBINARY";

    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String dmdbType = metadata.getColumnTypeName(colIndex).toUpperCase();
        int precision = metadata.getPrecision(colIndex);
        switch (dmdbType) {
            case DM_BIT:
                return BasicType.BOOLEAN_TYPE;

            case DM_INT:
            case DM_INTEGER:
            case DM_PLS_INTEGER:
                return BasicType.INT_TYPE;

            case DM_TINYINT:
            case DM_BYTE:
                return BasicType.BYTE_TYPE;

            case DM_SMALLINT:
                return BasicType.SHORT_TYPE;

            case DM_BIGINT:
                return BasicType.LONG_TYPE;

            case DM_NUMERIC:
            case DM_NUMBER:
            case DM_DECIMAL:
            case DM_DEC:
                if (precision > 0) {
                    return new DecimalType(precision, metadata.getScale(colIndex));
                }
                return new DecimalType(38, 18);

            case DM_REAL:
                return BasicType.FLOAT_TYPE;

            case DM_FLOAT:
            case DM_DOUBLE_PRECISION:
            case DM_DOUBLE:
                return BasicType.DOUBLE_TYPE;

            case DM_CHAR:
            case DM_CHARACTER:
            case DM_VARCHAR:
            case DM_VARCHAR2:
                // 100G-1 byte
            case DM_TEXT:
            case DM_LONG:
            case DM_LONGVARCHAR:
            case DM_CLOB:
                return BasicType.STRING_TYPE;

            case DM_TIMESTAMP:
            case DM_DATETIME:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;

            case DM_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;

            case DM_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;

                // 100G-1 byte
            case DM_BLOB:
            case DM_BINARY:
            case DM_VARBINARY:
            case DM_LONGVARBINARY:
            case DM_IMAGE:
            case DM_BFILE:
                return PrimitiveByteArrayType.INSTANCE;

                // Doesn't support yet
            case DM_INTERVAL_YEAR_TO_MONTH:
            case DM_INTERVAL_YEAR:
            case DM_INTERVAL_MONTH:
            case DM_INTERVAL_DAY:
            case DM_INTERVAL_DAY_TO_HOUR:
            case DM_INTERVAL_DAY_TO_MINUTE:
            case DM_INTERVAL_DAY_TO_SECOND:
            case DM_INTERVAL_HOUR:
            case DM_INTERVAL_HOUR_TO_MINUTE:
            case DM_INTERVAL_HOUR_TO_SECOND:
            case DM_INTERVAL_MINUTE:
            case DM_INTERVAL_MINUTE_TO_SECOND:
            case DM_INTERVAL_SECOND:
            case DM_TIME_WITH_TIME_ZONE:
            case DM_TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new JdbcConnectorException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format(
                                "Doesn't support Dmdb type '%s' on column '%s'  yet.",
                                dmdbType, jdbcColumnName));
        }
    }
}
