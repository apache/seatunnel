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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.xugu;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

@Slf4j
public class XuguTypeMapper implements JdbcDialectTypeMapper {

    // ----------------------------number-------------------------
    private static final String XUGU_NUMERIC = "NUMERIC";
    private static final String XUGU_NUMBER = "NUMBER";
    private static final String XUGU_INTEGER = "INTEGER";
    private static final String XUGU_INT = "INT";
    private static final String XUGU_BIGINT = "BIGINT";
    private static final String XUGU_TINYINT = "TINYINT";
    private static final String XUGU_SMALLINT = "SMALLINT";
    private static final String XUGU_FLOAT = "FLOAT";
    private static final String XUGU_DOUBLE = "DOUBLE";

    // ----------------------------string-------------------------
    private static final String XUGU_CHAR = "CHAR";
    private static final String XUGU_NCHAR = "NCHAR";
    private static final String XUGU_VARCHAR = "VARCHAR";
    private static final String XUGU_VARCHAR2 = "VARCHAR2";
    private static final String XUGU_CLOB = "CLOB";

    // ------------------------------time-------------------------
    private static final String XUGU_DATE = "DATE";
    private static final String XUGU_TIME = "TIME";
    private static final String XUGU_TIMESTAMP = "TIMESTAMP";
    private static final String XUGU_DATETIME = "DATETIME";
    private static final String XUGU_TIME_WITH_TIME_ZONE = "TIME WITH TIME ZONE";
    private static final String XUGU_TIMESTAMP_WITH_TIME_ZONE = "TIMESTAMP WITH TIME ZONE";

    // ---------------------------binary---------------------------
    private static final String XUGU_BINARY = "BINARY";
    private static final String XUGU_BLOB = "BLOB";

    // ---------------------------other---------------------------
    private static final String XUGU_GUID = "GUID";
    private static final String XUGU_BOOLEAN = "BOOLEAN";

    // -------------------------time interval-----------------------
    private static final String XUGU_INTERVAL_YEAR_TO_MONTH = "INTERVAL YEAR TO MONTH";
    private static final String XUGU_INTERVAL_YEAR = "INTERVAL YEAR";
    private static final String XUGU_INTERVAL_MONTH = "INTERVAL MONTH";
    private static final String XUGU_INTERVAL_DAY = "INTERVAL DAY";
    private static final String XUGU_INTERVAL_DAY_TO_HOUR = "INTERVAL DAY TO HOUR";
    private static final String XUGU_INTERVAL_DAY_TO_MINUTE = "INTERVAL DAY TO MINUTE";
    private static final String XUGU_INTERVAL_DAY_TO_SECOND = "INTERVAL DAY TO SECOND";
    private static final String XUGU_INTERVAL_HOUR = "INTERVAL HOUR";
    private static final String XUGU_INTERVAL_HOUR_TO_MINUTE = "INTERVAL HOUR TO MINUTE";
    private static final String XUGU_INTERVAL_HOUR_TO_SECOND = "INTERVAL HOUR TO SECOND";
    private static final String XUGU_INTERVAL_MINUTE = "INTERVAL MINUTE";
    private static final String XUGU_INTERVAL_MINUTE_TO_SECOND = "INTERVAL MINUTE TO SECOND";
    private static final String XUGU_INTERVAL_SECOND = "INTERVAL SECOND";

    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String xuguType = metadata.getColumnTypeName(colIndex).toUpperCase();
        int precision = metadata.getPrecision(colIndex);
        switch (xuguType) {
            case XUGU_BOOLEAN:
                return BasicType.BOOLEAN_TYPE;

            case XUGU_INT:
            case XUGU_INTEGER:
                return BasicType.INT_TYPE;

            case XUGU_TINYINT:
                return BasicType.BYTE_TYPE;

            case XUGU_SMALLINT:
                return BasicType.SHORT_TYPE;

            case XUGU_BIGINT:
            case XUGU_NUMBER:
                return BasicType.LONG_TYPE;

            case XUGU_NUMERIC:
                if (precision > 0) {
                    return new DecimalType(precision, metadata.getScale(colIndex));
                }
                return new DecimalType(38, 18);

            case XUGU_FLOAT:
            case XUGU_DOUBLE:
                return BasicType.DOUBLE_TYPE;

            case XUGU_CHAR:
            case XUGU_NCHAR:
            case XUGU_VARCHAR:
            case XUGU_VARCHAR2:
            case XUGU_CLOB:
            case XUGU_GUID:
                return BasicType.STRING_TYPE;

            case XUGU_TIMESTAMP:
            case XUGU_DATETIME:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;

            case XUGU_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;

            case XUGU_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;

            case XUGU_BLOB:
            case XUGU_BINARY:
                return PrimitiveByteArrayType.INSTANCE;

                // Doesn't support yet
            case XUGU_INTERVAL_YEAR_TO_MONTH:
            case XUGU_INTERVAL_YEAR:
            case XUGU_INTERVAL_MONTH:
            case XUGU_INTERVAL_DAY:
            case XUGU_INTERVAL_DAY_TO_HOUR:
            case XUGU_INTERVAL_DAY_TO_MINUTE:
            case XUGU_INTERVAL_DAY_TO_SECOND:
            case XUGU_INTERVAL_HOUR:
            case XUGU_INTERVAL_HOUR_TO_MINUTE:
            case XUGU_INTERVAL_HOUR_TO_SECOND:
            case XUGU_INTERVAL_MINUTE:
            case XUGU_INTERVAL_MINUTE_TO_SECOND:
            case XUGU_INTERVAL_SECOND:
            case XUGU_TIME_WITH_TIME_ZONE:
            case XUGU_TIMESTAMP_WITH_TIME_ZONE:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.XUGU, xuguType, jdbcColumnName);
        }
    }
}
