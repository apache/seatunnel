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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.hive;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class HiveTypeMapper implements JdbcDialectTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(HiveTypeMapper.class);

    // reference https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types

    // Numeric Types
    private static final String HIVE_TINYINT = "TINYINT";
    private static final String HIVE_SMALLINT = "SMALLINT";
    private static final String HIVE_INT = "INT";
    private static final String HIVE_INTEGER = "INTEGER";
    private static final String HIVE_BIGINT = "BIGINT";
    private static final String HIVE_FLOAT = "FLOAT";
    private static final String HIVE_DOUBLE = "DOUBLE";
    private static final String HIVE_DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final String HIVE_DECIMAL = "DECIMAL";
    private static final String HIVE_NUMERIC = "NUMERIC";
    // Date/Time Types
    private static final String HIVE_TIMESTAMP = "TIMESTAMP";
    private static final String HIVE_DATE = "DATE";
    private static final String HIVE_INTERVAL = "INTERVAL";
    // String Types
    private static final String HIVE_STRING = "STRING";
    private static final String HIVE_VARCHAR = "VARCHAR";
    private static final String HIVE_CHAR = "CHAR";
    // Misc Types
    private static final String HIVE_BOOLEAN = "BOOLEAN";
    private static final String HIVE_BINARY = "BINARY";
    // Complex Types
    private static final String HIVE_ARRAY = "ARRAY";
    private static final String HIVE_MAP = "MAP";
    private static final String HIVE_STRUCT = "STRUCT";
    private static final String HIVE_UNIONTYPE = "UNIONTYPE";

    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String columnType = metadata.getColumnTypeName(colIndex).toUpperCase();
        int precision = metadata.getPrecision(colIndex);
        switch (columnType) {
            case HIVE_TINYINT:
                return BasicType.BYTE_TYPE;
            case HIVE_SMALLINT:
                return BasicType.SHORT_TYPE;
            case HIVE_INT:
            case HIVE_INTEGER:
                return BasicType.INT_TYPE;
            case HIVE_BIGINT:
                return BasicType.LONG_TYPE;
            case HIVE_FLOAT:
                return BasicType.FLOAT_TYPE;
            case HIVE_DOUBLE:
            case HIVE_DOUBLE_PRECISION:
                return BasicType.DOUBLE_TYPE;
            case HIVE_DECIMAL:
            case HIVE_NUMERIC:
                if (precision > 0) {
                    return new DecimalType(precision, metadata.getScale(colIndex));
                }
                LOG.warn("decimal did define precision,scale, will be Decimal(38,18)");
                return new DecimalType(38, 18);
            case HIVE_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case HIVE_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case HIVE_STRING:
            case HIVE_VARCHAR:
            case HIVE_CHAR:
                return BasicType.STRING_TYPE;
            case HIVE_BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case HIVE_BINARY:
            case HIVE_ARRAY:
            case HIVE_INTERVAL:
            case HIVE_MAP:
            case HIVE_STRUCT:
            case HIVE_UNIONTYPE:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.HIVE, columnType, jdbcColumnName);
        }
    }
}
