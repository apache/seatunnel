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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.phoenix;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class PhoenixTypeMapper implements JdbcDialectTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(PhoenixTypeMapper.class);

    // ============================data types=====================

    private static final String PHOENIX_UNKNOWN = "UNKNOWN";
    private static final String PHOENIX_BOOLEAN = "BOOLEAN";
    private static final String PHOENIX_ARRAY = "ARRAY";

    // -------------------------number----------------------------
    private static final String PHOENIX_TINYINT = "TINYINT";
    private static final String PHOENIX_UNSIGNED_TINYINT = "UNSIGNED_TINYINT";
    private static final String PHOENIX_SMALLINT = "SMALLINT";
    private static final String PHOENIX_UNSIGNED_SMALLINT = "UNSIGNED_SMALLINT";
    private static final String PHOENIX_UNSIGNED_INT = "UNSIGNED_INT";
    private static final String PHOENIX_INTEGER = "INTEGER";
    private static final String PHOENIX_BIGINT = "BIGINT";
    private static final String PHOENIX_UNSIGNED_LONG = "UNSIGNED_LONG";
    private static final String PHOENIX_DECIMAL = "DECIMAL";
    private static final String PHOENIX_FLOAT = "FLOAT";
    private static final String PHOENIX_UNSIGNED_FLOAT = "UNSIGNED_FLOAT";
    private static final String PHOENIX_DOUBLE = "DOUBLE";
    private static final String PHOENIX_UNSIGNED_DOUBLE = "UNSIGNED_DOUBLE";

    // -------------------------string----------------------------
    private static final String PHOENIX_CHAR = "CHAR";
    private static final String PHOENIX_VARCHAR = "VARCHAR";

    // ------------------------------time-------------------------
    private static final String PHOENIX_DATE = "DATE";
    private static final String PHOENIX_TIME = "TIME";
    private static final String PHOENIX_TIMESTAMP = "TIMESTAMP";
    private static final String PHOENIX_DATE_UNSIGNED = "UNSIGNED_DATE";
    private static final String PHOENIX_TIME_UNSIGNED = "UNSIGNED_TIME";
    private static final String PHOENIX_TIMESTAMP_UNSIGNED = "UNSIGNED_TIMESTAMP";

    // ------------------------------blob-------------------------
    private static final String PHOENIX_BINARY = "BINARY";
    private static final String PHOENIX_VARBINARY = "VARBINARY";

    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String phoenixType = metadata.getColumnTypeName(colIndex).toUpperCase();
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (phoenixType) {
            case PHOENIX_BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case PHOENIX_TINYINT:
            case PHOENIX_UNSIGNED_TINYINT:
                return BasicType.BYTE_TYPE;
            case PHOENIX_UNSIGNED_INT:
            case PHOENIX_INTEGER:
                return BasicType.INT_TYPE;
            case PHOENIX_UNSIGNED_SMALLINT:
            case PHOENIX_SMALLINT:
                return BasicType.SHORT_TYPE;
            case PHOENIX_BIGINT:
            case PHOENIX_UNSIGNED_LONG:
                return BasicType.LONG_TYPE;
            case PHOENIX_DECIMAL:
                return new DecimalType(precision, scale);
            case PHOENIX_FLOAT:
                return BasicType.FLOAT_TYPE;
            case PHOENIX_UNSIGNED_FLOAT:
                LOG.warn("{} will probably cause value overflow.", PHOENIX_UNSIGNED_FLOAT);
                return BasicType.FLOAT_TYPE;
            case PHOENIX_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case PHOENIX_UNSIGNED_DOUBLE:
                LOG.warn("{} will probably cause value overflow.", PHOENIX_UNSIGNED_DOUBLE);
                return BasicType.DOUBLE_TYPE;
            case PHOENIX_CHAR:
            case PHOENIX_VARCHAR:
                return BasicType.STRING_TYPE;
            case PHOENIX_DATE:
            case PHOENIX_DATE_UNSIGNED:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case PHOENIX_TIME:
            case PHOENIX_TIME_UNSIGNED:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case PHOENIX_TIMESTAMP_UNSIGNED:
            case PHOENIX_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case PHOENIX_VARBINARY:
            case PHOENIX_BINARY:
                return PrimitiveByteArrayType.INSTANCE;
                // Doesn't support yet
            case PHOENIX_UNKNOWN:
            case PHOENIX_ARRAY:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new JdbcConnectorException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format(
                                "Doesn't support PHOENIX type '%s' on column '%s'  yet.",
                                phoenixType, jdbcColumnName));
        }
    }
}
