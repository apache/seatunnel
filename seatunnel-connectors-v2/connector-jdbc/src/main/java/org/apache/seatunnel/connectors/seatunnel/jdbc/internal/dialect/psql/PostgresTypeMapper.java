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

import org.apache.seatunnel.api.table.type.ArrayType;
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

public class PostgresTypeMapper implements JdbcDialectTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDialectTypeMapper.class);

    // Postgres jdbc driver maps several alias to real type, we use real type rather than alias:
    // serial2 <=> int2
    // smallserial <=> int2
    // serial4 <=> serial
    // serial8 <=> bigserial
    // smallint <=> int2
    // integer <=> int4
    // int <=> int4
    // bigint <=> int8
    // float <=> float8
    // boolean <=> bool
    // decimal <=> numeric
    private static final String PG_SMALLSERIAL = "smallserial";
    private static final String PG_SERIAL = "serial";
    private static final String PG_BIGSERIAL = "bigserial";
    private static final String PG_BYTEA = "bytea";
    private static final String PG_BYTEA_ARRAY = "_bytea";
    private static final String PG_SMALLINT = "int2";
    private static final String PG_SMALLINT_ARRAY = "_int2";
    private static final String PG_INTEGER = "int4";
    private static final String PG_INTEGER_ARRAY = "_int4";
    private static final String PG_BIGINT = "int8";
    private static final String PG_BIGINT_ARRAY = "_int8";
    private static final String PG_REAL = "float4";
    private static final String PG_REAL_ARRAY = "_float4";
    private static final String PG_DOUBLE_PRECISION = "float8";
    private static final String PG_DOUBLE_PRECISION_ARRAY = "_float8";
    private static final String PG_NUMERIC = "numeric";
    private static final String PG_NUMERIC_ARRAY = "_numeric";
    private static final String PG_BOOLEAN = "bool";
    private static final String PG_BOOLEAN_ARRAY = "_bool";
    private static final String PG_TIMESTAMP = "timestamp";
    private static final String PG_TIMESTAMP_ARRAY = "_timestamp";
    private static final String PG_TIMESTAMPTZ = "timestamptz";
    private static final String PG_TIMESTAMPTZ_ARRAY = "_timestamptz";
    private static final String PG_DATE = "date";
    private static final String PG_DATE_ARRAY = "_date";
    private static final String PG_TIME = "time";
    private static final String PG_TIME_ARRAY = "_time";
    private static final String PG_TEXT = "text";
    private static final String PG_TEXT_ARRAY = "_text";
    private static final String PG_CHAR = "bpchar";
    private static final String PG_CHAR_ARRAY = "_bpchar";
    private static final String PG_CHARACTER = "character";
    private static final String PG_CHARACTER_ARRAY = "_character";
    private static final String PG_CHARACTER_VARYING = "varchar";
    private static final String PG_CHARACTER_VARYING_ARRAY = "_varchar";
    private static final String PG_GEOMETRY = "geometry";
    private static final String PG_GEOGRAPHY = "geography";

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {

        String pgType = metadata.getColumnTypeName(colIndex);

        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        switch (pgType) {
            case PG_BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case PG_BOOLEAN_ARRAY:
                return ArrayType.BOOLEAN_ARRAY_TYPE;
            case PG_BYTEA:
                return PrimitiveByteArrayType.INSTANCE;
            case PG_BYTEA_ARRAY:
                return ArrayType.BYTE_ARRAY_TYPE;
            case PG_SMALLINT:
            case PG_SMALLSERIAL:
            case PG_INTEGER:
            case PG_SERIAL:
                return BasicType.INT_TYPE;
            case PG_SMALLINT_ARRAY:
            case PG_INTEGER_ARRAY:
                return ArrayType.INT_ARRAY_TYPE;
            case PG_BIGINT:
            case PG_BIGSERIAL:
                return BasicType.LONG_TYPE;
            case PG_BIGINT_ARRAY:
                return ArrayType.LONG_ARRAY_TYPE;
            case PG_REAL:
                return BasicType.FLOAT_TYPE;
            case PG_REAL_ARRAY:
                return ArrayType.FLOAT_ARRAY_TYPE;
            case PG_DOUBLE_PRECISION:
                return BasicType.DOUBLE_TYPE;
            case PG_DOUBLE_PRECISION_ARRAY:
                return ArrayType.DOUBLE_ARRAY_TYPE;
            case PG_NUMERIC:
                // see SPARK-26538: handle numeric without explicit precision and scale.
                if (precision > 0) {
                    return new DecimalType(precision, metadata.getScale(colIndex));
                }
                return new DecimalType(38, 18);
            case PG_CHAR:
            case PG_CHARACTER:
            case PG_CHARACTER_VARYING:
            case PG_TEXT:
            case PG_GEOMETRY:
            case PG_GEOGRAPHY:
                return BasicType.STRING_TYPE;
            case PG_CHAR_ARRAY:
            case PG_CHARACTER_ARRAY:
            case PG_CHARACTER_VARYING_ARRAY:
            case PG_TEXT_ARRAY:
                return ArrayType.STRING_ARRAY_TYPE;
            case PG_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case PG_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case PG_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;

            case PG_TIMESTAMP_ARRAY:
            case PG_NUMERIC_ARRAY:
            case PG_TIMESTAMPTZ:
            case PG_TIMESTAMPTZ_ARRAY:
            case PG_TIME_ARRAY:
            case PG_DATE_ARRAY:
            default:
                throw new JdbcConnectorException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format("Doesn't support Postgres type '%s' yet", pgType));
        }
    }
}
