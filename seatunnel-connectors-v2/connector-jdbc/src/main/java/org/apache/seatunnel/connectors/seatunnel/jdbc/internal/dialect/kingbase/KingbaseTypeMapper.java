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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.kingbase;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class KingbaseTypeMapper implements JdbcDialectTypeMapper {

    private static final String KB_SMALLSERIAL = "SMALLSERIAL";
    private static final String KB_SERIAL = "SERIAL";
    private static final String KB_BIGSERIAL = "BIGSERIAL";
    private static final String KB_BYTEA = "BYTEA";
    private static final String KB_BYTEA_ARRAY = "_BYTEA";
    private static final String KB_SMALLINT = "INT2";
    private static final String KB_SMALLINT_ARRAY = "_INT2";
    private static final String KB_INTEGER = "INT4";
    private static final String KB_INTEGER_ARRAY = "_INT4";
    private static final String KB_BIGINT = "INT8";
    private static final String KB_BIGINT_ARRAY = "_INT8";
    private static final String KB_REAL = "FLOAT4";
    private static final String KB_REAL_ARRAY = "_FLOAT4";
    private static final String KB_DOUBLE_PRECISION = "FLOAT8";
    private static final String KB_DOUBLE_PRECISION_ARRAY = "_FLOAT8";
    private static final String KB_NUMERIC = "NUMERIC";
    private static final String KB_NUMERIC_ARRAY = "_NUMERIC";
    private static final String KB_BOOLEAN = "BOOL";
    private static final String KB_BOOLEAN_ARRAY = "_BOOL";
    private static final String KB_TIMESTAMP = "TIMESTAMP";
    private static final String KB_TIMESTAMP_ARRAY = "_TIMESTAMP";
    private static final String KB_TIMESTAMPTZ = "TIMESTAMPTZ";
    private static final String KB_TIMESTAMPTZ_ARRAY = "_TIMESTAMPTZ";
    private static final String KB_DATE = "DATE";
    private static final String KB_DATE_ARRAY = "_DATE";
    private static final String KB_TIME = "TIME";
    private static final String KB_TIME_ARRAY = "_TIME";
    private static final String KB_TEXT = "TEXT";
    private static final String KB_TEXT_ARRAY = "_TEXT";
    private static final String KB_CHAR = "BPCHAR";
    private static final String KB_CHAR_ARRAY = "_BPCHAR";
    private static final String KB_CHARACTER = "CHARACTER";

    private static final String KB_CHARACTER_VARYING = "VARCHAR";
    private static final String KB_CHARACTER_VARYING_ARRAY = "_VARCHAR";
    private static final String KB_JSON = "JSON";
    private static final String KB_JSONB = "JSONB";

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {

        String kbType = metadata.getColumnTypeName(colIndex).toUpperCase();

        int precision = metadata.getPrecision(colIndex);

        switch (kbType) {
            case KB_BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case KB_SMALLINT:
                return BasicType.SHORT_TYPE;
            case KB_SMALLSERIAL:
            case KB_INTEGER:
            case KB_SERIAL:
                return BasicType.INT_TYPE;
            case KB_BIGINT:
            case KB_BIGSERIAL:
                return BasicType.LONG_TYPE;
            case KB_REAL:
                return BasicType.FLOAT_TYPE;
            case KB_DOUBLE_PRECISION:
                return BasicType.DOUBLE_TYPE;
            case KB_NUMERIC:
                // see SPARK-26538: handle numeric without explicit precision and scale.
                if (precision > 0) {
                    return new DecimalType(precision, metadata.getScale(colIndex));
                }
                return new DecimalType(38, 18);
            case KB_CHAR:
            case KB_CHARACTER:
            case KB_CHARACTER_VARYING:
            case KB_TEXT:
                return BasicType.STRING_TYPE;
            case KB_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case KB_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case KB_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case KB_CHAR_ARRAY:
            case KB_CHARACTER_VARYING_ARRAY:
            case KB_TEXT_ARRAY:
            case KB_DOUBLE_PRECISION_ARRAY:
            case KB_REAL_ARRAY:
            case KB_BIGINT_ARRAY:
            case KB_SMALLINT_ARRAY:
            case KB_INTEGER_ARRAY:
            case KB_BYTEA_ARRAY:
            case KB_BOOLEAN_ARRAY:
            case KB_TIMESTAMP_ARRAY:
            case KB_NUMERIC_ARRAY:
            case KB_TIMESTAMPTZ:
            case KB_TIMESTAMPTZ_ARRAY:
            case KB_TIME_ARRAY:
            case KB_DATE_ARRAY:
            case KB_JSONB:
            case KB_JSON:
            case KB_BYTEA:
            default:
                throw new JdbcConnectorException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format("Doesn't support KingBaseES type '%s' yet", kbType));
        }
    }
}
