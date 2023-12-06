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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgresql.utils;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import io.debezium.relational.Column;

public class PostgresTypeUtils {

    public static final String PG_SMALLSERIAL = "SMALLSERIAL";
    public static final String PG_SERIAL = "SERIAL";
    public static final String PG_BIGSERIAL = "BIGSERIAL";
    public static final String PG_BYTEA = "BYTEA";

    public static final String PG_BIT = "BIT";
    public static final String PG_BYTEA_ARRAY = "_BYTEA";
    public static final String PG_SMALLINT = "INT2";
    public static final String PG_SMALLINT_ARRAY = "_INT2";
    public static final String PG_INTEGER = "INT4";
    public static final String PG_INTEGER_ARRAY = "_INT4";
    public static final String PG_BIGINT = "INT8";
    public static final String PG_BIGINT_ARRAY = "_INT8";
    public static final String PG_REAL = "FLOAT4";
    public static final String PG_REAL_ARRAY = "_FLOAT4";
    public static final String PG_DOUBLE_PRECISION = "FLOAT8";
    public static final String PG_DOUBLE_PRECISION_ARRAY = "_FLOAT8";
    public static final String PG_NUMERIC = "NUMERIC";
    public static final String PG_NUMERIC_ARRAY = "_NUMERIC";
    public static final String PG_BOOLEAN = "BOOL";
    public static final String PG_BOOLEAN_ARRAY = "_BOOL";
    public static final String PG_TIMESTAMP = "TIMESTAMP";
    public static final String PG_TIMESTAMP_ARRAY = "_TIMESTAMP";
    public static final String PG_TIMESTAMPTZ = "TIMESTAMPTZ";
    public static final String PG_TIMESTAMPTZ_ARRAY = "_TIMESTAMPTZ";
    public static final String PG_DATE = "DATE";
    public static final String PG_DATE_ARRAY = "_DATE";
    public static final String PG_TIME = "TIME";
    public static final String PG_TIME_ARRAY = "_TIME";
    public static final String PG_TEXT = "TEXT";
    public static final String PG_TEXT_ARRAY = "_TEXT";
    public static final String PG_CHAR = "BPCHAR";
    public static final String PG_CHAR_ARRAY = "_BPCHAR";
    public static final String PG_CHARACTER = "CHARACTER";
    public static final String PG_CHARACTER_ARRAY = "_CHARACTER";
    public static final String PG_CHARACTER_VARYING = "VARCHAR";
    public static final String PG_CHARACTER_VARYING_ARRAY = "_VARCHAR";
    public static final String PG_INTERVAL = "INTERVAL";
    public static final String PG_GEOMETRY = "GEOMETRY";
    public static final String PG_GEOGRAPHY = "GEOGRAPHY";
    public static final String PG_JSON = "JSON";
    public static final String PG_JSONB = "JSONB";
    public static final String PG_XML = "XML";

    private PostgresTypeUtils() {}

    public static SeaTunnelDataType<?> convertFromColumn(Column column) {
        String typeName = column.typeName().toUpperCase();
        switch (typeName) {
            case PG_CHAR:
            case PG_CHARACTER:
            case PG_CHARACTER_VARYING:
            case PG_TEXT:
            case PG_INTERVAL:
            case PG_GEOMETRY:
            case PG_GEOGRAPHY:
            case PG_JSON:
            case PG_JSONB:
            case PG_XML:
                return BasicType.STRING_TYPE;
            case PG_BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case PG_BOOLEAN_ARRAY:
                return ArrayType.BOOLEAN_ARRAY_TYPE;
            case PG_BYTEA:
            case PG_BIT:
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
                return new DecimalType(column.length(), column.scale().orElse(0));
            case PG_CHAR_ARRAY:
            case PG_CHARACTER_ARRAY:
            case PG_CHARACTER_VARYING_ARRAY:
            case PG_TEXT_ARRAY:
                return ArrayType.STRING_ARRAY_TYPE;
            case PG_TIMESTAMP:
            case PG_TIMESTAMPTZ:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case PG_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case PG_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;

            case PG_TIMESTAMP_ARRAY:
            case PG_NUMERIC_ARRAY:
            case PG_TIMESTAMPTZ_ARRAY:
            case PG_TIME_ARRAY:
            case PG_DATE_ARRAY:
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Don't support Postgres type '%s' yet, jdbcType:'%s'.",
                                column.typeName(), column.jdbcType()));
        }
    }
}
