/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql;

import org.apache.seatunnel.api.table.catalog.DataTypeConvertException;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import org.apache.commons.collections4.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@AutoService(DataTypeConvertor.class)
public class PostgresDataTypeConvertor implements DataTypeConvertor<String> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDialectTypeMapper.class);

    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";

    public static final Integer DEFAULT_PRECISION = 38;

    public static final Integer DEFAULT_SCALE = 18;

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
    public static final String PG_SMALLSERIAL = "smallserial";
    public static final String PG_SERIAL = "serial";
    public static final String PG_BIGSERIAL = "bigserial";
    public static final String PG_BYTEA = "bytea";

    public static final String PG_BIT = "bit";
    public static final String PG_BYTEA_ARRAY = "_bytea";
    public static final String PG_SMALLINT = "int2";
    public static final String PG_SMALLINT_ARRAY = "_int2";
    public static final String PG_INTEGER = "int4";
    public static final String PG_INTEGER_ARRAY = "_int4";
    public static final String PG_BIGINT = "int8";
    public static final String PG_BIGINT_ARRAY = "_int8";
    public static final String PG_REAL = "float4";
    public static final String PG_REAL_ARRAY = "_float4";
    public static final String PG_DOUBLE_PRECISION = "float8";
    public static final String PG_DOUBLE_PRECISION_ARRAY = "_float8";
    public static final String PG_NUMERIC = "numeric";
    public static final String PG_NUMERIC_ARRAY = "_numeric";
    public static final String PG_BOOLEAN = "bool";
    public static final String PG_BOOLEAN_ARRAY = "_bool";
    public static final String PG_TIMESTAMP = "timestamp";
    public static final String PG_TIMESTAMP_ARRAY = "_timestamp";
    public static final String PG_TIMESTAMPTZ = "timestamptz";
    public static final String PG_TIMESTAMPTZ_ARRAY = "_timestamptz";
    public static final String PG_DATE = "date";
    public static final String PG_DATE_ARRAY = "_date";
    public static final String PG_TIME = "time";
    public static final String PG_TIME_ARRAY = "_time";
    public static final String PG_TEXT = "text";
    public static final String PG_TEXT_ARRAY = "_text";
    public static final String PG_CHAR = "bpchar";
    public static final String PG_CHAR_ARRAY = "_bpchar";
    public static final String PG_CHARACTER = "character";
    public static final String PG_CHARACTER_ARRAY = "_character";
    public static final String PG_CHARACTER_VARYING = "varchar";
    public static final String PG_CHARACTER_VARYING_ARRAY = "_varchar";
    public static final String PG_INTERVAL = "interval";
    public static final String PG_GEOMETRY = "geometry";
    public static final String PG_GEOGRAPHY = "geography";
    public static final String PG_JSON = "json";
    public static final String PG_JSONB = "jsonb";

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String connectorDataType) {
        return toSeaTunnelType(connectorDataType, new HashMap<>(0));
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String connectorDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        checkNotNull(connectorDataType, "Postgres Type cannot be null");
        switch (connectorDataType) {
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
                int precision =
                        MapUtils.getInteger(dataTypeProperties, PRECISION, DEFAULT_PRECISION);
                ;
                int scale = MapUtils.getInteger(dataTypeProperties, SCALE, DEFAULT_SCALE);
                return new DecimalType(precision, scale);
            case PG_CHAR:
            case PG_CHARACTER:
            case PG_CHARACTER_VARYING:
            case PG_TEXT:
            case PG_INTERVAL:
            case PG_GEOMETRY:
            case PG_GEOGRAPHY:
            case PG_JSON:
            case PG_JSONB:
                return BasicType.STRING_TYPE;
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
                                "Doesn't support POSTGRES type '%s''  yet.", connectorDataType));
        }
    }

    @Override
    public String toConnectorType(
            SeaTunnelDataType<?> seaTunnelDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        checkNotNull(seaTunnelDataType, "seaTunnelDataType cannot be null");
        SqlType sqlType = seaTunnelDataType.getSqlType();
        switch (sqlType) {
            case TINYINT:
            case SMALLINT:
                return PG_SMALLINT;
            case INT:
                return PG_INTEGER;
            case BIGINT:
                return PG_BIGINT;
            case DECIMAL:
                return PG_NUMERIC;
            case FLOAT:
                return PG_REAL;
            case DOUBLE:
                return PG_DOUBLE_PRECISION;
            case BOOLEAN:
                return PG_BOOLEAN;
            case STRING:
                return PG_TEXT;
            case DATE:
                return PG_DATE;
            case BYTES:
                return PG_BYTEA;
            case TIME:
                return PG_TIME;
            case TIMESTAMP:
                return PG_TIMESTAMP;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support SeaTunnel type '%s''  yet.", seaTunnelDataType));
        }
    }

    @Override
    public String getIdentity() {
        return DatabaseIdentifier.POSTGRESQL;
    }
}
