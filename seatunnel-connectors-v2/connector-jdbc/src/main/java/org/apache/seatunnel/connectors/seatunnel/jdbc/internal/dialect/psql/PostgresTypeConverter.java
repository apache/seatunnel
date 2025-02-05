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

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.common.source.TypeDefineUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

// reference http://www.postgres.cn/docs/13/datatype.html
@Slf4j
@AutoService(TypeConverter.class)
public class PostgresTypeConverter implements TypeConverter<BasicTypeDefine> {

    // Postgres jdbc driver maps several alias to real type, we use real type rather than alias:
    // boolean <=> bool
    public static final String PG_BOOLEAN = "bool";
    // bool[] <=> boolean[] <=> _bool
    public static final String PG_BOOLEAN_ARRAY = "_bool";
    public static final String PG_BYTEA = "bytea";
    // smallint <=> smallserial <=> int2
    public static final String PG_SMALLINT = "int2";
    public static final String PG_SMALLSERIAL = "smallserial";
    // smallint[] <=> int2[] <=> _int2
    public static final String PG_SMALLINT_ARRAY = "_int2";
    // integer <=> serial <=> int <=> int4
    public static final String PG_INTEGER = "int4";
    public static final String PG_SERIAL = "serial";
    // integer[] <=> int[] <=> _int4
    public static final String PG_INTEGER_ARRAY = "_int4";
    // bigint <=> bigserial <=> int8
    public static final String PG_BIGINT = "int8";
    public static final String PG_BIGSERIAL = "bigserial";
    // bigint[] <=> _int8
    public static final String PG_BIGINT_ARRAY = "_int8";
    // real <=> float4
    public static final String PG_REAL = "float4";
    // real[] <=> _float4
    public static final String PG_REAL_ARRAY = "_float4";
    // double precision <=> float8
    public static final String PG_DOUBLE_PRECISION = "float8";
    // double precision[] <=> _float8
    public static final String PG_DOUBLE_PRECISION_ARRAY = "_float8";
    // numeric <=> decimal
    public static final String PG_NUMERIC = "numeric";

    // money
    public static final String PG_MONEY = "money";

    // char <=> character <=> bpchar
    public static final String PG_CHAR = "char";
    public static final String PG_BPCHAR = "bpchar";
    public static final String PG_CHARACTER = "character";
    // char[] <=> _character <=> _bpchar
    public static final String PG_CHAR_ARRAY = "_bpchar";
    // character varying <=> varchar
    public static final String PG_VARCHAR = "varchar";
    public static final String PG_INET = "inet";
    public static final String PG_CHARACTER_VARYING = "character varying";
    // character varying[] <=> varchar[] <=> _varchar
    public static final String PG_VARCHAR_ARRAY = "_varchar";
    public static final String PG_TEXT = "text";
    public static final String PG_TEXT_ARRAY = "_text";
    public static final String PG_JSON = "json";
    public static final String PG_JSONB = "jsonb";
    public static final String PG_XML = "xml";
    public static final String PG_UUID = "uuid";
    private static final String PG_GEOMETRY = "geometry";
    private static final String PG_GEOGRAPHY = "geography";
    public static final String PG_DATE = "date";
    // time without time zone <=> time
    public static final String PG_TIME = "time";
    // time with time zone <=> timetz
    public static final String PG_TIME_TZ = "timetz";
    // timestamp without time zone <=> timestamp
    public static final String PG_TIMESTAMP = "timestamp";
    // timestamp with time zone <=> timestamptz
    public static final String PG_TIMESTAMP_TZ = "timestamptz";

    public static final int MAX_PRECISION = 1000;
    public static final int DEFAULT_PRECISION = 38;
    public static final int MAX_SCALE = MAX_PRECISION - 1;
    public static final int DEFAULT_SCALE = 18;
    public static final int MAX_TIME_SCALE = 6;
    public static final int MAX_TIMESTAMP_SCALE = 6;
    public static final int MAX_VARCHAR_LENGTH = 10485760;
    public static final PostgresTypeConverter INSTANCE = new PostgresTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.POSTGRESQL;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .sourceType(typeDefine.getColumnType())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());

        String pgDataType = typeDefine.getDataType().toLowerCase();
        switch (pgDataType) {
            case PG_BOOLEAN:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case PG_BOOLEAN_ARRAY:
                builder.dataType(ArrayType.BOOLEAN_ARRAY_TYPE);
                break;
            case PG_SMALLSERIAL:
            case PG_SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case PG_SMALLINT_ARRAY:
                builder.dataType(ArrayType.SHORT_ARRAY_TYPE);
                break;
            case PG_INTEGER:
            case PG_SERIAL:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case PG_INTEGER_ARRAY:
                builder.dataType(ArrayType.INT_ARRAY_TYPE);
                break;
            case PG_BIGINT:
            case PG_BIGSERIAL:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case PG_BIGINT_ARRAY:
                builder.dataType(ArrayType.LONG_ARRAY_TYPE);
                break;
            case PG_REAL:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case PG_REAL_ARRAY:
                builder.dataType(ArrayType.FLOAT_ARRAY_TYPE);
                break;
            case PG_DOUBLE_PRECISION:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case PG_DOUBLE_PRECISION_ARRAY:
                builder.dataType(ArrayType.DOUBLE_ARRAY_TYPE);
                break;
            case PG_NUMERIC:
                DecimalType decimalType;
                if (typeDefine.getPrecision() != null && typeDefine.getPrecision() > 0) {
                    decimalType =
                            new DecimalType(
                                    typeDefine.getPrecision().intValue(), typeDefine.getScale());
                } else {
                    decimalType = new DecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
                }
                builder.dataType(decimalType);
                break;
            case PG_MONEY:
                // -92233720368547758.08 to +92233720368547758.07, With the sign bit it's 20, we use
                // 30 precision to save it
                DecimalType moneyDecimalType;
                moneyDecimalType = new DecimalType(30, 2);
                builder.dataType(moneyDecimalType);
                builder.columnLength(30L);
                builder.scale(2);
                break;
            case PG_CHAR:
            case PG_BPCHAR:
            case PG_CHARACTER:
                builder.dataType(BasicType.STRING_TYPE);
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.columnLength(TypeDefineUtils.charTo4ByteLength(1L));
                    builder.sourceType(pgDataType);
                } else {
                    builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                    builder.sourceType(String.format("%s(%s)", pgDataType, typeDefine.getLength()));
                }
                break;
            case PG_VARCHAR:
            case PG_CHARACTER_VARYING:
                builder.dataType(BasicType.STRING_TYPE);
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.sourceType(pgDataType);
                } else {
                    builder.sourceType(String.format("%s(%s)", pgDataType, typeDefine.getLength()));
                    builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                }
                break;
            case PG_TEXT:
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case PG_UUID:
                builder.dataType(BasicType.STRING_TYPE);
                builder.sourceType(pgDataType);
                builder.columnLength(128L);
                break;
            case PG_JSON:
            case PG_JSONB:
            case PG_XML:
            case PG_GEOMETRY:
            case PG_GEOGRAPHY:
            case PG_INET:
                builder.dataType(BasicType.STRING_TYPE);
                builder.sourceType(pgDataType);
                break;
            case PG_CHAR_ARRAY:
            case PG_VARCHAR_ARRAY:
            case PG_TEXT_ARRAY:
                builder.dataType(ArrayType.STRING_ARRAY_TYPE);
                break;
            case PG_BYTEA:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                break;
            case PG_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case PG_TIME:
            case PG_TIME_TZ:
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                if (typeDefine.getScale() != null && typeDefine.getScale() > MAX_TIME_SCALE) {
                    builder.scale(MAX_TIME_SCALE);
                    log.warn(
                            "The scale of time type is larger than {}, it will be truncated to {}",
                            MAX_TIME_SCALE,
                            MAX_TIME_SCALE);
                } else {
                    builder.scale(typeDefine.getScale());
                }
                break;
            case PG_TIMESTAMP:
            case PG_TIMESTAMP_TZ:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                if (typeDefine.getScale() != null && typeDefine.getScale() > MAX_TIMESTAMP_SCALE) {
                    builder.scale(MAX_TIMESTAMP_SCALE);
                    log.warn(
                            "The scale of timestamp type is larger than {}, it will be truncated to {}",
                            MAX_TIMESTAMP_SCALE,
                            MAX_TIMESTAMP_SCALE);
                } else {
                    builder.scale(typeDefine.getScale());
                }
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        identifier(), typeDefine.getDataType(), typeDefine.getName());
        }
        return builder.build();
    }

    @Override
    public BasicTypeDefine reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder =
                BasicTypeDefine.builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());
        switch (column.getDataType().getSqlType()) {
            case BOOLEAN:
                builder.columnType(PG_BOOLEAN);
                builder.dataType(PG_BOOLEAN);
                break;
            case TINYINT:
            case SMALLINT:
                builder.columnType(PG_SMALLINT);
                builder.dataType(PG_SMALLINT);
                break;
            case INT:
                builder.columnType(PG_INTEGER);
                builder.dataType(PG_INTEGER);
                break;
            case BIGINT:
                builder.columnType(PG_BIGINT);
                builder.dataType(PG_BIGINT);
                break;
            case FLOAT:
                builder.columnType(PG_REAL);
                builder.dataType(PG_REAL);
                break;
            case DOUBLE:
                builder.columnType(PG_DOUBLE_PRECISION);
                builder.dataType(PG_DOUBLE_PRECISION);
                break;
            case DECIMAL:
                if (column.getSourceType() != null
                        && column.getSourceType().equalsIgnoreCase(PG_MONEY)) {
                    builder.columnType(PG_MONEY);
                    builder.dataType(PG_MONEY);
                } else {
                    DecimalType decimalType = (DecimalType) column.getDataType();
                    long precision = decimalType.getPrecision();
                    int scale = decimalType.getScale();
                    if (precision <= 0) {
                        precision = DEFAULT_PRECISION;
                        scale = DEFAULT_SCALE;
                        log.warn(
                                "The decimal column {} type decimal({},{}) is out of range, "
                                        + "which is precision less than 0, "
                                        + "it will be converted to decimal({},{})",
                                column.getName(),
                                decimalType.getPrecision(),
                                decimalType.getScale(),
                                precision,
                                scale);
                    } else if (precision > MAX_PRECISION) {
                        scale = (int) Math.max(0, scale - (precision - MAX_PRECISION));
                        precision = MAX_PRECISION;
                        log.warn(
                                "The decimal column {} type decimal({},{}) is out of range, "
                                        + "which exceeds the maximum precision of {}, "
                                        + "it will be converted to decimal({},{})",
                                column.getName(),
                                decimalType.getPrecision(),
                                decimalType.getScale(),
                                MAX_PRECISION,
                                precision,
                                scale);
                    }
                    if (scale < 0) {
                        scale = 0;
                        log.warn(
                                "The decimal column {} type decimal({},{}) is out of range, "
                                        + "which is scale less than 0, "
                                        + "it will be converted to decimal({},{})",
                                column.getName(),
                                decimalType.getPrecision(),
                                decimalType.getScale(),
                                precision,
                                scale);
                    } else if (scale > MAX_SCALE) {
                        scale = MAX_SCALE;
                        log.warn(
                                "The decimal column {} type decimal({},{}) is out of range, "
                                        + "which exceeds the maximum scale of {}, "
                                        + "it will be converted to decimal({},{})",
                                column.getName(),
                                decimalType.getPrecision(),
                                decimalType.getScale(),
                                MAX_SCALE,
                                precision,
                                scale);
                    }

                    builder.columnType(String.format("%s(%s,%s)", PG_NUMERIC, precision, scale));
                    builder.dataType(PG_NUMERIC);
                    builder.precision(precision);
                    builder.scale(scale);
                }
                break;
            case BYTES:
                builder.columnType(PG_BYTEA);
                builder.dataType(PG_BYTEA);
                break;
            case STRING:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(PG_TEXT);
                    builder.dataType(PG_TEXT);
                } else if (column.getColumnLength() <= MAX_VARCHAR_LENGTH) {
                    builder.columnType(
                            String.format("%s(%s)", PG_VARCHAR, column.getColumnLength()));
                    builder.dataType(PG_VARCHAR);
                } else {
                    builder.columnType(PG_TEXT);
                    builder.dataType(PG_TEXT);
                }
                break;
            case DATE:
                builder.columnType(PG_DATE);
                builder.dataType(PG_DATE);
                break;
            case TIME:
                Integer timeScale = column.getScale();
                if (timeScale != null && timeScale > MAX_TIME_SCALE) {
                    timeScale = MAX_TIME_SCALE;
                    log.warn(
                            "The time column {} type time({}) is out of range, "
                                    + "which exceeds the maximum scale of {}, "
                                    + "it will be converted to time({})",
                            column.getName(),
                            column.getScale(),
                            MAX_SCALE,
                            timeScale);
                }
                if (timeScale != null && timeScale > 0) {
                    builder.columnType(String.format("%s(%s)", PG_TIME, timeScale));
                } else {
                    builder.columnType(PG_TIME);
                }
                builder.dataType(PG_TIME);
                builder.scale(timeScale);
                break;
            case TIMESTAMP:
                Integer timestampScale = column.getScale();
                if (timestampScale != null && timestampScale > MAX_TIMESTAMP_SCALE) {
                    timestampScale = MAX_TIMESTAMP_SCALE;
                    log.warn(
                            "The timestamp column {} type timestamp({}) is out of range, "
                                    + "which exceeds the maximum scale of {}, "
                                    + "it will be converted to timestamp({})",
                            column.getName(),
                            column.getScale(),
                            MAX_TIMESTAMP_SCALE,
                            timestampScale);
                }
                if (timestampScale != null && timestampScale > 0) {
                    builder.columnType(String.format("%s(%s)", PG_TIMESTAMP, timestampScale));
                } else {
                    builder.columnType(PG_TIMESTAMP);
                }
                builder.dataType(PG_TIMESTAMP);
                builder.scale(timestampScale);
                break;
            case ARRAY:
                ArrayType arrayType = (ArrayType) column.getDataType();
                SeaTunnelDataType elementType = arrayType.getElementType();
                switch (elementType.getSqlType()) {
                    case BOOLEAN:
                        builder.columnType(PG_BOOLEAN_ARRAY);
                        builder.dataType(PG_BOOLEAN_ARRAY);
                        break;
                    case TINYINT:
                    case SMALLINT:
                        builder.columnType(PG_SMALLINT_ARRAY);
                        builder.dataType(PG_SMALLINT_ARRAY);
                        break;
                    case INT:
                        builder.columnType(PG_INTEGER_ARRAY);
                        builder.dataType(PG_INTEGER_ARRAY);
                        break;
                    case BIGINT:
                        builder.columnType(PG_BIGINT_ARRAY);
                        builder.dataType(PG_BIGINT_ARRAY);
                        break;
                    case FLOAT:
                        builder.columnType(PG_REAL_ARRAY);
                        builder.dataType(PG_REAL_ARRAY);
                        break;
                    case DOUBLE:
                        builder.columnType(PG_DOUBLE_PRECISION_ARRAY);
                        builder.dataType(PG_DOUBLE_PRECISION_ARRAY);
                        break;
                    case BYTES:
                        builder.columnType(PG_BYTEA);
                        builder.dataType(PG_BYTEA);
                        break;
                    case STRING:
                        builder.columnType(PG_TEXT_ARRAY);
                        builder.dataType(PG_TEXT_ARRAY);
                        break;
                    default:
                        throw CommonError.convertToConnectorTypeError(
                                DatabaseIdentifier.POSTGRESQL,
                                elementType.getSqlType().name(),
                                column.getName());
                }
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.POSTGRESQL,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }

        return builder.build();
    }
}
