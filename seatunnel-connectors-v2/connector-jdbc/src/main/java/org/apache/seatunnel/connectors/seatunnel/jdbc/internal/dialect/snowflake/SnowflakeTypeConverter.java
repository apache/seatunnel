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

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.common.source.TypeDefineUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

// reference https://docs.snowflake.com/en/sql-reference/intro-summary-data-types
@Slf4j
@AutoService(TypeConverter.class)
public class SnowflakeTypeConverter implements TypeConverter<BasicTypeDefine> {

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

    public static final SnowflakeTypeConverter INSTANCE = new SnowflakeTypeConverter();
    public static final int MAX_PRECISION = 38;
    public static final int MAX_SCALE = 37;

    public static final int DEFAULT_PRECISION = 10;
    public static final int DEFAULT_SCALE = 0;

    @Override
    public String identifier() {
        return DatabaseIdentifier.SNOWFLAKE;
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
        String dataType = typeDefine.getDataType().toUpperCase();
        switch (dataType) {
            case SNOWFLAKE_SMALLINT:
            case SNOWFLAKE_TINYINT:
            case SNOWFLAKE_BYTEINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case SNOWFLAKE_INTEGER:
            case SNOWFLAKE_INT:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case SNOWFLAKE_BIGINT:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case SNOWFLAKE_DECIMAL:
            case SNOWFLAKE_NUMERIC:
            case SNOWFLAKE_NUMBER:
                builder.dataType(
                        new DecimalType(
                                Math.toIntExact(
                                        typeDefine.getPrecision() == null
                                                ? DEFAULT_PRECISION
                                                : typeDefine.getPrecision()),
                                typeDefine.getScale() == null
                                        ? DEFAULT_SCALE
                                        : typeDefine.getScale()));
                break;
            case SNOWFLAKE_REAL:
            case SNOWFLAKE_FLOAT4:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case SNOWFLAKE_DOUBLE:
            case SNOWFLAKE_DOUBLE_PRECISION:
            case SNOWFLAKE_FLOAT8:
            case SNOWFLAKE_FLOAT:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case SNOWFLAKE_BOOLEAN:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case SNOWFLAKE_CHAR:
            case SNOWFLAKE_CHARACTER:
            case SNOWFLAKE_VARCHAR:
            case SNOWFLAKE_STRING:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                break;
            case SNOWFLAKE_TEXT:
            case SNOWFLAKE_VARIANT:
            case SNOWFLAKE_OBJECT:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(typeDefine.getLength());
                break;
            case SNOWFLAKE_GEOGRAPHY:
            case SNOWFLAKE_GEOMETRY:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(typeDefine.getLength());
                break;
            case SNOWFLAKE_BINARY:
            case SNOWFLAKE_VARBINARY:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                break;
            case SNOWFLAKE_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case SNOWFLAKE_TIME:
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                builder.scale(9);
                break;
            case SNOWFLAKE_DATE_TIME:
            case SNOWFLAKE_TIMESTAMP:
            case SNOWFLAKE_TIMESTAMP_LTZ:
            case SNOWFLAKE_TIMESTAMP_NTZ:
            case SNOWFLAKE_TIMESTAMP_TZ:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(9);
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.SNOWFLAKE, dataType, typeDefine.getName());
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
            case TINYINT:
            case SMALLINT:
                builder.columnType(SNOWFLAKE_SMALLINT);
                builder.dataType(SNOWFLAKE_SMALLINT);
                break;
            case INT:
                builder.columnType(SNOWFLAKE_INTEGER);
                builder.dataType(SNOWFLAKE_INTEGER);
                break;
            case BIGINT:
                builder.columnType(SNOWFLAKE_BIGINT);
                builder.dataType(SNOWFLAKE_BIGINT);
                break;
            case DECIMAL:
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
                builder.columnType(String.format("%s(%s,%s)", SNOWFLAKE_DECIMAL, precision, scale));
                builder.dataType(SNOWFLAKE_DECIMAL);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case FLOAT:
                builder.columnType(SNOWFLAKE_FLOAT4);
                builder.dataType(SNOWFLAKE_FLOAT4);
                break;
            case DOUBLE:
                builder.columnType(SNOWFLAKE_DOUBLE_PRECISION);
                builder.dataType(SNOWFLAKE_DOUBLE_PRECISION);
                break;
            case BOOLEAN:
                builder.columnType(SNOWFLAKE_BOOLEAN);
                builder.dataType(SNOWFLAKE_BOOLEAN);
                break;
            case STRING:
                if (column.getColumnLength() != null) {
                    if (column.getColumnLength() > 16777216) {
                        builder.columnType(SNOWFLAKE_BINARY);
                        builder.dataType(SNOWFLAKE_BINARY);
                    } else if (column.getColumnLength() > 0) {
                        builder.columnType(
                                String.format(
                                        "%s(%s)", SNOWFLAKE_VARCHAR, column.getColumnLength()));
                        builder.dataType(SNOWFLAKE_VARCHAR);
                    } else {
                        builder.columnType(SNOWFLAKE_STRING);
                        builder.dataType(SNOWFLAKE_STRING);
                    }
                } else {
                    builder.columnType(SNOWFLAKE_STRING);
                    builder.dataType(SNOWFLAKE_STRING);
                }
                builder.length(column.getColumnLength());
                break;
            case DATE:
                builder.columnType(SNOWFLAKE_DATE);
                builder.dataType(SNOWFLAKE_DATE);
                break;
            case BYTES:
                builder.columnType(SNOWFLAKE_GEOMETRY);
                builder.dataType(SNOWFLAKE_GEOMETRY);
                break;
            case TIME:
                if (column.getScale() > 9) {
                    log.warn(
                            "The timestamp column {} type time({}) is out of range, "
                                    + "which exceeds the maximum scale of {}, "
                                    + "it will be converted to time({})",
                            column.getName(),
                            column.getScale(),
                            9,
                            9);
                }
                builder.columnType(SNOWFLAKE_TIME);
                builder.dataType(SNOWFLAKE_TIME);
                break;
            case TIMESTAMP:
                if (column.getScale() > 9) {
                    log.warn(
                            "The timestamp column {} type timestamp({}) is out of range, "
                                    + "which exceeds the maximum scale of {}, "
                                    + "it will be converted to timestamp({})",
                            column.getName(),
                            column.getScale(),
                            9,
                            9);
                }
                builder.columnType(SNOWFLAKE_TIMESTAMP);
                builder.dataType(SNOWFLAKE_TIMESTAMP);
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.SNOWFLAKE,
                        column.getDataType().getSqlType().toString(),
                        column.getName());
        }
        return builder.build();
    }
}
