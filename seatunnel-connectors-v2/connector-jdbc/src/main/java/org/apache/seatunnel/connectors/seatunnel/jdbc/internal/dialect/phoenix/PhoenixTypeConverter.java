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

import lombok.extern.slf4j.Slf4j;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier.PHOENIX;

// reference https://phoenix.apache.org/language/datatypes.html
@Slf4j
public class PhoenixTypeConverter implements TypeConverter<BasicTypeDefine> {

    public static final String PHOENIX_UNKNOWN = "UNKNOWN";
    public static final String PHOENIX_BOOLEAN = "BOOLEAN";
    public static final String PHOENIX_ARRAY = "ARRAY";

    // -------------------------number----------------------------
    public static final String PHOENIX_TINYINT = "TINYINT";
    public static final String PHOENIX_UNSIGNED_TINYINT = "UNSIGNED_TINYINT";
    public static final String PHOENIX_SMALLINT = "SMALLINT";
    public static final String PHOENIX_UNSIGNED_SMALLINT = "UNSIGNED_SMALLINT";
    public static final String PHOENIX_UNSIGNED_INT = "UNSIGNED_INT";
    public static final String PHOENIX_INTEGER = "INTEGER";
    public static final String PHOENIX_BIGINT = "BIGINT";
    public static final String PHOENIX_UNSIGNED_LONG = "UNSIGNED_LONG";
    public static final String PHOENIX_DECIMAL = "DECIMAL";
    public static final String PHOENIX_FLOAT = "FLOAT";
    public static final String PHOENIX_UNSIGNED_FLOAT = "UNSIGNED_FLOAT";
    public static final String PHOENIX_DOUBLE = "DOUBLE";
    public static final String PHOENIX_UNSIGNED_DOUBLE = "UNSIGNED_DOUBLE";

    // -------------------------string----------------------------
    public static final String PHOENIX_CHAR = "CHAR";
    public static final String PHOENIX_VARCHAR = "VARCHAR";

    // ------------------------------time-------------------------
    public static final String PHOENIX_DATE = "DATE";
    public static final String PHOENIX_TIME = "TIME";
    public static final String PHOENIX_TIMESTAMP = "TIMESTAMP";
    public static final String PHOENIX_DATE_UNSIGNED = "UNSIGNED_DATE";
    public static final String PHOENIX_TIME_UNSIGNED = "UNSIGNED_TIME";
    public static final String PHOENIX_TIMESTAMP_UNSIGNED = "UNSIGNED_TIMESTAMP";

    // ------------------------------blob-------------------------
    public static final String PHOENIX_BINARY = "BINARY";
    public static final String PHOENIX_VARBINARY = "VARBINARY";

    public static final int MAX_PRECISION = 1000;
    public static final int DEFAULT_PRECISION = 38;
    public static final int MAX_SCALE = MAX_PRECISION - 1;
    public static final int DEFAULT_SCALE = 18;
    public static final int MAX_TIME_SCALE = 6;
    public static final int MAX_TIMESTAMP_SCALE = 6;
    public static final int MAX_VARCHAR_LENGTH = 10485760;

    public static final PhoenixTypeConverter INSTANCE = new PhoenixTypeConverter();

    @Override
    public String identifier() {
        return PHOENIX;
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

        String phoenixDataType = typeDefine.getDataType().toUpperCase();
        switch (phoenixDataType) {
            case PHOENIX_BOOLEAN:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case PHOENIX_ARRAY:
                builder.dataType(ArrayType.STRING_ARRAY_TYPE);
                break;
            case PHOENIX_TINYINT:
            case PHOENIX_UNSIGNED_TINYINT:
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case PHOENIX_SMALLINT:
            case PHOENIX_UNSIGNED_SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case PHOENIX_INTEGER:
            case PHOENIX_UNSIGNED_INT:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case PHOENIX_BIGINT:
            case PHOENIX_UNSIGNED_LONG:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case PHOENIX_DECIMAL:
            case PHOENIX_FLOAT:
            case PHOENIX_UNSIGNED_FLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case PHOENIX_DOUBLE:
            case PHOENIX_UNSIGNED_DOUBLE:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case PHOENIX_CHAR:
            case PHOENIX_VARCHAR:
                builder.dataType(BasicType.STRING_TYPE);
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.columnLength(1L);
                    builder.sourceType(phoenixDataType);
                } else {
                    builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                    builder.sourceType(
                            String.format("%s(%s)", phoenixDataType, typeDefine.getLength()));
                }
                break;
            case PHOENIX_DATE:
            case PHOENIX_DATE_UNSIGNED:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case PHOENIX_TIME:
            case PHOENIX_TIME_UNSIGNED:
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                break;
            case PHOENIX_TIMESTAMP:
            case PHOENIX_TIMESTAMP_UNSIGNED:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                break;
            case PHOENIX_BINARY:
            case PHOENIX_VARBINARY:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        PHOENIX, typeDefine.getDataType(), typeDefine.getName());
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
                builder.columnType(PHOENIX_BOOLEAN);
                builder.dataType(PHOENIX_BOOLEAN);
                break;
            case TINYINT:
                builder.columnType(PHOENIX_TINYINT);
                builder.dataType(PHOENIX_TINYINT);
            case SMALLINT:
                builder.columnType(PHOENIX_SMALLINT);
                builder.dataType(PHOENIX_SMALLINT);
                break;
            case INT:
                builder.columnType(PHOENIX_INTEGER);
                builder.dataType(PHOENIX_INTEGER);
                break;
            case BIGINT:
                builder.columnType(PHOENIX_BIGINT);
                builder.dataType(PHOENIX_BIGINT);
                break;
            case FLOAT:
                builder.columnType(PHOENIX_FLOAT);
                builder.dataType(PHOENIX_FLOAT);
                break;
            case DOUBLE:
                builder.columnType(PHOENIX_DOUBLE);
                builder.dataType(PHOENIX_DOUBLE);
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

                builder.columnType(String.format("%s(%s,%s)", PHOENIX_DECIMAL, precision, scale));
                builder.dataType(PHOENIX_DECIMAL);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case BYTES:
                builder.columnType(PHOENIX_BINARY);
                builder.dataType(PHOENIX_BINARY);
                break;
            case STRING:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(String.format("%s", PHOENIX_VARCHAR));
                } else if (column.getColumnLength() <= Integer.MAX_VALUE) {
                    builder.columnType(
                            String.format("%s(%s)", PHOENIX_VARCHAR, column.getColumnLength()));
                } else if (column.getColumnLength() > Integer.MAX_VALUE) {
                    builder.columnType(String.format("%s(%s)", PHOENIX_VARCHAR, Integer.MAX_VALUE));
                }

                builder.dataType(PHOENIX_VARCHAR);
                break;
            case DATE:
                builder.columnType(PHOENIX_DATE);
                builder.dataType(PHOENIX_DATE);
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
                    builder.columnType(String.format("%s(%s)", PHOENIX_TIME, timeScale));
                } else {
                    builder.columnType(PHOENIX_TIME);
                }
                builder.dataType(PHOENIX_TIME);
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
                    builder.columnType(String.format("%s(%s)", PHOENIX_TIMESTAMP, timestampScale));
                } else {
                    builder.columnType(PHOENIX_TIMESTAMP);
                }
                builder.dataType(PHOENIX_TIMESTAMP);
                builder.scale(timestampScale);
                break;
            case ARRAY:
                ArrayType arrayType = (ArrayType) column.getDataType();
                SeaTunnelDataType elementType = arrayType.getElementType();
                switch (elementType.getSqlType()) {
                    case BOOLEAN:
                        builder.columnType(PHOENIX_BOOLEAN + " " + PHOENIX_ARRAY);
                        builder.dataType(PHOENIX_BOOLEAN + " " + PHOENIX_ARRAY);
                        break;
                    case TINYINT:
                        builder.columnType(PHOENIX_TINYINT + " " + PHOENIX_ARRAY);
                        builder.dataType(PHOENIX_TINYINT + " " + PHOENIX_ARRAY);
                        break;
                    case SMALLINT:
                        builder.columnType(PHOENIX_SMALLINT + " " + PHOENIX_ARRAY);
                        builder.dataType(PHOENIX_SMALLINT + " " + PHOENIX_ARRAY);
                        break;
                    case INT:
                        builder.columnType(PHOENIX_INTEGER + " " + PHOENIX_ARRAY);
                        builder.dataType(PHOENIX_INTEGER + " " + PHOENIX_ARRAY);
                        break;
                    case BIGINT:
                        builder.columnType(PHOENIX_BIGINT + " " + PHOENIX_ARRAY);
                        builder.dataType(PHOENIX_BIGINT + " " + PHOENIX_ARRAY);
                        break;
                    case FLOAT:
                        builder.columnType(PHOENIX_FLOAT + " " + PHOENIX_ARRAY);
                        builder.dataType(PHOENIX_FLOAT + " " + PHOENIX_ARRAY);
                        break;
                    case DOUBLE:
                        builder.columnType(PHOENIX_DOUBLE + " " + PHOENIX_ARRAY);
                        builder.dataType(PHOENIX_DOUBLE + " " + PHOENIX_ARRAY);
                        break;
                    case STRING:
                        builder.columnType(PHOENIX_VARCHAR + " " + PHOENIX_ARRAY);
                        builder.dataType(PHOENIX_VARCHAR + " " + PHOENIX_ARRAY);
                        break;
                    default:
                        throw CommonError.convertToConnectorTypeError(
                                PHOENIX, elementType.getSqlType().name(), column.getName());
                }
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        PHOENIX, column.getDataType().getSqlType().name(), column.getName());
        }

        return builder.build();
    }
}
