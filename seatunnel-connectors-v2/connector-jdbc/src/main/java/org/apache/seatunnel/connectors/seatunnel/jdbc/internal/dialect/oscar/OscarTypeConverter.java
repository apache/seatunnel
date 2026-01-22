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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oscar;

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

@Slf4j
@AutoService(TypeConverter.class)
public class OscarTypeConverter implements TypeConverter<BasicTypeDefine> {
    // ============================data types=====================
    public static final String OSCAR_BIT = "BIT";

    // ----------------------------int-----------------------------
    public static final String OSCAR_INT1 = "INT1";
    public static final String OSCAR_TINYINT = "TINYINT";

    public static final String OSCAR_INT2 = "INT2";
    public static final String OSCAR_SMALLINT = "SMALLINT";

    public static final String OSCAR_INT4 = "INT4";
    public static final String OSCAR_INT = "INT";
    public static final String OSCAR_INTEGER = "INTEGER";

    public static final String OSCAR_INT8 = "INT8";
    public static final String OSCAR_BIGINT = "BIGINT";

    // oscar float is double for Cpp.
    public static final String OSCAR_FLOAT4 = "FLOAT4";
    public static final String OSCAR_FLOAT8 = "FLOAT8";
    public static final String OSCAR_FLOAT = "FLOAT";
    public static final String OSCAR_DOUBLE = "DOUBLE";
    public static final String OSCAR_DOUBLE_PRECISION = "DOUBLE PRECISION";
    public static final String OSCAR_REAL = "REAL";

    // ----------------------------number-------------------------
    public static final String OSCAR_NUMERIC = "NUMERIC";
    public static final String OSCAR_NUMBER = "NUMBER";
    public static final String OSCAR_DECIMAL = "DECIMAL";
    // -------------------------char------------------------
    public static final String OSCAR_CHAR = "CHAR";
    public static final String OSCAR_BPCHAR = "BPCHAR";

    public static final String OSCAR_CHARACTER = "CHARACTER";
    public static final String OSCAR_VARCHAR = "VARCHAR";
    public static final String OSCAR_VARCHAR2 = "VARCHAR2";
    public static final String OSCAR_CLOB = "CLOB";
    public static final String OSCAR_TEXT = "TEXT";
    public static final String OSCAR_LONG = "LONG";

    // ---------------------------binary---------------------------
    public static final String OSCAR_BINARY = "BINARY";
    public static final String OSCAR_VARBINARY = "VARBINARY";

    // ------------------------------blob-------------------------
    public static final String OSCAR_BLOB = "BLOB";
    public static final String OSCAR_BFILE = "BFILE";

    // ------------------------------time-------------------------
    public static final String OSCAR_DATE = "DATE";
    public static final String OSCAR_TIME = "TIME";
    public static final String OSCAR_TIME_WITH_TIME_ZONE = "TIME WITH TIME ZONE";
    public static final String OSCAR_TIMESTAMP = "TIMESTAMP";
    public static final String OSCAR_DATETIME = "DATETIME";

    public static final int DEFAULT_PRECISION = 38;
    public static final int MAX_PRECISION = DEFAULT_PRECISION;
    public static final int DEFAULT_SCALE = 18;
    public static final int MAX_SCALE = MAX_PRECISION;
    public static final int MAX_TIME_SCALE = 6;
    public static final int MAX_TIMESTAMP_SCALE = 6;
    public static final long MAX_VARCHAR_LENGTH = 8000;
    public static final OscarTypeConverter INSTANCE = new OscarTypeConverter();
    public static final long CHAR_16M = 2L << 23;
    public static final long BYTES_4GB = 2L << 32;

    @Override
    public String identifier() {
        return DatabaseIdentifier.OSCAR;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());

        String oscarType = typeDefine.getDataType().toUpperCase();
        switch (oscarType) {
            case OSCAR_BIT:
                builder.sourceType(OSCAR_BIT);
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case OSCAR_TINYINT:
                builder.sourceType(OSCAR_TINYINT);
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case OSCAR_INT1:
                builder.sourceType(OSCAR_INT1);
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case OSCAR_SMALLINT:
                builder.sourceType(OSCAR_SMALLINT);
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case OSCAR_INT2:
                builder.sourceType(OSCAR_INT2);
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case OSCAR_INT:
                builder.sourceType(OSCAR_INT);
                builder.dataType(BasicType.INT_TYPE);
                break;
            case OSCAR_INT4:
                builder.sourceType(OSCAR_INT4);
                builder.dataType(BasicType.INT_TYPE);
                break;
            case OSCAR_INTEGER:
                builder.sourceType(OSCAR_INTEGER);
                builder.dataType(BasicType.INT_TYPE);
                break;
            case OSCAR_BIGINT:
                builder.sourceType(OSCAR_BIGINT);
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case OSCAR_INT8:
                builder.sourceType(OSCAR_INT8);
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case OSCAR_REAL:
                builder.sourceType(OSCAR_REAL);
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case OSCAR_FLOAT4:
                builder.sourceType(OSCAR_FLOAT4);
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case OSCAR_FLOAT:
                builder.sourceType(OSCAR_FLOAT);
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case OSCAR_DOUBLE:
                builder.sourceType(OSCAR_DOUBLE);
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case OSCAR_FLOAT8:
                builder.sourceType(OSCAR_FLOAT8);
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case OSCAR_DOUBLE_PRECISION:
                builder.sourceType(OSCAR_DOUBLE_PRECISION);
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case OSCAR_NUMERIC:
            case OSCAR_NUMBER:
            case OSCAR_DECIMAL:
                DecimalType decimalType;
                if (typeDefine.getPrecision() != null && typeDefine.getPrecision() > 0) {
                    decimalType =
                            new DecimalType(
                                    typeDefine.getPrecision().intValue(), typeDefine.getScale());
                } else {
                    decimalType = new DecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
                }
                builder.sourceType(
                        String.format(
                                "%s(%s,%s)",
                                OSCAR_DECIMAL, decimalType.getPrecision(), decimalType.getScale()));
                builder.dataType(decimalType);
                builder.columnLength((long) decimalType.getPrecision());
                builder.scale(decimalType.getScale());
                break;
            case OSCAR_CHAR:
            case OSCAR_BPCHAR:
            case OSCAR_CHARACTER:
                builder.sourceType(String.format("%s(%s)", OSCAR_CHAR, typeDefine.getLength()));
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                break;
            case OSCAR_VARCHAR:
            case OSCAR_VARCHAR2:
                builder.sourceType(String.format("%s(%s)", OSCAR_VARCHAR, typeDefine.getLength()));
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                break;
            case OSCAR_TEXT:
                builder.sourceType(OSCAR_TEXT);
                builder.dataType(BasicType.STRING_TYPE);
                // oscar text max length is 16777215
                builder.columnLength(CHAR_16M - 1);
                break;
            case OSCAR_LONG:
                builder.sourceType(OSCAR_LONG);
                builder.dataType(BasicType.STRING_TYPE);
                // long = clob
                builder.columnLength(BYTES_4GB - 1);
                break;
            case OSCAR_CLOB:
                builder.sourceType(OSCAR_CLOB);
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(BYTES_4GB - 1);
                break;
            case OSCAR_BINARY:
                builder.sourceType(String.format("%s(%s)", OSCAR_BINARY, typeDefine.getLength()));
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(typeDefine.getLength());
                break;
            case OSCAR_VARBINARY:
                builder.sourceType(
                        String.format("%s(%s)", OSCAR_VARBINARY, typeDefine.getLength()));
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(typeDefine.getLength());
                break;
            case OSCAR_BLOB:
                builder.sourceType(OSCAR_BLOB);
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(BYTES_4GB - 1);
                break;
            case OSCAR_BFILE:
                builder.sourceType(OSCAR_BFILE);
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(typeDefine.getLength());
                break;
            case OSCAR_DATE:
                builder.sourceType(OSCAR_DATE);
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case OSCAR_TIME:
                if (typeDefine.getScale() == null) {
                    builder.sourceType(OSCAR_TIME);
                } else {
                    builder.sourceType(String.format("%s(%s)", OSCAR_TIME, typeDefine.getScale()));
                }
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            case OSCAR_TIME_WITH_TIME_ZONE:
                if (typeDefine.getScale() == null) {
                    builder.sourceType(OSCAR_TIME_WITH_TIME_ZONE);
                } else {
                    builder.sourceType(
                            String.format("TIME(%s) WITH TIME ZONE", typeDefine.getScale()));
                }
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            case OSCAR_TIMESTAMP:
                if (typeDefine.getScale() == null) {
                    builder.sourceType(OSCAR_TIMESTAMP);
                } else {
                    builder.sourceType(
                            String.format("%s(%s)", OSCAR_TIMESTAMP, typeDefine.getScale()));
                }
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            case OSCAR_DATETIME:
                if (typeDefine.getScale() == null) {
                    builder.sourceType(OSCAR_DATETIME);
                } else {
                    builder.sourceType(
                            String.format("%s(%s)", OSCAR_DATETIME, typeDefine.getScale()));
                }
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.OSCAR, typeDefine.getDataType(), typeDefine.getName());
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
                builder.columnType(OSCAR_BIT);
                builder.dataType(OSCAR_BIT);
                break;
            case TINYINT:
                builder.columnType(OSCAR_TINYINT);
                builder.dataType(OSCAR_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(OSCAR_SMALLINT);
                builder.dataType(OSCAR_SMALLINT);
                break;
            case INT:
                builder.columnType(OSCAR_INT);
                builder.dataType(OSCAR_INT);
                break;
            case BIGINT:
                builder.columnType(OSCAR_BIGINT);
                builder.dataType(OSCAR_BIGINT);
                break;
            case FLOAT:
                builder.columnType(OSCAR_REAL);
                builder.dataType(OSCAR_REAL);
                break;
            case DOUBLE:
                builder.columnType(OSCAR_DOUBLE);
                builder.dataType(OSCAR_DOUBLE);
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
                builder.columnType(String.format("%s(%s,%s)", OSCAR_DECIMAL, precision, scale));
                builder.dataType(OSCAR_DECIMAL);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case STRING:
                builder.length(column.getColumnLength());
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(OSCAR_TEXT);
                    builder.dataType(OSCAR_TEXT);
                } else if (column.getColumnLength() <= MAX_VARCHAR_LENGTH) {
                    builder.columnType(
                            String.format("%s(%s)", OSCAR_VARCHAR2, column.getColumnLength()));
                    builder.dataType(OSCAR_VARCHAR2);
                } else {
                    builder.columnType(OSCAR_TEXT);
                    builder.dataType(OSCAR_TEXT);
                }
                break;
            case BYTES:
                builder.length(column.getColumnLength());
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(OSCAR_BLOB);
                    builder.dataType(OSCAR_BLOB);
                } else if (column.getColumnLength() <= MAX_VARCHAR_LENGTH) {
                    builder.columnType(
                            String.format("%s(%s)", OSCAR_VARBINARY, column.getColumnLength()));
                    builder.dataType(OSCAR_VARBINARY);
                } else {
                    builder.columnType(OSCAR_BLOB);
                    builder.dataType(OSCAR_BLOB);
                }
                break;
            case DATE:
                builder.columnType(OSCAR_DATE);
                builder.dataType(OSCAR_DATE);
                break;
            case TIME:
                builder.dataType(OSCAR_TIME);
                if (column.getScale() != null && column.getScale() > 0) {
                    Integer timeScale = column.getScale();
                    if (timeScale > MAX_TIME_SCALE) {
                        timeScale = MAX_TIME_SCALE;
                        log.warn(
                                "The time column {} type time({}) is out of range, "
                                        + "which exceeds the maximum scale of {}, "
                                        + "it will be converted to time({})",
                                column.getName(),
                                column.getScale(),
                                MAX_TIME_SCALE,
                                timeScale);
                    }
                    builder.columnType(String.format("%s(%s)", OSCAR_TIME, timeScale));
                    builder.scale(timeScale);
                } else {
                    builder.columnType(OSCAR_TIME);
                }
                break;
            case TIMESTAMP:
                builder.dataType(OSCAR_TIMESTAMP);
                if (column.getScale() != null && column.getScale() > 0) {
                    Integer timestampScale = column.getScale();
                    if (timestampScale > MAX_TIMESTAMP_SCALE) {
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
                    builder.columnType(String.format("%s(%s)", OSCAR_TIMESTAMP, timestampScale));
                    builder.scale(timestampScale);
                } else {
                    builder.columnType(OSCAR_TIMESTAMP);
                }
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.OSCAR,
                        column.getDataType().toString(),
                        column.getName());
        }
        return builder.build();
    }
}
