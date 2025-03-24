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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm;

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

// reference https://eco.dameng.com/document/dm/zh-cn/sql-dev/dmpl-sql-datatype.html
@Slf4j
@AutoService(TypeConverter.class)
public class DmdbTypeConverter implements TypeConverter<BasicTypeDefine> {
    // ============================data types=====================
    public static final String DM_BIT = "BIT";

    // ----------------------------int-----------------------------
    public static final String DM_INTEGER = "INTEGER";
    public static final String DM_INT = "INT";
    public static final String DM_PLS_INTEGER = "PLS_INTEGER";
    public static final String DM_BIGINT = "BIGINT";
    public static final String DM_TINYINT = "TINYINT";
    public static final String DM_BYTE = "BYTE";
    public static final String DM_SMALLINT = "SMALLINT";

    // dm float is double for Cpp.
    public static final String DM_FLOAT = "FLOAT";
    public static final String DM_DOUBLE = "DOUBLE";
    public static final String DM_DOUBLE_PRECISION = "DOUBLE PRECISION";
    public static final String DM_REAL = "REAL";

    // ----------------------------number-------------------------
    public static final String DM_NUMERIC = "NUMERIC";
    public static final String DM_NUMBER = "NUMBER";
    public static final String DM_DECIMAL = "DECIMAL";
    /** same to DECIMAL */
    public static final String DM_DEC = "DEC";
    // -------------------------char------------------------
    public static final String DM_CHAR = "CHAR";

    public static final String DM_CHARACTER = "CHARACTER";
    public static final String DM_VARCHAR = "VARCHAR";
    public static final String DM_VARCHAR2 = "VARCHAR2";
    public static final String DM_NVARCHAR = "NVARCHAR";
    public static final String DM_LONGVARCHAR = "LONGVARCHAR";
    public static final String DM_CLOB = "CLOB";
    public static final String DM_TEXT = "TEXT";
    public static final String DM_LONG = "LONG";

    // ---------------------------binary---------------------------
    public static final String DM_BINARY = "BINARY";
    public static final String DM_VARBINARY = "VARBINARY";

    // ------------------------------blob-------------------------
    public static final String DM_BLOB = "BLOB";
    public static final String DM_BFILE = "BFILE";
    public static final String DM_IMAGE = "IMAGE";
    public static final String DM_LONGVARBINARY = "LONGVARBINARY";

    // ------------------------------time-------------------------
    public static final String DM_DATE = "DATE";
    public static final String DM_TIME = "TIME";
    public static final String DM_TIME_WITH_TIME_ZONE = "TIME WITH TIME ZONE";
    public static final String DM_TIMESTAMP = "TIMESTAMP";
    public static final String DM_DATETIME = "DATETIME";
    public static final String DM_DATETIME_WITH_TIME_ZONE = "DATETIME WITH TIME ZONE";

    public static final int DEFAULT_PRECISION = 38;
    public static final int MAX_PRECISION = 38;
    public static final int DEFAULT_SCALE = 18;
    public static final int MAX_SCALE = MAX_PRECISION - 1;
    public static final int MAX_TIME_SCALE = 6;
    public static final int MAX_TIMESTAMP_SCALE = 6;
    /**
     * DM_CHAR DM_CHARACTER DM_VARCHAR DM_VARCHAR2 max logical length is 32767
     *
     * <p>DM_CHAR DM_CHARACTER DM_VARCHAR DM_VARCHAR2 max physical length: page 4K 1900 page 8K 3900
     * page 16K 8000 page 32K 8188
     */
    public static final long MAX_CHAR_LENGTH_FOR_PAGE_4K = 1900;

    public static final long MAX_BINARY_LENGTH_FOR_PAGE_4K = 1900;
    public static final DmdbTypeConverter INSTANCE = new DmdbTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.DAMENG;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());

        String dmType = typeDefine.getDataType().toUpperCase();
        switch (dmType) {
            case DM_BIT:
                builder.sourceType(DM_BIT);
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case DM_TINYINT:
                builder.sourceType(DM_TINYINT);
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case DM_BYTE:
                builder.sourceType(DM_BYTE);
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case DM_SMALLINT:
                builder.sourceType(DM_SMALLINT);
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case DM_INT:
                builder.sourceType(DM_INT);
                builder.dataType(BasicType.INT_TYPE);
                break;
            case DM_INTEGER:
                builder.sourceType(DM_INTEGER);
                builder.dataType(BasicType.INT_TYPE);
                break;
            case DM_PLS_INTEGER:
                builder.sourceType(DM_PLS_INTEGER);
                builder.dataType(BasicType.INT_TYPE);
                break;
            case DM_BIGINT:
                builder.sourceType(DM_BIGINT);
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case DM_REAL:
                builder.sourceType(DM_REAL);
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case DM_FLOAT:
                builder.sourceType(DM_FLOAT);
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case DM_DOUBLE:
                builder.sourceType(DM_DOUBLE);
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case DM_DOUBLE_PRECISION:
                builder.sourceType(DM_DOUBLE_PRECISION);
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case DM_NUMERIC:
            case DM_NUMBER:
            case DM_DECIMAL:
            case DM_DEC:
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
                                DM_DECIMAL, decimalType.getPrecision(), decimalType.getScale()));
                builder.dataType(decimalType);
                builder.columnLength((long) decimalType.getPrecision());
                builder.scale(decimalType.getScale());
                break;
            case DM_CHAR:
            case DM_CHARACTER:
                builder.sourceType(String.format("%s(%s)", DM_CHAR, typeDefine.getLength()));
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                break;
            case DM_VARCHAR:
            case DM_VARCHAR2:
                builder.sourceType(String.format("%s(%s)", DM_VARCHAR2, typeDefine.getLength()));
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                break;
            case DM_NVARCHAR:
                builder.sourceType(String.format("%s(%s)", DM_NVARCHAR, typeDefine.getLength()));
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                break;
            case DM_TEXT:
                builder.sourceType(DM_TEXT);
                builder.dataType(BasicType.STRING_TYPE);
                // dm text max length is 2147483647
                builder.columnLength(typeDefine.getLength());
                break;
            case DM_LONG:
                builder.sourceType(DM_LONG);
                builder.dataType(BasicType.STRING_TYPE);
                // dm long max length is 2147483647
                builder.columnLength(typeDefine.getLength());
                break;
            case DM_LONGVARCHAR:
                builder.sourceType(DM_LONGVARCHAR);
                builder.dataType(BasicType.STRING_TYPE);
                // dm longvarchar max length is 2147483647
                builder.columnLength(typeDefine.getLength());
                break;
            case DM_CLOB:
                builder.sourceType(DM_CLOB);
                builder.dataType(BasicType.STRING_TYPE);
                // dm clob max length is 2147483647
                builder.columnLength(typeDefine.getLength());
                break;
            case DM_BINARY:
                builder.sourceType(String.format("%s(%s)", DM_BINARY, typeDefine.getLength()));
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(typeDefine.getLength());
                break;
            case DM_VARBINARY:
                builder.sourceType(String.format("%s(%s)", DM_VARBINARY, typeDefine.getLength()));
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(typeDefine.getLength());
                break;
            case DM_LONGVARBINARY:
                builder.sourceType(DM_LONGVARBINARY);
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(typeDefine.getLength());
                break;
            case DM_IMAGE:
                builder.sourceType(DM_IMAGE);
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(typeDefine.getLength());
                break;
            case DM_BLOB:
                builder.sourceType(DM_BLOB);
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(typeDefine.getLength());
                break;
            case DM_BFILE:
                builder.sourceType(DM_BFILE);
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(typeDefine.getLength());
                break;
            case DM_DATE:
                builder.sourceType(DM_DATE);
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case DM_TIME:
                if (typeDefine.getScale() == null) {
                    builder.sourceType(DM_TIME);
                } else {
                    builder.sourceType(String.format("%s(%s)", DM_TIME, typeDefine.getScale()));
                }
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            case DM_TIME_WITH_TIME_ZONE:
                if (typeDefine.getScale() == null) {
                    builder.sourceType(DM_TIME_WITH_TIME_ZONE);
                } else {
                    builder.sourceType(
                            String.format("TIME(%s) WITH TIME ZONE", typeDefine.getScale()));
                }
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            case DM_TIMESTAMP:
                if (typeDefine.getScale() == null) {
                    builder.sourceType(DM_TIMESTAMP);
                } else {
                    builder.sourceType(
                            String.format("%s(%s)", DM_TIMESTAMP, typeDefine.getScale()));
                }
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            case DM_DATETIME:
                if (typeDefine.getScale() == null) {
                    builder.sourceType(DM_DATETIME);
                } else {
                    builder.sourceType(String.format("%s(%s)", DM_DATETIME, typeDefine.getScale()));
                }
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            case DM_DATETIME_WITH_TIME_ZONE:
                if (typeDefine.getScale() == null) {
                    builder.sourceType(DM_DATETIME_WITH_TIME_ZONE);
                } else {
                    builder.sourceType(
                            String.format("DATETIME(%s) WITH TIME ZONE", typeDefine.getScale()));
                }
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.DAMENG, typeDefine.getDataType(), typeDefine.getName());
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
                builder.columnType(DM_BIT);
                builder.dataType(DM_BIT);
                break;
            case TINYINT:
                builder.columnType(DM_TINYINT);
                builder.dataType(DM_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(DM_SMALLINT);
                builder.dataType(DM_SMALLINT);
                break;
            case INT:
                builder.columnType(DM_INT);
                builder.dataType(DM_INT);
                break;
            case BIGINT:
                builder.columnType(DM_BIGINT);
                builder.dataType(DM_BIGINT);
                break;
            case FLOAT:
                builder.columnType(DM_REAL);
                builder.dataType(DM_REAL);
                break;
            case DOUBLE:
                builder.columnType(DM_DOUBLE);
                builder.dataType(DM_DOUBLE);
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
                builder.columnType(String.format("%s(%s,%s)", DM_DECIMAL, precision, scale));
                builder.dataType(DM_DECIMAL);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case STRING:
                builder.length(column.getColumnLength());
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(DM_TEXT);
                    builder.dataType(DM_TEXT);
                } else if (column.getColumnLength() <= MAX_CHAR_LENGTH_FOR_PAGE_4K) {
                    builder.columnType(
                            String.format("%s(%s)", DM_VARCHAR2, column.getColumnLength()));
                    builder.dataType(DM_VARCHAR2);
                } else {
                    builder.columnType(DM_TEXT);
                    builder.dataType(DM_TEXT);
                }
                break;
            case BYTES:
                builder.length(column.getColumnLength());
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(DM_LONGVARBINARY);
                    builder.dataType(DM_LONGVARBINARY);
                } else if (column.getColumnLength() <= MAX_BINARY_LENGTH_FOR_PAGE_4K) {
                    builder.columnType(
                            String.format("%s(%s)", DM_VARBINARY, column.getColumnLength()));
                    builder.dataType(DM_VARBINARY);
                } else {
                    builder.columnType(DM_LONGVARBINARY);
                    builder.dataType(DM_LONGVARBINARY);
                }
                break;
            case DATE:
                builder.columnType(DM_DATE);
                builder.dataType(DM_DATE);
                break;
            case TIME:
                builder.dataType(DM_TIME);
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
                                MAX_SCALE,
                                timeScale);
                    }
                    builder.columnType(String.format("%s(%s)", DM_TIME, timeScale));
                    builder.scale(timeScale);
                } else {
                    builder.columnType(DM_TIME);
                }
                break;
            case TIMESTAMP:
                builder.dataType(DM_TIMESTAMP);
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
                    builder.columnType(String.format("%s(%s)", DM_TIMESTAMP, timestampScale));
                    builder.scale(timestampScale);
                } else {
                    builder.columnType(DM_TIMESTAMP);
                }
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.DAMENG,
                        column.getDataType().toString(),
                        column.getName());
        }
        return builder.build();
    }
}
