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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.xugu;

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

// reference
// https://docs.xugudb.com/%E8%99%9A%E8%B0%B7%E6%95%B0%E6%8D%AE%E5%BA%93%E5%AF%B9%E5%A4%96%E5%8F%91%E5%B8%83/06%E5%8F%82%E8%80%83%E6%8C%87%E5%8D%97/SQL%E8%AF%AD%E6%B3%95%E5%8F%82%E8%80%83/%E6%95%B0%E6%8D%AE%E7%B1%BB%E5%9E%8B/%E6%A6%82%E8%BF%B0/
@Slf4j
@AutoService(TypeConverter.class)
public class XuguTypeConverter implements TypeConverter<BasicTypeDefine> {
    // ============================data types=====================
    // -------------------------number----------------------------
    public static final String XUGU_NUMERIC = "NUMERIC";
    public static final String XUGU_NUMBER = "NUMBER";
    public static final String XUGU_DECIMAL = "DECIMAL";
    public static final String XUGU_INTEGER = "INTEGER";
    public static final String XUGU_INT = "INT";
    public static final String XUGU_BIGINT = "BIGINT";
    public static final String XUGU_TINYINT = "TINYINT";
    public static final String XUGU_SMALLINT = "SMALLINT";
    public static final String XUGU_FLOAT = "FLOAT";
    public static final String XUGU_DOUBLE = "DOUBLE";

    // ----------------------------string-------------------------
    public static final String XUGU_CHAR = "CHAR";
    public static final String XUGU_NCHAR = "NCHAR";
    public static final String XUGU_VARCHAR = "VARCHAR";
    public static final String XUGU_VARCHAR2 = "VARCHAR2";
    public static final String XUGU_CLOB = "CLOB";

    // ------------------------------time-------------------------
    public static final String XUGU_DATE = "DATE";
    public static final String XUGU_TIME = "TIME";
    public static final String XUGU_TIMESTAMP = "TIMESTAMP";
    public static final String XUGU_DATETIME = "DATETIME";
    public static final String XUGU_DATETIME_WITH_TIME_ZONE = "DATETIME WITH TIME ZONE";
    public static final String XUGU_TIME_WITH_TIME_ZONE = "TIME WITH TIME ZONE";
    public static final String XUGU_TIMESTAMP_WITH_TIME_ZONE = "TIMESTAMP WITH TIME ZONE";

    // ---------------------------binary---------------------------
    public static final String XUGU_BINARY = "BINARY";
    public static final String XUGU_BLOB = "BLOB";

    // ---------------------------other---------------------------
    public static final String XUGU_GUID = "GUID";
    public static final String XUGU_BOOLEAN = "BOOLEAN";
    public static final String XUGU_BOOL = "BOOL";
    public static final String XUGU_JSON = "JSON";

    public static final int MAX_PRECISION = 38;
    public static final int DEFAULT_PRECISION = MAX_PRECISION;
    public static final int MAX_SCALE = 38;
    public static final int DEFAULT_SCALE = 18;
    public static final int TIMESTAMP_DEFAULT_SCALE = 3;
    public static final int MAX_TIMESTAMP_SCALE = 6;
    public static final int MAX_TIME_SCALE = 3;
    public static final long MAX_VARCHAR_LENGTH = 60000;
    public static final long POWER_2_16 = (long) Math.pow(2, 16);
    public static final long BYTES_2GB = (long) Math.pow(2, 31);
    public static final long MAX_BINARY_LENGTH = POWER_2_16 - 4;
    public static final XuguTypeConverter INSTANCE = new XuguTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.XUGU;
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

        String xuguDataType = typeDefine.getDataType().toUpperCase();
        switch (xuguDataType) {
            case XUGU_BOOLEAN:
            case XUGU_BOOL:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case XUGU_TINYINT:
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case XUGU_SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case XUGU_INT:
            case XUGU_INTEGER:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case XUGU_BIGINT:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case XUGU_FLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case XUGU_DOUBLE:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case XUGU_NUMBER:
            case XUGU_DECIMAL:
            case XUGU_NUMERIC:
                DecimalType decimalType;
                if (typeDefine.getPrecision() != null && typeDefine.getPrecision() > 0) {
                    decimalType =
                            new DecimalType(
                                    typeDefine.getPrecision().intValue(), typeDefine.getScale());
                } else {
                    decimalType = new DecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
                }
                builder.dataType(decimalType);
                builder.columnLength(Long.valueOf(decimalType.getPrecision()));
                builder.scale(decimalType.getScale());
                break;

            case XUGU_CHAR:
            case XUGU_NCHAR:
                builder.dataType(BasicType.STRING_TYPE);
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.columnLength(TypeDefineUtils.charTo4ByteLength(1L));
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                break;
            case XUGU_VARCHAR:
            case XUGU_VARCHAR2:
                builder.dataType(BasicType.STRING_TYPE);
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.columnLength(TypeDefineUtils.charTo4ByteLength(MAX_VARCHAR_LENGTH));
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                break;
            case XUGU_CLOB:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(BYTES_2GB - 1);
                break;
            case XUGU_JSON:
            case XUGU_GUID:
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case XUGU_BINARY:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(MAX_BINARY_LENGTH);
                break;
            case XUGU_BLOB:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(BYTES_2GB - 1);
                break;
            case XUGU_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case XUGU_TIME:
            case XUGU_TIME_WITH_TIME_ZONE:
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                break;
            case XUGU_DATETIME:
            case XUGU_DATETIME_WITH_TIME_ZONE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                break;
            case XUGU_TIMESTAMP:
            case XUGU_TIMESTAMP_WITH_TIME_ZONE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                if (typeDefine.getScale() == null) {
                    builder.scale(TIMESTAMP_DEFAULT_SCALE);
                } else {
                    builder.scale(typeDefine.getScale());
                }
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.XUGU, xuguDataType, typeDefine.getName());
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
                builder.columnType(XUGU_BOOLEAN);
                builder.dataType(XUGU_BOOLEAN);
                break;
            case TINYINT:
                builder.columnType(XUGU_TINYINT);
                builder.dataType(XUGU_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(XUGU_SMALLINT);
                builder.dataType(XUGU_SMALLINT);
                break;
            case INT:
                builder.columnType(XUGU_INTEGER);
                builder.dataType(XUGU_INTEGER);
                break;
            case BIGINT:
                builder.columnType(XUGU_BIGINT);
                builder.dataType(XUGU_BIGINT);
                break;
            case FLOAT:
                builder.columnType(XUGU_FLOAT);
                builder.dataType(XUGU_FLOAT);
                break;
            case DOUBLE:
                builder.columnType(XUGU_DOUBLE);
                builder.dataType(XUGU_DOUBLE);
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
                builder.columnType(String.format("%s(%s,%s)", XUGU_NUMERIC, precision, scale));
                builder.dataType(XUGU_NUMERIC);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case BYTES:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(XUGU_BLOB);
                    builder.dataType(XUGU_BLOB);
                } else if (column.getColumnLength() <= MAX_BINARY_LENGTH) {
                    builder.columnType(XUGU_BINARY);
                    builder.dataType(XUGU_BINARY);
                } else {
                    builder.columnType(XUGU_BLOB);
                    builder.dataType(XUGU_BLOB);
                }
                break;
            case STRING:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(String.format("%s(%s)", XUGU_VARCHAR, MAX_VARCHAR_LENGTH));
                    builder.dataType(XUGU_VARCHAR);
                } else if (column.getColumnLength() <= MAX_VARCHAR_LENGTH) {
                    builder.columnType(
                            String.format("%s(%s)", XUGU_VARCHAR, column.getColumnLength()));
                    builder.dataType(XUGU_VARCHAR);
                } else {
                    builder.columnType(XUGU_CLOB);
                    builder.dataType(XUGU_CLOB);
                }
                break;
            case DATE:
                builder.columnType(XUGU_DATE);
                builder.dataType(XUGU_DATE);
                break;
            case TIME:
                builder.dataType(XUGU_TIME);
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
                    builder.columnType(String.format("%s(%s)", XUGU_TIME, timeScale));
                    builder.scale(timeScale);
                } else {
                    builder.columnType(XUGU_TIME);
                }
                break;
            case TIMESTAMP:
                if (column.getScale() == null || column.getScale() <= 0) {
                    builder.columnType(XUGU_TIMESTAMP);
                } else {
                    int timestampScale = column.getScale();
                    if (column.getScale() > MAX_TIMESTAMP_SCALE) {
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
                    builder.columnType(String.format("TIMESTAMP(%s)", timestampScale));
                    builder.scale(timestampScale);
                }
                builder.dataType(XUGU_TIMESTAMP);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.XUGU,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }

        return builder.build();
    }
}
