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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mariadb;

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;

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
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(TypeConverter.class)
public class MariaDbTypeConverter implements TypeConverter<BasicTypeDefine> {

    // ============================data types=====================
    public static final String MARIADB_NULL = "NULL";
    public static final String MARIADB_BIT = "BIT";
    public static final String MARIADB_BOOLEAN = "BOOLEAN";
    public static final String MARIADB_BOOL = "BOOL";

    // -------------------------number----------------------------
    public static final String MARIADB_TINYINT = "TINYINT";
    public static final String MARIADB_TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    public static final String MARIADB_SMALLINT = "SMALLINT";
    public static final String MARIADB_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    public static final String MARIADB_MEDIUMINT = "MEDIUMINT";
    public static final String MARIADB_MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    public static final String MARIADB_INT = "INT";
    public static final String MARIADB_INT_UNSIGNED = "INT UNSIGNED";
    public static final String MARIADB_INTEGER = "INTEGER";
    public static final String MARIADB_INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    public static final String MARIADB_BIGINT = "BIGINT";
    public static final String MARIADB_BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    public static final String MARIADB_DECIMAL = "DECIMAL";
    public static final String MARIADB_DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    public static final String MARIADB_NUMERIC = "NUMERIC";
    public static final String MARIADB_NUMERIC_UNSIGNED = "NUMERIC UNSIGNED";
    public static final String MARIADB_DEC = "DEC";
    public static final String MARIADB_DEC_UNSIGNED = "DEC UNSIGNED";
    public static final String MARIADB_FIXED = "FIXED";
    public static final String MARIADB_FIXED_UNSIGNED = "FIXED UNSIGNED";
    public static final String MARIADB_FLOAT = "FLOAT";
    public static final String MARIADB_FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    public static final String MARIADB_DOUBLE = "DOUBLE";
    public static final String MARIADB_DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";

    // -------------------------string----------------------------
    public static final String MARIADB_CHAR = "CHAR";
    public static final String MARIADB_VARCHAR = "VARCHAR";
    public static final String MARIADB_TINYTEXT = "TINYTEXT";
    public static final String MARIADB_MEDIUMTEXT = "MEDIUMTEXT";
    public static final String MARIADB_TEXT = "TEXT";
    public static final String MARIADB_LONGTEXT = "LONGTEXT";
    public static final String MARIADB_JSON = "JSON";
    public static final String MARIADB_ENUM = "ENUM";
    public static final String MARIADB_SET = "SET";
    public static final String MARIADB_UUID = "UUID";
    public static final String MARIADB_INET4 = "INET4";
    public static final String MARIADB_INET6 = "INET6";

    // ------------------------------time-------------------------
    public static final String MARIADB_DATE = "DATE";
    public static final String MARIADB_DATETIME = "DATETIME";
    public static final String MARIADB_TIME = "TIME";
    public static final String MARIADB_TIMESTAMP = "TIMESTAMP";
    public static final String MARIADB_YEAR = "YEAR";

    // ------------------------------blob-------------------------
    public static final String MARIADB_TINYBLOB = "TINYBLOB";
    public static final String MARIADB_MEDIUMBLOB = "MEDIUMBLOB";
    public static final String MARIADB_BLOB = "BLOB";
    public static final String MARIADB_LONGBLOB = "LONGBLOB";
    public static final String MARIADB_BINARY = "BINARY";
    public static final String MARIADB_VARBINARY = "VARBINARY";
    public static final String MARIADB_GEOMETRY = "GEOMETRY";

    public static final int DEFAULT_PRECISION = 38;
    public static final int MAX_PRECISION = 65;
    public static final int DEFAULT_SCALE = 18;
    public static final int MAX_SCALE = 30;
    public static final int MAX_TIME_SCALE = 6;
    public static final int MAX_TIMESTAMP_SCALE = 6;
    public static final long POWER_2_8 = (long) Math.pow(2, 8);
    public static final long POWER_2_16 = (long) Math.pow(2, 16);
    public static final long POWER_2_24 = (long) Math.pow(2, 24);
    public static final long POWER_2_32 = (long) Math.pow(2, 32);
    public static final long MAX_VARBINARY_LENGTH = POWER_2_16 - 4;

    private final boolean intTypeNarrowing;

    public static final MariaDbTypeConverter DEFAULT_INSTANCE = new MariaDbTypeConverter();

    public MariaDbTypeConverter() {
        this(JdbcCommonOptions.INT_TYPE_NARROWING.defaultValue());
    }

    public MariaDbTypeConverter(boolean intTypeNarrowing) {
        this.intTypeNarrowing = intTypeNarrowing;
    }

    @Override
    public String identifier() {
        return DatabaseIdentifier.MARIADB;
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

        String mariaDbDataType = typeDefine.getDataType().toUpperCase();
        if (typeDefine.isUnsigned() && !(mariaDbDataType.endsWith(" UNSIGNED"))) {
            mariaDbDataType = mariaDbDataType + " UNSIGNED";
        }
        switch (mariaDbDataType) {
            case MARIADB_NULL:
                builder.dataType(BasicType.VOID_TYPE);
                break;
            case MARIADB_BIT:
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.dataType(BasicType.BOOLEAN_TYPE);
                } else if (typeDefine.getLength() == 1) {
                    builder.dataType(BasicType.BOOLEAN_TYPE);
                } else {
                    builder.dataType(PrimitiveByteArrayType.INSTANCE);
                    long byteLength = typeDefine.getLength() / 8;
                    byteLength += typeDefine.getLength() % 8 > 0 ? 1 : 0;
                    builder.columnLength(byteLength);
                }
                break;
            case MARIADB_BOOLEAN:
            case MARIADB_BOOL:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case MARIADB_TINYINT:
                if (typeDefine.getColumnType().equalsIgnoreCase("tinyint(1)") && intTypeNarrowing) {
                    builder.dataType(BasicType.BOOLEAN_TYPE);
                } else {
                    builder.dataType(BasicType.BYTE_TYPE);
                }
                break;
            case MARIADB_TINYINT_UNSIGNED:
            case MARIADB_SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case MARIADB_SMALLINT_UNSIGNED:
            case MARIADB_MEDIUMINT:
            case MARIADB_MEDIUMINT_UNSIGNED:
            case MARIADB_INT:
            case MARIADB_INTEGER:
            case MARIADB_YEAR:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case MARIADB_INT_UNSIGNED:
            case MARIADB_INTEGER_UNSIGNED:
            case MARIADB_BIGINT:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case MARIADB_BIGINT_UNSIGNED:
                DecimalType intDecimalType = new DecimalType(20, 0);
                builder.dataType(intDecimalType);
                builder.columnLength(Long.valueOf(intDecimalType.getPrecision()));
                builder.scale(intDecimalType.getScale());
                break;
            case MARIADB_FLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case MARIADB_FLOAT_UNSIGNED:
                log.warn("{} will probably cause value overflow.", MARIADB_FLOAT_UNSIGNED);
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case MARIADB_DOUBLE:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case MARIADB_DOUBLE_UNSIGNED:
                log.warn("{} will probably cause value overflow.", MARIADB_DOUBLE_UNSIGNED);
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case MARIADB_DECIMAL:
            case MARIADB_NUMERIC:
            case MARIADB_DEC:
            case MARIADB_FIXED:
                Preconditions.checkArgument(typeDefine.getPrecision() > 0);

                DecimalType decimalType;
                if (typeDefine.getPrecision() > DEFAULT_PRECISION) {
                    log.warn("{} will probably cause value overflow.", mariaDbDataType);
                    decimalType = new DecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
                } else {
                    decimalType =
                            new DecimalType(
                                    typeDefine.getPrecision().intValue(),
                                    typeDefine.getScale() == null
                                            ? 0
                                            : typeDefine.getScale().intValue());
                }
                builder.dataType(decimalType);
                builder.columnLength(Long.valueOf(decimalType.getPrecision()));
                builder.scale(decimalType.getScale());
                break;
            case MARIADB_DECIMAL_UNSIGNED:
            case MARIADB_NUMERIC_UNSIGNED:
            case MARIADB_DEC_UNSIGNED:
            case MARIADB_FIXED_UNSIGNED:
                Preconditions.checkArgument(typeDefine.getPrecision() > 0);

                log.warn("{} will probably cause value overflow.", mariaDbDataType);
                DecimalType decimalUnsignedType =
                        new DecimalType(
                                typeDefine.getPrecision().intValue() + 1,
                                typeDefine.getScale() == null
                                        ? 0
                                        : typeDefine.getScale().intValue());
                builder.dataType(decimalUnsignedType);
                builder.columnLength(Long.valueOf(decimalUnsignedType.getPrecision()));
                builder.scale(decimalUnsignedType.getScale());
                break;
            case MARIADB_ENUM:
            case MARIADB_SET:
                builder.dataType(BasicType.STRING_TYPE);
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.columnLength(100L);
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                break;
            case MARIADB_CHAR:
            case MARIADB_VARCHAR:
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.columnLength(TypeDefineUtils.charTo4ByteLength(1L));
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case MARIADB_TINYTEXT:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(POWER_2_8 - 1);
                break;
            case MARIADB_TEXT:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(POWER_2_16 - 1);
                break;
            case MARIADB_MEDIUMTEXT:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(POWER_2_24 - 1);
                break;
            case MARIADB_LONGTEXT:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(POWER_2_32 - 1);
                break;
            case MARIADB_JSON:
            case MARIADB_UUID:
            case MARIADB_INET4:
            case MARIADB_INET6:
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case MARIADB_BINARY:
            case MARIADB_VARBINARY:
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.columnLength(1L);
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                break;
            case MARIADB_TINYBLOB:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(POWER_2_8 - 1);
                break;
            case MARIADB_BLOB:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(POWER_2_16 - 1);
                break;
            case MARIADB_MEDIUMBLOB:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(POWER_2_24 - 1);
                break;
            case MARIADB_LONGBLOB:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(POWER_2_32 - 1);
                break;
            case MARIADB_GEOMETRY:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                break;
            case MARIADB_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case MARIADB_TIME:
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            case MARIADB_DATETIME:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            case MARIADB_TIMESTAMP:
                builder.dataType(LocalTimeType.OFFSET_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.MARIADB, mariaDbDataType, typeDefine.getName());
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
            case NULL:
                builder.nativeType(MARIADB_NULL);
                builder.columnType(MARIADB_NULL);
                builder.dataType(MARIADB_NULL);
                break;
            case BOOLEAN:
                builder.nativeType(MARIADB_BOOLEAN);
                builder.columnType(String.format("%s(%s)", MARIADB_TINYINT, 1));
                builder.dataType(MARIADB_TINYINT);
                builder.length(1L);
                break;
            case TINYINT:
                builder.nativeType(MARIADB_TINYINT);
                builder.columnType(MARIADB_TINYINT);
                builder.dataType(MARIADB_TINYINT);
                break;
            case SMALLINT:
                builder.nativeType(MARIADB_SMALLINT);
                builder.columnType(MARIADB_SMALLINT);
                builder.dataType(MARIADB_SMALLINT);
                break;
            case INT:
                builder.nativeType(MARIADB_INT);
                builder.columnType(MARIADB_INT);
                builder.dataType(MARIADB_INT);
                break;
            case BIGINT:
                builder.nativeType(MARIADB_BIGINT);
                builder.columnType(MARIADB_BIGINT);
                builder.dataType(MARIADB_BIGINT);
                break;
            case FLOAT:
                builder.nativeType(MARIADB_FLOAT);
                builder.columnType(MARIADB_FLOAT);
                builder.dataType(MARIADB_FLOAT);
                break;
            case DOUBLE:
                builder.nativeType(MARIADB_DOUBLE);
                builder.columnType(MARIADB_DOUBLE);
                builder.dataType(MARIADB_DOUBLE);
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

                builder.nativeType(MARIADB_DECIMAL);
                builder.columnType(String.format("%s(%s,%s)", MARIADB_DECIMAL, precision, scale));
                builder.dataType(MARIADB_DECIMAL);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case BYTES:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.nativeType(MARIADB_VARBINARY);
                    builder.columnType(
                            String.format("%s(%s)", MARIADB_VARBINARY, MAX_VARBINARY_LENGTH / 2));
                    builder.dataType(MARIADB_VARBINARY);
                } else if (column.getColumnLength() < MAX_VARBINARY_LENGTH) {
                    builder.nativeType(MARIADB_VARBINARY);
                    builder.columnType(
                            String.format("%s(%s)", MARIADB_VARBINARY, column.getColumnLength()));
                    builder.dataType(MARIADB_VARBINARY);
                } else if (column.getColumnLength() < POWER_2_24) {
                    builder.nativeType(MARIADB_MEDIUMBLOB);
                    builder.columnType(MARIADB_MEDIUMBLOB);
                    builder.dataType(MARIADB_MEDIUMBLOB);
                } else {
                    builder.nativeType(MARIADB_LONGBLOB);
                    builder.columnType(MARIADB_LONGBLOB);
                    builder.dataType(MARIADB_LONGBLOB);
                }
                break;
            case STRING:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.nativeType(MARIADB_LONGTEXT);
                    builder.columnType(MARIADB_LONGTEXT);
                    builder.dataType(MARIADB_LONGTEXT);
                } else if (column.getColumnLength() < POWER_2_8) {
                    builder.nativeType(MARIADB_VARCHAR);
                    builder.columnType(
                            String.format("%s(%s)", MARIADB_VARCHAR, column.getColumnLength()));
                    builder.dataType(MARIADB_VARCHAR);
                } else if (column.getColumnLength() < POWER_2_16) {
                    builder.nativeType(MARIADB_TEXT);
                    builder.columnType(MARIADB_TEXT);
                    builder.dataType(MARIADB_TEXT);
                } else if (column.getColumnLength() < POWER_2_24) {
                    builder.nativeType(MARIADB_MEDIUMTEXT);
                    builder.columnType(MARIADB_MEDIUMTEXT);
                    builder.dataType(MARIADB_MEDIUMTEXT);
                } else {
                    builder.nativeType(MARIADB_LONGTEXT);
                    builder.columnType(MARIADB_LONGTEXT);
                    builder.dataType(MARIADB_LONGTEXT);
                }
                break;
            case DATE:
                builder.nativeType(MARIADB_DATE);
                builder.columnType(MARIADB_DATE);
                builder.dataType(MARIADB_DATE);
                break;
            case TIME:
                builder.nativeType(MARIADB_TIME);
                builder.dataType(MARIADB_TIME);
                if (column.getScale() != null && column.getScale() > 0) {
                    int timeScale = column.getScale();
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
                    builder.columnType(String.format("%s(%s)", MARIADB_TIME, timeScale));
                    builder.scale(timeScale);
                } else {
                    builder.columnType(MARIADB_TIME);
                }
                break;
            case TIMESTAMP:
                builder.nativeType(MARIADB_DATETIME);
                builder.dataType(MARIADB_DATETIME);
                if (column.getScale() != null && column.getScale() > 0) {
                    int timestampScale = column.getScale();
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
                    builder.columnType(String.format("%s(%s)", MARIADB_DATETIME, timestampScale));
                    builder.scale(timestampScale);
                } else {
                    builder.columnType(MARIADB_DATETIME);
                }
                break;
            case TIMESTAMP_TZ:
                builder.nativeType(MARIADB_TIMESTAMP);
                builder.dataType(MARIADB_TIMESTAMP);
                if (column.getScale() != null && column.getScale() > 0) {
                    int timestampTzScale = column.getScale();
                    if (timestampTzScale > MAX_TIMESTAMP_SCALE) {
                        timestampTzScale = MAX_TIMESTAMP_SCALE;
                        log.warn(
                                "The timestamp_tz column {} type timestamp({}) is out of range, "
                                        + "which exceeds the maximum scale of {}, "
                                        + "it will be converted to timestamp({})",
                                column.getName(),
                                column.getScale(),
                                MAX_TIMESTAMP_SCALE,
                                timestampTzScale);
                    }
                    builder.columnType(
                            String.format("%s(%s)", MARIADB_TIMESTAMP, timestampTzScale));
                    builder.scale(timestampTzScale);
                } else {
                    builder.columnType(MARIADB_TIMESTAMP);
                }
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.MARIADB,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }

        return builder.build();
    }
}
