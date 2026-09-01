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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashandb;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.common.source.TypeDefineUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

// reference
// https://doc.yashandb.com/yashandb/23.5/zh/All-Manuals/Development-Guide/SQL-Reference-Manual/Data-Types/00Data-Types.html
@Slf4j
@AutoService(TypeConverter.class)
public class YashanDbTypeConverter implements TypeConverter<BasicTypeDefine> {

    // ============================ Numeric Data Types ============================

    // Integer types: TINYINT(1B), SMALLINT(2B), INT(4B), BIGINT(8B)
    public static final String TINYINT = "TINYINT";
    public static final String SMALLINT = "SMALLINT";
    public static final String INT = "INT";
    public static final String INTEGER = "INTEGER";
    public static final String BIGINT = "BIGINT";

    // Floating-point types (IEEE 754): FLOAT(4B),
    // DOUBLE(8B)
    public static final String FLOAT = "FLOAT";
    public static final String DOUBLE = "DOUBLE";

    // NUMBER type: P in [1,38], S in [-84,127].
    public static final String NUMBER = "NUMBER";

    // BIT type: Size in [1,64]
    public static final String BIT = "BIT";

    // Boolean type
    public static final String BOOLEAN = "BOOLEAN";

    // ============================ Character Data Types ============================

    // CHAR: [1,8000] bytes; VARCHAR: [1,65534] bytes
    public static final String CHAR = "CHAR";
    public static final String VARCHAR = "VARCHAR";
    public static final String VARCHAR2 = "VARCHAR2";

    // NCHAR: [1,4000]; NVARCHAR: [1,32767] (Unicode)
    public static final String NCHAR = "NCHAR";
    public static final String NVARCHAR = "NVARCHAR";
    public static final String NVARCHAR2 = "NVARCHAR2";

    // ============================ Date & Time Data Types ============================

    public static final String DATE = "DATE";
    public static final String TIME = "TIME";
    public static final String TIMESTAMP = "TIMESTAMP";
    public static final String TIMESTAMP_WITH_TIME_ZONE = TIMESTAMP + " WITH TIME ZONE";
    public static final String TIMESTAMP_WITH_LOCAL_TIME_ZONE = TIMESTAMP + " WITH LOCAL TIME ZONE";

    // Interval types
    public static final String INTERVAL_YEAR_TO_MONTH = "INTERVAL YEAR TO MONTH";
    public static final String INTERVAL_DAY_TO_SECOND = "INTERVAL DAY TO SECOND";

    // ============================ Binary / LOB Data Types ============================

    // LOB text types: max 4G*DB_BLOCK_SIZE
    public static final String CLOB = "CLOB";
    public static final String NCLOB = "NCLOB";

    // BLOB: max 4G*DB_BLOCK_SIZE
    public static final String BLOB = "BLOB";
    // RAW: [1,65534] bytes
    public static final String RAW = "RAW";

    // ============================ ROWID Types ============================

    // ROWID: 16 bytes; UROWID: [1,8000], default 4000
    public static final String ROWID = "ROWID";
    public static final String UROWID = "UROWID";

    // ============================ Other Types ============================

    // XMLTYPE: [1, 4G*DB_BLOCK_SIZE]
    public static final String XMLTYPE = "XMLTYPE";

    // JSON: [1, 32MB]
    public static final String JSON = "JSON";

    private static final String VECTOR_NAME = "VECTOR";

    // ============================ Limits & Defaults ============================

    public static final int MAX_PRECISION = 38;
    public static final int DEFAULT_PRECISION = MAX_PRECISION;
    public static final int MAX_SCALE = 127;
    public static final int DEFAULT_SCALE = 18;
    public static final int TIMESTAMP_DEFAULT_SCALE = 6;
    public static final int MAX_TIMESTAMP_SCALE = 9;
    public static final long MAX_RAW_LENGTH = 65534;
    public static final long MAX_ROWID_LENGTH = 18;
    public static final long MAX_CHAR_LENGTH = 8000;
    public static final long MAX_VARCHAR_LENGTH = 65534;
    public static final long MAX_NCHAR_LENGTH = 4000;
    public static final long MAX_NVARCHAR_LENGTH = 32767;
    public static final long MAX_UROWID_LENGTH = 4000;
    public static final int MAX_JSON_LENGTH = 32 * 1024 * 1024;
    public static final long BYTES_4GB = (long) Math.pow(2, 32);

    public static final YashanDbTypeConverter INSTANCE = new YashanDbTypeConverter();
    static final Set<SqlType> SUPPORT_FLOAT32_VECTOR =
            Collections.unmodifiableSet(
                    EnumSet.of(SqlType.FLOAT, SqlType.INT, SqlType.SMALLINT, SqlType.TINYINT));
    static final Set<SqlType> SUPPORT_FLOAT64_VECTOR =
            Collections.unmodifiableSet(EnumSet.of(SqlType.BIGINT, SqlType.DOUBLE));

    public YashanDbTypeConverter() {}

    @Override
    public String identifier() {
        return DatabaseIdentifier.YASHANDB;
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

        String yashanType = typeDefine.getDataType().toUpperCase();

        switch (yashanType) {
                // ====================== Integer types ======================
            case TINYINT:
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case INT:
            case INTEGER:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case BIGINT:
                builder.dataType(BasicType.LONG_TYPE);
                break;

                // ====================== NUMBER ======================
            case NUMBER:
                DecimalType decimalType;
                if (typeDefine.getPrecision() != null && typeDefine.getPrecision() > 0) {
                    decimalType =
                            new DecimalType(
                                    typeDefine.getPrecision().intValue(), typeDefine.getScale());
                } else {
                    decimalType = new DecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
                }
                builder.dataType(decimalType);
                builder.columnLength((long) decimalType.getPrecision());
                builder.scale(decimalType.getScale());
                break;

                // ====================== Floating-point types ======================
            case FLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case DOUBLE:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;

                // ====================== Boolean type ======================
            case BOOLEAN:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;

                // ====================== BIT type ======================
            case BIT:
                builder.dataType(BasicType.LONG_TYPE);
                break;

                // ====================== Character types ======================
            case CHAR:
            case VARCHAR:
            case VARCHAR2:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                break;
            case NCHAR:
            case NVARCHAR:
            case NVARCHAR2:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(
                        TypeDefineUtils.doubleByteTo4ByteLength(typeDefine.getLength()));
                break;

                // ====================== ROWID types ======================
            case ROWID:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(MAX_ROWID_LENGTH);
                break;
            case UROWID:
                builder.dataType(BasicType.STRING_TYPE);
                if (typeDefine.getLength() != null && typeDefine.getLength() > 0) {
                    builder.columnLength(typeDefine.getLength());
                } else {
                    builder.columnLength(MAX_UROWID_LENGTH);
                }
                break;

                // ====================== XML types ======================
            case XMLTYPE:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(typeDefine.getLength());
                break;

                // ====================== JSON type ======================
            case JSON:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength((long) MAX_JSON_LENGTH);
                break;

                // ====================== LOB text types ======================
            case CLOB:
            case NCLOB:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(BYTES_4GB - 1);
                break;

                // ====================== Binary / LOB types ======================
            case BLOB:
                builder.sourceType(BLOB);
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(typeDefine.getLength());
                break;
            case RAW:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                if (typeDefine.getLength() == null || typeDefine.getLength() == 0) {
                    builder.columnLength(MAX_RAW_LENGTH);
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                break;

                // ====================== Date & Time types ======================
            case DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                break;
            case TIME:
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                builder.columnLength(typeDefine.getPrecision());
                break;
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                if (typeDefine.getScale() == null) {
                    builder.scale(TIMESTAMP_DEFAULT_SCALE);
                } else {
                    builder.scale(typeDefine.getScale());
                }
                break;

                // ====================== Interval types (as STRING) ======================
            case INTERVAL_YEAR_TO_MONTH:
            case INTERVAL_DAY_TO_SECOND:
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case VECTOR_NAME:
                builder.dataType(VectorType.VECTOR_FLOAT_TYPE);
                builder.scale(typeDefine.getScale());
                builder.columnLength(typeDefine.getLength());
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.YASHANDB, yashanType, typeDefine.getName());
        }
        return builder.build();
    }

    @Override
    public BasicTypeDefine<?> reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder<?> builder =
                BasicTypeDefine.builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());
        switch (column.getDataType().getSqlType()) {
            case BOOLEAN:
                builder.columnType(BOOLEAN);
                builder.dataType(BOOLEAN);
                break;
            case TINYINT:
                builder.columnType(TINYINT);
                builder.dataType(TINYINT);
                break;
            case SMALLINT:
                builder.columnType(SMALLINT);
                builder.dataType(SMALLINT);
                break;
            case INT:
                builder.columnType(INT);
                builder.dataType(INT);
                break;
            case BIGINT:
                builder.columnType(BIGINT);
                builder.dataType(BIGINT);
                break;
            case FLOAT:
                builder.columnType(FLOAT);
                builder.dataType(FLOAT);
                break;
            case DOUBLE:
                builder.columnType(DOUBLE);
                builder.dataType(DOUBLE);
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
                builder.columnType(String.format("%s(%s,%s)", NUMBER, precision, scale));
                builder.dataType(NUMBER);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case BYTES:
                builder.columnType(BLOB);
                builder.dataType(BLOB);
                break;
            case ARRAY:
                // YashanDB does not have a native ARRAY column type, store as vector
                if (SUPPORT_FLOAT32_VECTOR.contains(
                        ((ArrayType) column.getDataType()).getElementType().getSqlType())) {
                    log.warn(
                            "The column {} with type ARRAY will be converted to VECTOR in YashanDB",
                            column.getName());
                    builder.columnType(
                            String.format("%s(%s,FLOAT32)", VECTOR_NAME, column.getColumnLength()));
                    builder.dataType(VECTOR_NAME);
                } else if (SUPPORT_FLOAT64_VECTOR.contains(
                        ((ArrayType) column.getDataType()).getElementType().getSqlType())) {
                    log.warn(
                            "The column {} with type ARRAY will be converted to VECTOR in YashanDB",
                            column.getName());
                    builder.columnType(
                            String.format("%s(%s,FLOAT64)", VECTOR_NAME, column.getColumnLength()));
                    builder.dataType(VECTOR_NAME);
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupport "
                                    + ((ArrayType) column.getDataType())
                                            .getElementType()
                                            .getSqlType()
                                    + " ARRAY");
                }
                break;
            case FLOAT_VECTOR:
            case FLOAT16_VECTOR:
                builder.columnType(String.format("%s(%s)", VECTOR_NAME, column.getScale()));
                builder.dataType(VECTOR_NAME);
                break;
            case BFLOAT16_VECTOR:
                // Vector types are stored as VECTOR in YashanDB
                log.warn(
                        "The column {} with type {} will be converted to VECTOR in YashanDB",
                        column.getName(),
                        column.getDataType().getSqlType().name());
                builder.columnType(String.format("%s(%s)", VECTOR_NAME, column.getScale()));
                builder.dataType(VECTOR_NAME);
                break;
            case STRING:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(String.format("%s(%s)", VARCHAR, MAX_VARCHAR_LENGTH));
                    builder.dataType(VARCHAR);
                } else if (column.getColumnLength() <= MAX_VARCHAR_LENGTH) {
                    builder.columnType(String.format("%s(%s)", VARCHAR, column.getColumnLength()));
                    builder.dataType(VARCHAR);
                } else {
                    builder.columnType(CLOB);
                    builder.dataType(CLOB);
                }
                break;
            case DATE:
                builder.columnType(DATE);
                builder.dataType(DATE);
                break;
            case TIME:
                builder.columnType(TIME);
                builder.dataType(TIME);
                break;
            case TIMESTAMP:
                if (column.getScale() == null || column.getScale() <= 0) {
                    builder.columnType(TIMESTAMP);
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
                builder.dataType(TIMESTAMP);
                break;
            case TIMESTAMP_TZ:
                if (column.getScale() == null || column.getScale() <= 0) {
                    builder.columnType(TIMESTAMP_WITH_TIME_ZONE);
                } else {
                    int tsScale = column.getScale();
                    if (tsScale > MAX_TIMESTAMP_SCALE) {
                        tsScale = MAX_TIMESTAMP_SCALE;
                        log.warn(
                                "The timestamp column {} type timestamp({}) is out of range, "
                                        + "which exceeds the maximum scale of {}, "
                                        + "it will be converted to timestamp({})",
                                column.getName(),
                                column.getScale(),
                                MAX_TIMESTAMP_SCALE,
                                tsScale);
                    }
                    builder.columnType(String.format("TIMESTAMP(%s) WITH TIME ZONE", tsScale));
                    builder.scale(tsScale);
                }
                builder.dataType(TIMESTAMP_WITH_TIME_ZONE);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.YASHANDB,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        return builder.build();
    }
}
