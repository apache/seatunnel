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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.iris;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

/**
 * reference
 * https://docs.intersystems.com/iris20241/csp/docbook/DocBook.UI.Page.cls?KEY=RSQL_datatype#RSQL_datatype_view_data_type_mappings_to_intersyst
 */
@Slf4j
@AutoService(TypeConverter.class)
public class IrisTypeConverter implements TypeConverter<BasicTypeDefine> {
    // ============================data types=====================
    public static final String IRIS_NULL = "NULL";

    // -------------------------number----------------------------
    public static final String IRIS_NUMERIC = "NUMERIC";
    public static final String IRIS_MONEY = "MONEY";
    public static final String IRIS_SMALLMONEY = "SMALLMONEY";
    public static final String IRIS_NUMBER = "NUMBER";
    public static final String IRIS_DEC = "DEC";
    public static final String IRIS_DECIMAL = "DECIMAL";
    public static final String IRIS_INTEGER = "INTEGER";
    public static final String IRIS_INT = "INT";
    public static final String IRIS_ROWVERSION = "ROWVERSION";
    public static final String IRIS_BIGINT = "BIGINT";
    public static final String IRIS_SERIAL = "SERIAL";

    public static final String IRIS_TINYINT = "TINYINT";
    public static final String IRIS_SMALLINT = "SMALLINT";
    public static final String IRIS_MEDIUMINT = "MEDIUMINT";
    public static final String IRIS_FLOAT = "FLOAT";
    public static final String IRIS_DOUBLE = "DOUBLE";
    public static final String IRIS_REAL = "REAL";
    public static final String IRIS_DOUBLE_PRECISION = "DOUBLE PRECISION";

    // ----------------------------string-------------------------
    public static final String IRIS_CHAR = "CHAR";
    public static final String IRIS_CHAR_VARYING = "CHAR VARYING";
    public static final String IRIS_CHARACTER_VARYING = "CHARACTER VARYING";
    public static final String IRIS_NATIONAL_CHAR = "NATIONAL CHAR";
    public static final String IRIS_NATIONAL_CHAR_VARYING = "NATIONAL CHAR VARYING";
    public static final String IRIS_NATIONAL_CHARACTER = "NATIONAL CHARACTER";
    public static final String IRIS_NATIONAL_CHARACTER_VARYING = "NATIONAL CHARACTER VARYING";
    public static final String IRIS_NATIONAL_VARCHAR = "NATIONAL VARCHAR";
    public static final String IRIS_NCHAR = "NCHAR";
    public static final String IRIS_NVARCHAR = "NVARCHAR";
    public static final String IRIS_SYSNAME = "SYSNAME";
    public static final String IRIS_VARCHAR2 = "VARCHAR2";
    public static final String IRIS_VARCHAR = "VARCHAR";
    public static final String IRIS_UNIQUEIDENTIFIER = "UNIQUEIDENTIFIER";
    public static final String IRIS_GUID = "GUID";
    public static final String IRIS_CHARACTER = "CHARACTER";
    public static final String IRIS_NTEXT = "NTEXT";
    public static final String IRIS_CLOB = "CLOB";
    public static final String IRIS_LONG_VARCHAR = "LONG VARCHAR";
    public static final String IRIS_LONG = "LONG";
    public static final String IRIS_LONGTEXT = "LONGTEXT";
    public static final String IRIS_MEDIUMTEXT = "MEDIUMTEXT";
    public static final String IRIS_TEXT = "TEXT";
    public static final String IRIS_LONGVARCHAR = "LONGVARCHAR";

    // ------------------------------time-------------------------
    public static final String IRIS_DATE = "DATE";

    public static final String IRIS_TIME = "TIME";

    public static final String IRIS_TIMESTAMP = "TIMESTAMP";
    public static final String IRIS_POSIXTIME = "POSIXTIME";
    public static final String IRIS_TIMESTAMP2 = "TIMESTAMP2";

    public static final String IRIS_DATETIME = "DATETIME";
    public static final String IRIS_SMALLDATETIME = "SMALLDATETIME";
    public static final String IRIS_DATETIME2 = "DATETIME2";

    // ---------------------------binary---------------------------
    public static final String IRIS_BINARY = "BINARY";
    public static final String IRIS_VARBINARY = "VARBINARY";
    public static final String IRIS_RAW = "RAW";
    public static final String IRIS_LONGVARBINARY = "LONGVARBINARY";
    public static final String IRIS_BINARY_VARYING = "BINARY VARYING";
    public static final String IRIS_BLOB = "BLOB";
    public static final String IRIS_IMAGE = "IMAGE";
    public static final String IRIS_LONG_BINARY = "LONG BINARY";
    public static final String IRIS_LONG_RAW = "LONG RAW";

    // ---------------------------other---------------------------
    public static final String IRIS_BIT = "BIT";

    public static final int MAX_SCALE = 18;
    public static final int DEFAULT_SCALE = 0;
    public static final int MAX_PRECISION = 19 + MAX_SCALE;
    public static final int DEFAULT_PRECISION = 15;
    public static final int MAX_TIME_SCALE = 9;
    public static final long GUID_LENGTH = 36;
    public static final long MAX_VARCHAR_LENGTH = Integer.MAX_VALUE;
    public static final long MAX_BINARY_LENGTH = Integer.MAX_VALUE;
    public static final IrisTypeConverter INSTANCE = new IrisTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.IRIS;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        Long typeDefineLength = typeDefine.getLength();
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .sourceType(typeDefine.getColumnType())
                        .columnLength(typeDefineLength)
                        .scale(typeDefine.getScale())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());
        String irisDataType = typeDefine.getDataType().toUpperCase();
        long charOrBinaryLength =
                Objects.nonNull(typeDefineLength) && typeDefineLength > 0 ? typeDefineLength : 1;
        switch (irisDataType) {
            case IRIS_NULL:
                builder.dataType(BasicType.VOID_TYPE);
                break;
            case IRIS_BIT:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case IRIS_NUMERIC:
            case IRIS_MONEY:
            case IRIS_SMALLMONEY:
            case IRIS_NUMBER:
            case IRIS_DEC:
            case IRIS_DECIMAL:
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
            case IRIS_INT:
            case IRIS_INTEGER:
            case IRIS_MEDIUMINT:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case IRIS_ROWVERSION:
            case IRIS_BIGINT:
            case IRIS_SERIAL:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case IRIS_TINYINT:
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case IRIS_SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case IRIS_FLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case IRIS_DOUBLE:
            case IRIS_REAL:
            case IRIS_DOUBLE_PRECISION:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case IRIS_CHAR:
            case IRIS_CHAR_VARYING:
            case IRIS_CHARACTER_VARYING:
            case IRIS_NATIONAL_CHAR:
            case IRIS_NATIONAL_CHAR_VARYING:
            case IRIS_NATIONAL_CHARACTER:
            case IRIS_NATIONAL_CHARACTER_VARYING:
            case IRIS_NATIONAL_VARCHAR:
            case IRIS_NCHAR:
            case IRIS_SYSNAME:
            case IRIS_VARCHAR2:
            case IRIS_VARCHAR:
            case IRIS_NVARCHAR:
            case IRIS_UNIQUEIDENTIFIER:
            case IRIS_GUID:
            case IRIS_CHARACTER:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(charOrBinaryLength);
                break;
            case IRIS_NTEXT:
            case IRIS_CLOB:
            case IRIS_LONG_VARCHAR:
            case IRIS_LONG:
            case IRIS_LONGTEXT:
            case IRIS_MEDIUMTEXT:
            case IRIS_TEXT:
            case IRIS_LONGVARCHAR:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(Long.valueOf(Integer.MAX_VALUE));
                break;
            case IRIS_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case IRIS_TIME:
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                break;
            case IRIS_DATETIME:
            case IRIS_DATETIME2:
            case IRIS_SMALLDATETIME:
            case IRIS_TIMESTAMP:
            case IRIS_TIMESTAMP2:
            case IRIS_POSIXTIME:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                break;
            case IRIS_BINARY:
            case IRIS_BINARY_VARYING:
            case IRIS_RAW:
            case IRIS_VARBINARY:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(charOrBinaryLength);
                break;
            case IRIS_LONGVARBINARY:
            case IRIS_BLOB:
            case IRIS_IMAGE:
            case IRIS_LONG_BINARY:
            case IRIS_LONG_RAW:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(Long.valueOf(Integer.MAX_VALUE));
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.IRIS, irisDataType, typeDefine.getName());
        }
        return builder.build();
    }

    @Override
    public BasicTypeDefine reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder =
                BasicTypeDefine.builder()
                        .name(column.getName())
                        .precision(column.getColumnLength())
                        .length(column.getColumnLength())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .scale(column.getScale())
                        .defaultValue(column.getDefaultValue());
        switch (column.getDataType().getSqlType()) {
            case NULL:
                builder.columnType(IRIS_NULL);
                builder.dataType(IRIS_NULL);
                break;
            case STRING:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(String.format("%s(%s)", IRIS_VARCHAR, MAX_VARCHAR_LENGTH));
                    builder.dataType(IRIS_VARCHAR);
                } else if (column.getColumnLength() < MAX_VARCHAR_LENGTH) {
                    builder.columnType(
                            String.format("%s(%s)", IRIS_VARCHAR, column.getColumnLength()));
                    builder.dataType(IRIS_VARCHAR);
                } else {
                    builder.columnType(IRIS_LONG_VARCHAR);
                    builder.dataType(IRIS_LONG_VARCHAR);
                }
                break;
            case BOOLEAN:
                builder.columnType(IRIS_BIT);
                builder.dataType(IRIS_BIT);
                break;
            case TINYINT:
                builder.columnType(IRIS_TINYINT);
                builder.dataType(IRIS_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(IRIS_SMALLINT);
                builder.dataType(IRIS_SMALLINT);
                break;
            case INT:
                builder.columnType(IRIS_INTEGER);
                builder.dataType(IRIS_INTEGER);
                break;
            case BIGINT:
                builder.columnType(IRIS_BIGINT);
                builder.dataType(IRIS_BIGINT);
                break;
            case FLOAT:
                builder.columnType(IRIS_FLOAT);
                builder.dataType(IRIS_FLOAT);
                break;
            case DOUBLE:
                builder.columnType(IRIS_DOUBLE);
                builder.dataType(IRIS_DOUBLE);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) column.getDataType();
                long precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
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
                if (precision < scale) {
                    precision = scale;
                }
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
                    scale = MAX_SCALE;
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
                builder.columnType(String.format("%s(%s,%s)", IRIS_DECIMAL, precision, scale));
                builder.dataType(IRIS_DECIMAL);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case BYTES:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(IRIS_LONG_BINARY);
                    builder.dataType(IRIS_LONG_BINARY);
                } else if (column.getColumnLength() < MAX_BINARY_LENGTH) {
                    builder.dataType(IRIS_BINARY);
                    builder.columnType(
                            String.format("%s(%s)", IRIS_BINARY, column.getColumnLength()));
                } else {
                    builder.columnType(IRIS_LONG_BINARY);
                    builder.dataType(IRIS_LONG_BINARY);
                }
                break;
            case DATE:
                builder.columnType(IRIS_DATE);
                builder.dataType(IRIS_DATE);
                break;
            case TIME:
                builder.dataType(IRIS_TIME);
                if (Objects.nonNull(column.getScale()) && column.getScale() > 0) {
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
                    builder.columnType(String.format("%s(%s)", IRIS_TIME, timeScale));
                    builder.scale(timeScale);
                } else {
                    builder.columnType(IRIS_TIME);
                }
                break;
            case TIMESTAMP:
                builder.columnType(IRIS_TIMESTAMP2);
                builder.dataType(IRIS_TIMESTAMP2);
                break;

            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.IRIS,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        return builder.build();
    }
}
