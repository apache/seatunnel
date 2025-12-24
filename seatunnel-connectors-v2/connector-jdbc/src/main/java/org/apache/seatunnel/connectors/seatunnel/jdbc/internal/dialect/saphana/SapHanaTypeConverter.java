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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.saphana;

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

import java.util.Arrays;
import java.util.List;

// reference
// https://help.sap.com/docs/SAP_HANA_PLATFORM/4fe29514fd584807ac9f2a04f6754767/20a1569875191014b507cf392724b7eb.html?locale=en-US
@Slf4j
@AutoService(TypeConverter.class)
public class SapHanaTypeConverter implements TypeConverter<BasicTypeDefine> {
    // ============================data types=====================

    // -------------------------binary-------------------------
    public static final String HANA_BINARY = "BINARY";
    public static final String HANA_VARBINARY = "VARBINARY";

    // -------------------------boolean----------------------------
    public static final String HANA_BOOLEAN = "BOOLEAN";

    // -------------------------string----------------------------
    public static final String HANA_VARCHAR = "VARCHAR";
    public static final String HANA_NVARCHAR = "NVARCHAR";
    public static final String HANA_ALPHANUM = "ALPHANUM";
    public static final String HANA_SHORTTEXT = "SHORTTEXT";

    // -------------------------datetime----------------------------
    public static final String HANA_DATE = "DATE";
    public static final String HANA_TIME = "TIME";
    public static final String HANA_SECONDDATE = "SECONDDATE";
    public static final String HANA_TIMESTAMP = "TIMESTAMP";

    // -------------------------lob----------------------------
    public static final String HANA_BLOB = "BLOB";
    public static final String HANA_CLOB = "CLOB";
    public static final String HANA_NCLOB = "NCLOB";
    public static final String HANA_TEXT = "TEXT";
    public static final String HANA_BINTEXT = "BINTEXT";

    // -------------------------array----------------------------
    public static final String HANA_ARRAY = "ARRAY";

    // -------------------------number----------------------------
    public static final String HANA_TINYINT = "TINYINT";
    public static final String HANA_SMALLINT = "SMALLINT";
    public static final String HANA_INTEGER = "INTEGER";
    public static final String HANA_BIGINT = "BIGINT";
    public static final String HANA_SMALLDECIMAL = "SMALLDECIMAL";
    public static final String HANA_DECIMAL = "DECIMAL";
    public static final String HANA_DOUBLE = "DOUBLE";
    public static final String HANA_REAL = "REAL";

    // -------------------------special----------------------------
    public static final String HANA_ST_POINT = "ST_POINT";
    public static final String HANA_ST_GEOMETRY = "ST_GEOMETRY";

    public static final int MAX_PRECISION = 38;
    public static final int DEFAULT_PRECISION = MAX_PRECISION;
    public static final int MAX_SCALE = 6176;
    public static final int MAX_SMALL_DECIMAL_SCALE = 368;
    public static final int DEFAULT_SCALE = 0;
    public static final int TIMESTAMP_DEFAULT_SCALE = 7;
    public static final int MAX_TIMESTAMP_SCALE = 7;
    public static final long MAX_BINARY_LENGTH = 5000;
    public static final long MAX_LOB_LENGTH = Integer.MAX_VALUE;
    public static final long MAX_NVARCHAR_LENGTH = 5000;

    public static final List<String> shouldAppendLength =
            Arrays.asList(
                    HANA_BINARY,
                    HANA_VARBINARY,
                    HANA_VARCHAR,
                    HANA_NVARCHAR,
                    HANA_ALPHANUM,
                    HANA_SHORTTEXT);

    public static final SapHanaTypeConverter INSTANCE = new SapHanaTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.SAP_HANA;
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

        String hanaType = typeDefine.getDataType().toUpperCase();
        if (typeDefine.getColumnType().endsWith(" ARRAY")) {
            typeDefine.setColumnType(typeDefine.getColumnType().replace(" ARRAY", ""));
            typeDefine.setDataType(removeColumnSizeIfNeed(typeDefine.getColumnType()));
            Column arrayColumn = convert(typeDefine);
            SeaTunnelDataType<?> newType;
            switch (arrayColumn.getDataType().getSqlType()) {
                case STRING:
                    newType = ArrayType.STRING_ARRAY_TYPE;
                    break;
                case BOOLEAN:
                    newType = ArrayType.BOOLEAN_ARRAY_TYPE;
                    break;
                case TINYINT:
                    newType = ArrayType.BYTE_ARRAY_TYPE;
                    break;
                case SMALLINT:
                    newType = ArrayType.SHORT_ARRAY_TYPE;
                    break;
                case INT:
                    newType = ArrayType.INT_ARRAY_TYPE;
                    break;
                case BIGINT:
                    newType = ArrayType.LONG_ARRAY_TYPE;
                    break;
                case FLOAT:
                    newType = ArrayType.FLOAT_ARRAY_TYPE;
                    break;
                case DOUBLE:
                    newType = ArrayType.DOUBLE_ARRAY_TYPE;
                    break;
                case DATE:
                    newType = ArrayType.LOCAL_DATE_ARRAY_TYPE;
                    break;
                case TIME:
                    newType = ArrayType.LOCAL_TIME_ARRAY_TYPE;
                    break;
                case TIMESTAMP:
                    newType = ArrayType.LOCAL_DATE_TIME_ARRAY_TYPE;
                    break;
                default:
                    throw CommonError.unsupportedDataType(
                            "SeaTunnel",
                            arrayColumn.getDataType().getSqlType().toString(),
                            typeDefine.getName());
            }
            return new PhysicalColumn(
                    arrayColumn.getName(),
                    newType,
                    arrayColumn.getColumnLength(),
                    arrayColumn.getScale(),
                    arrayColumn.isNullable(),
                    arrayColumn.getDefaultValue(),
                    arrayColumn.getComment(),
                    arrayColumn.getSourceType() + " ARRAY",
                    arrayColumn.getOptions());
        }
        switch (hanaType) {
            case HANA_BINARY:
            case HANA_VARBINARY:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                if (typeDefine.getLength() == null || typeDefine.getLength() == 0) {
                    builder.columnLength(MAX_BINARY_LENGTH);
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                break;
            case HANA_BOOLEAN:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case HANA_VARCHAR:
            case HANA_ALPHANUM:
            case HANA_CLOB:
            case HANA_NCLOB:
            case HANA_TEXT:
            case HANA_BINTEXT:
                builder.dataType(BasicType.STRING_TYPE);
                if (typeDefine.getLength() == null || typeDefine.getLength() == 0) {
                    builder.columnLength(MAX_LOB_LENGTH);
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                break;
            case HANA_NVARCHAR:
            case HANA_SHORTTEXT:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                break;
            case HANA_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case HANA_TIME:
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                builder.scale(0);
                break;
            case HANA_SECONDDATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(0);
                break;
            case HANA_TIMESTAMP:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                if (typeDefine.getScale() == null) {
                    builder.scale(TIMESTAMP_DEFAULT_SCALE);
                } else {
                    builder.scale(typeDefine.getScale());
                }
                break;
            case HANA_BLOB:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(typeDefine.getLength());
                break;
            case HANA_TINYINT:
            case HANA_SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case HANA_INTEGER:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case HANA_BIGINT:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case HANA_DECIMAL:
                Integer scale = typeDefine.getScale();
                long precision =
                        typeDefine.getLength() != null
                                ? typeDefine.getLength().intValue()
                                : MAX_PRECISION - 4;
                if (scale == null) {
                    builder.dataType(new DecimalType((int) precision, 0));
                    builder.columnLength(precision);
                    builder.scale(0);
                } else if (scale < 0) {
                    int newPrecision = (int) (precision - scale);
                    if (newPrecision == 1) {
                        builder.dataType(BasicType.SHORT_TYPE);
                    } else if (newPrecision <= 9) {
                        builder.dataType(BasicType.INT_TYPE);
                    } else if (newPrecision <= 18) {
                        builder.dataType(BasicType.LONG_TYPE);
                    } else if (newPrecision < 38) {
                        builder.dataType(new DecimalType(newPrecision, 0));
                        builder.columnLength((long) newPrecision);
                    } else {
                        builder.dataType(new DecimalType(DEFAULT_PRECISION, 0));
                        builder.columnLength((long) DEFAULT_PRECISION);
                    }
                } else {
                    builder.dataType(new DecimalType((int) precision, scale));
                    builder.columnLength(precision);
                    builder.scale(scale);
                }
                break;
            case HANA_SMALLDECIMAL:
                int smallDecimalScale = typeDefine.getScale() != null ? typeDefine.getScale() : 0;
                if (typeDefine.getPrecision() == null) {
                    builder.dataType(new DecimalType(DEFAULT_PRECISION, smallDecimalScale));
                    builder.columnLength((long) DEFAULT_PRECISION);
                    builder.scale(smallDecimalScale);
                } else {
                    builder.dataType(
                            new DecimalType(
                                    typeDefine.getPrecision().intValue(), smallDecimalScale));
                    builder.columnLength(typeDefine.getPrecision());
                    builder.scale(smallDecimalScale);
                }
                break;
            case HANA_REAL:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case HANA_DOUBLE:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case HANA_ST_POINT:
            case HANA_ST_GEOMETRY:
                builder.columnLength(typeDefine.getLength());
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.SAP_HANA, hanaType, typeDefine.getName());
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
                builder.columnType(HANA_BOOLEAN);
                builder.dataType(HANA_BOOLEAN);
                builder.length(2L);
                break;
            case TINYINT:
                builder.columnType(HANA_TINYINT);
                builder.dataType(HANA_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(HANA_SMALLINT);
                builder.dataType(HANA_SMALLINT);
                break;
            case INT:
                builder.columnType(HANA_INTEGER);
                builder.dataType(HANA_INTEGER);
                break;
            case BIGINT:
                builder.columnType(HANA_BIGINT);
                builder.dataType(HANA_BIGINT);
                break;
            case FLOAT:
                builder.columnType(HANA_REAL);
                builder.dataType(HANA_REAL);
                break;
            case DOUBLE:
                builder.columnType(HANA_DOUBLE);
                builder.dataType(HANA_DOUBLE);
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
                builder.columnType(String.format("%s(%s,%s)", HANA_DECIMAL, precision, scale));
                builder.dataType(HANA_DECIMAL);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case BYTES:
                builder.columnType(HANA_BLOB);
                builder.dataType(HANA_BLOB);
                break;
            case STRING:
                if (column.getColumnLength() == null
                        || column.getColumnLength() <= MAX_NVARCHAR_LENGTH) {
                    builder.columnType(HANA_NVARCHAR);
                    builder.dataType(HANA_NVARCHAR);
                    builder.length(
                            column.getColumnLength() == null
                                    ? MAX_NVARCHAR_LENGTH
                                    : column.getColumnLength());
                } else {
                    builder.columnType(HANA_CLOB);
                    builder.dataType(HANA_CLOB);
                }
                break;
            case DATE:
                builder.columnType(HANA_DATE);
                builder.dataType(HANA_DATE);
                break;
            case TIME:
                builder.columnType(HANA_TIME);
                builder.dataType(HANA_TIME);
                break;
            case TIMESTAMP:
                if (column.getScale() == null || column.getScale() <= 0) {
                    builder.columnType(HANA_SECONDDATE);
                    builder.dataType(HANA_SECONDDATE);
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
                    builder.columnType(HANA_TIMESTAMP);
                    builder.dataType(HANA_TIMESTAMP);
                    builder.scale(timestampScale);
                }
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.SAP_HANA,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        BasicTypeDefine typeDefine = builder.build();
        typeDefine.setColumnType(
                appendColumnSizeIfNeed(
                        typeDefine.getColumnType(), typeDefine.getLength(), typeDefine.getScale()));
        return typeDefine;
    }

    public static String appendColumnSizeIfNeed(String columnType, Long length, Integer scale) {
        if (shouldAppendLength.contains(columnType) && length != null && length != 0) {
            return columnType + "(" + length + ")";
        } else if (columnType.equalsIgnoreCase(HANA_DECIMAL)
                && length != null
                && scale != null
                && length != 0) {
            return columnType + "(" + length + "," + scale + ")";
        }
        return columnType;
    }

    public static String removeColumnSizeIfNeed(String columnType) {
        for (String s : shouldAppendLength) {
            if (columnType.startsWith(s)) {
                return columnType.split("\\(")[0];
            }
        }
        return columnType;
    }
}
