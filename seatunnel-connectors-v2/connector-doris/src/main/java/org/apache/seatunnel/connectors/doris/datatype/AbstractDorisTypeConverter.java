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

package org.apache.seatunnel.connectors.doris.datatype;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonError;

import lombok.extern.slf4j.Slf4j;

import java.util.Locale;

import static org.apache.seatunnel.connectors.doris.config.DorisOptions.IDENTIFIER;

@Slf4j
public abstract class AbstractDorisTypeConverter implements TypeConverter<BasicTypeDefine> {
    public static final String DORIS_NULL = "NULL";
    public static final String DORIS_BOOLEAN = "BOOLEAN";
    public static final String DORIS_TINYINT = "TINYINT";
    public static final String DORIS_SMALLINT = "SMALLINT";
    public static final String DORIS_INT = "INT";
    public static final String DORIS_BIGINT = "BIGINT";
    public static final String DORIS_LARGEINT = "LARGEINT";
    public static final String DORIS_FLOAT = "FLOAT";
    public static final String DORIS_DOUBLE = "DOUBLE";
    public static final String DORIS_DECIMAL = "DECIMAL";
    public static final String DORIS_DECIMALV3 = "DECIMALV3";
    public static final String DORIS_DATE = "DATE";
    public static final String DORIS_DATETIME = "DATETIME";
    public static final String DORIS_CHAR = "CHAR";
    public static final String DORIS_VARCHAR = "VARCHAR";
    public static final String DORIS_STRING = "STRING";

    public static final String DORIS_BOOLEAN_ARRAY = "ARRAY<boolean>";
    public static final String DORIS_TINYINT_ARRAY = "ARRAY<tinyint>";
    public static final String DORIS_SMALLINT_ARRAY = "ARRAY<smallint>";
    public static final String DORIS_INT_ARRAY = "ARRAY<int(11)>";
    public static final String DORIS_BIGINT_ARRAY = "ARRAY<bigint>";
    public static final String DORIS_FLOAT_ARRAY = "ARRAY<float>";
    public static final String DORIS_DOUBLE_ARRAY = "ARRAY<double>";
    public static final String DORIS_DECIMALV3_ARRAY = "ARRAY<DECIMALV3>";
    public static final String DORIS_DECIMALV3_ARRAY_COLUMN_TYPE_TMP = "ARRAY<DECIMALV3(%s, %s)>";
    public static final String DORIS_DATEV2_ARRAY = "ARRAY<DATEV2>";
    public static final String DORIS_DATETIMEV2_ARRAY = "ARRAY<DATETIMEV2>";
    public static final String DORIS_STRING_ARRAY = "ARRAY<STRING>";

    // Because can not get the column length from array, So the following types of arrays cannot be
    // generated properly.
    public static final String DORIS_LARGEINT_ARRAY = "ARRAY<largeint>";
    public static final String DORIS_CHAR_ARRAY = "ARRAY<CHAR>";
    public static final String DORIS_CHAR_ARRAY_COLUMN_TYPE_TMP = "ARRAY<CHAR(%s)>";
    public static final String DORIS_VARCHAR_ARRAY = "ARRAY<VARCHAR>";
    public static final String DORIS_VARCHAR_ARRAY_COLUMN_TYPE_TMP = "ARRAY<VARCHAR(%s)>";

    public static final String DORIS_JSON = "JSON";
    public static final String DORIS_JSONB = "JSONB";

    public static final Long DEFAULT_PRECISION = 9L;
    public static final Long MAX_PRECISION = 38L;

    public static final Integer DEFAULT_SCALE = 0;
    public static final Integer MAX_SCALE = 10;

    public static final Integer MAX_DATETIME_SCALE = 6;

    // Min value of LARGEINT is -170141183460469231731687303715884105728, it will use 39 bytes in
    // UTF-8.
    // Add a bit to prevent overflow
    public static final long MAX_DORIS_LARGEINT_TO_VARCHAR_LENGTH = 39L;

    public static final long POWER_2_8 = (long) Math.pow(2, 8);
    public static final long POWER_2_16 = (long) Math.pow(2, 16);
    public static final long MAX_STRING_LENGTH = 2147483643;

    protected PhysicalColumn.PhysicalColumnBuilder getPhysicalColumnBuilder(
            BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .sourceType(typeDefine.getColumnType())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());
        return builder;
    }

    protected BasicTypeDefine.BasicTypeDefineBuilder getBasicTypeDefineBuilder(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder =
                BasicTypeDefine.builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());
        return builder;
    }

    protected String getDorisColumnName(BasicTypeDefine typeDefine) {
        String dorisColumnType = typeDefine.getColumnType();
        return getDorisColumnName(dorisColumnType);
    }

    protected String getDorisColumnName(String dorisColumnType) {
        dorisColumnType = dorisColumnType.toUpperCase(Locale.ROOT);
        int idx = dorisColumnType.indexOf("(");
        int idx2 = dorisColumnType.indexOf("<");
        if (idx != -1) {
            dorisColumnType = dorisColumnType.substring(0, idx);
        }
        if (idx2 != -1) {
            dorisColumnType = dorisColumnType.substring(0, idx2);
        }
        return dorisColumnType;
    }

    public void sampleTypeConverter(
            PhysicalColumn.PhysicalColumnBuilder builder,
            BasicTypeDefine typeDefine,
            String dorisColumnType) {
        switch (dorisColumnType) {
            case DORIS_NULL:
                builder.dataType(BasicType.VOID_TYPE);
                break;
            case DORIS_BOOLEAN:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case DORIS_TINYINT:
                if (typeDefine.getColumnType().equalsIgnoreCase("tinyint(1)")) {
                    builder.dataType(BasicType.BOOLEAN_TYPE);
                } else {
                    builder.dataType(BasicType.BYTE_TYPE);
                }
                break;
            case DORIS_SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case DORIS_INT:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case DORIS_BIGINT:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case DORIS_FLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case DORIS_DOUBLE:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case DORIS_CHAR:
            case DORIS_VARCHAR:
                if (typeDefine.getLength() != null && typeDefine.getLength() > 0) {
                    builder.columnLength(typeDefine.getLength());
                }
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case DORIS_LARGEINT:
                DecimalType decimalType;
                decimalType = new DecimalType(20, 0);
                builder.dataType(decimalType);
                builder.columnLength(20L);
                builder.scale(0);
                break;
            case DORIS_STRING:
            case DORIS_JSON:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(MAX_STRING_LENGTH);
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        IDENTIFIER, dorisColumnType, typeDefine.getName());
        }
    }

    protected void sampleReconvertString(
            Column column, BasicTypeDefine.BasicTypeDefineBuilder builder) {
        if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
            builder.columnType(DORIS_STRING);
            builder.dataType(DORIS_STRING);
            return;
        }

        if (column.getColumnLength() < POWER_2_8) {
            if (column.getSourceType() != null
                    && column.getSourceType().toUpperCase(Locale.ROOT).startsWith(DORIS_VARCHAR)) {
                builder.columnType(
                        String.format("%s(%s)", DORIS_VARCHAR, column.getColumnLength()));
                builder.dataType(DORIS_VARCHAR);
            } else {
                builder.columnType(String.format("%s(%s)", DORIS_CHAR, column.getColumnLength()));
                builder.dataType(DORIS_CHAR);
            }
            return;
        }

        if (column.getColumnLength() <= 65533) {
            builder.columnType(String.format("%s(%s)", DORIS_VARCHAR, column.getColumnLength()));
            builder.dataType(DORIS_VARCHAR);
            return;
        }

        if (column.getColumnLength() <= MAX_STRING_LENGTH) {
            builder.columnType(DORIS_STRING);
            builder.dataType(DORIS_STRING);
            return;
        }

        if (column.getColumnLength() > MAX_STRING_LENGTH) {
            log.warn(
                    String.format(
                            "The String type in Doris can only store up to 2GB bytes, and the current field [%s] length is [%s] bytes. If it is greater than the maximum length of the String in Doris, it may not be able to write data",
                            column.getName(), column.getColumnLength()));
            builder.columnType(DORIS_STRING);
            builder.dataType(DORIS_STRING);
            return;
        }

        throw CommonError.convertToConnectorTypeError(
                IDENTIFIER, column.getDataType().getSqlType().name(), column.getName());
    }

    protected BasicTypeDefine sampleReconvert(
            Column column, BasicTypeDefine.BasicTypeDefineBuilder builder) {

        switch (column.getDataType().getSqlType()) {
            case NULL:
                builder.columnType(DORIS_NULL);
                builder.dataType(DORIS_NULL);
                break;
            case BYTES:
                builder.columnType(DORIS_STRING);
                builder.dataType(DORIS_STRING);
                break;
            case BOOLEAN:
                builder.columnType(DORIS_BOOLEAN);
                builder.dataType(DORIS_BOOLEAN);
                builder.length(1L);
                break;
            case TINYINT:
                builder.columnType(DORIS_TINYINT);
                builder.dataType(DORIS_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(DORIS_SMALLINT);
                builder.dataType(DORIS_SMALLINT);
                break;
            case INT:
                builder.columnType(DORIS_INT);
                builder.dataType(DORIS_INT);
                break;
            case BIGINT:
                builder.columnType(DORIS_BIGINT);
                builder.dataType(DORIS_BIGINT);
                break;
            case FLOAT:
                builder.columnType(DORIS_FLOAT);
                builder.dataType(DORIS_FLOAT);
                break;
            case DOUBLE:
                builder.columnType(DORIS_DOUBLE);
                builder.dataType(DORIS_DOUBLE);
                break;
            case DECIMAL:
                // DORIS LARGEINT
                if (column.getSourceType() != null
                        && column.getSourceType().equalsIgnoreCase(DORIS_LARGEINT)) {
                    builder.dataType(DORIS_LARGEINT);
                    builder.columnType(DORIS_LARGEINT);
                    break;
                }
                DecimalType decimalType = (DecimalType) column.getDataType();
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                if (precision <= 0) {
                    precision = MAX_PRECISION.intValue();
                    scale = MAX_SCALE;
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
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which exceeds the maximum precision of {}, "
                                    + "it will be converted to varchar(200)",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            MAX_PRECISION);
                    builder.dataType(DORIS_VARCHAR);
                    builder.columnType(String.format("%s(%s)", DORIS_VARCHAR, 200));
                    break;
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
                } else if (scale > precision) {
                    scale = precision;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which exceeds the maximum scale of {}, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            precision,
                            precision,
                            scale);
                }

                builder.columnType(String.format("%s(%s,%s)", DORIS_DECIMALV3, precision, scale));
                builder.dataType(DORIS_DECIMALV3);
                builder.precision((long) precision);
                builder.scale(scale);
                break;
            case TIME:
                builder.length(8L);
                builder.columnType(String.format("%s(%s)", DORIS_VARCHAR, 8));
                builder.dataType(DORIS_VARCHAR);
                break;
            case ARRAY:
                SeaTunnelDataType<?> dataType = column.getDataType();
                SeaTunnelDataType elementType = null;
                if (dataType instanceof ArrayType) {
                    ArrayType arrayType = (ArrayType) dataType;
                    elementType = arrayType.getElementType();
                }

                reconvertBuildArrayInternal(elementType, builder, column.getName());
                break;
            case ROW:
                builder.columnType(DORIS_JSON);
                builder.dataType(DORIS_JSON);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        IDENTIFIER, column.getDataType().getSqlType().name(), column.getName());
        }
        return builder.build();
    }

    private void reconvertBuildArrayInternal(
            SeaTunnelDataType elementType,
            BasicTypeDefine.BasicTypeDefineBuilder builder,
            String columnName) {
        switch (elementType.getSqlType()) {
            case BOOLEAN:
                builder.columnType(DORIS_BOOLEAN_ARRAY);
                builder.dataType(DORIS_BOOLEAN_ARRAY);
                break;
            case TINYINT:
                builder.columnType(DORIS_TINYINT_ARRAY);
                builder.dataType(DORIS_TINYINT_ARRAY);
                break;
            case SMALLINT:
                builder.columnType(DORIS_SMALLINT_ARRAY);
                builder.dataType(DORIS_SMALLINT_ARRAY);
                break;
            case INT:
                builder.columnType(DORIS_INT_ARRAY);
                builder.dataType(DORIS_INT_ARRAY);
                break;
            case BIGINT:
                builder.columnType(DORIS_BIGINT_ARRAY);
                builder.dataType(DORIS_BIGINT_ARRAY);
                break;
            case FLOAT:
                builder.columnType(DORIS_FLOAT_ARRAY);
                builder.dataType(DORIS_FLOAT_ARRAY);
                break;
            case DOUBLE:
                builder.columnType(DORIS_DOUBLE_ARRAY);
                builder.dataType(DORIS_DOUBLE_ARRAY);
                break;
            case DECIMAL:
                int[] precisionAndScale = getPrecisionAndScale(elementType.toString());
                builder.columnType(
                        String.format(
                                DORIS_DECIMALV3_ARRAY_COLUMN_TYPE_TMP,
                                precisionAndScale[0],
                                precisionAndScale[1]));
                builder.dataType(DORIS_DECIMALV3_ARRAY);
                break;
            case STRING:
            case TIME:
                builder.columnType(DORIS_STRING_ARRAY);
                builder.dataType(DORIS_STRING_ARRAY);
                break;
            case DATE:
                builder.columnType(DORIS_DATEV2_ARRAY);
                builder.dataType(DORIS_DATEV2_ARRAY);
                break;
            case TIMESTAMP:
                builder.columnType(DORIS_DATETIMEV2_ARRAY);
                builder.dataType(DORIS_DATETIMEV2_ARRAY);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        IDENTIFIER, elementType.getSqlType().name(), columnName);
        }
    }

    protected static int[] getPrecisionAndScale(String decimalTypeDefinition) {
        // Remove the "DECIMALV3" part and the parentheses
        decimalTypeDefinition = decimalTypeDefinition.toUpperCase(Locale.ROOT);
        String numericPart = decimalTypeDefinition.replace("DECIMALV3(", "").replace(")", "");
        numericPart = numericPart.replace("DECIMAL(", "").replace(")", "");

        // Split by comma to separate precision and scale
        String[] parts = numericPart.split(",");

        if (parts.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid DECIMAL definition: " + decimalTypeDefinition);
        }

        // Parse precision and scale from the split parts
        int precision = Integer.parseInt(parts[0].trim());
        int scale = Integer.parseInt(parts[1].trim());

        // Return an array containing precision and scale
        return new int[] {precision, scale};
    }
}
