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

package org.apache.seatunnel.connectors.seatunnel.timeplus.datatype;

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

@Slf4j
public abstract class AbstractTimeplusTypeConverter implements TypeConverter<BasicTypeDefine> {
    public static final String TIMEPLUS_NULL = "null";
    public static final String TIMEPLUS_BOOLEAN = "bool";
    public static final String TIMEPLUS_TINYINT = "tiny_int";
    public static final String TIMEPLUS_SMALLINT = "small_int";
    public static final String TIMEPLUS_INT = "int32";
    public static final String TIMEPLUS_BIGINT = "int64";
    public static final String TIMEPLUS_LARGEINT = "int128";
    public static final String TIMEPLUS_FLOAT = "float32";
    public static final String TIMEPLUS_DOUBLE = "float64";
    public static final String TIMEPLUS_DECIMAL = "decimal";
    public static final String TIMEPLUS_DECIMALV3 = "decimal";
    public static final String TIMEPLUS_DATE = "date";
    public static final String TIMEPLUS_DATETIME = "datetime";
    public static final String TIMEPLUS_CHAR = "string";
    public static final String TIMEPLUS_VARCHAR = "string";
    public static final String TIMEPLUS_STRING = "string";

    public static final String TIMEPLUS_BOOLEAN_ARRAY = "array(bool)";
    public static final String TIMEPLUS_TINYINT_ARRAY = "array(tiny_int)";
    public static final String TIMEPLUS_SMALLINT_ARRAY = "array(small_int)";
    public static final String TIMEPLUS_INT_ARRAY = "array(int32)";
    public static final String TIMEPLUS_BIGINT_ARRAY = "array(int64)";
    public static final String TIMEPLUS_FLOAT_ARRAY = "array(float)";
    public static final String TIMEPLUS_DOUBLE_ARRAY = "array(double)";
    public static final String TIMEPLUS_DECIMALV3_ARRAY = "array(decimal)";
    public static final String TIMEPLUS_DECIMALV3_ARRAY_COLUMN_TYPE_TMP = "array(decimal(%s, %s))";
    public static final String TIMEPLUS_DATEV2_ARRAY = "array(date32)";
    public static final String TIMEPLUS_DATETIMEV2_ARRAY = "array(datetime64)";
    public static final String TIMEPLUS_STRING_ARRAY = "array(string)";

    // Because can not get the column length from array, So the following types of arrays cannot be
    // generated properly.
    public static final String TIMEPLUS_LARGEINT_ARRAY = "array(int64)";
    public static final String TIMEPLUS_CHAR_ARRAY = "array(string)";
    public static final String TIMEPLUS_CHAR_ARRAY_COLUMN_TYPE_TMP = "array(string(%s))";
    public static final String TIMEPLUS_VARCHAR_ARRAY = "array(string)";
    public static final String TIMEPLUS_VARCHAR_ARRAY_COLUMN_TYPE_TMP = "array(string(%s))";

    public static final String TIMEPLUS_JSON = "json";
    public static final String TIMEPLUS_JSONB = "json";

    public static final String TIMEPLUS_MAP = "map";

    public static final Long DEFAULT_PRECISION = 9L;
    public static final Long MAX_PRECISION = 38L;

    public static final Integer DEFAULT_SCALE = 0;
    public static final Integer MAX_SCALE = 10;

    public static final Integer MAX_DATETIME_SCALE = 6;

    // Min value of LARGEINT is -170141183460469231731687303715884105728, it will use 39 bytes in
    // UTF-8.
    // Add a bit to prevent overflow
    public static final long MAX_TIMEPLUS_LARGEINT_TO_VARCHAR_LENGTH = 39L;

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

    protected String getTimeplusColumnName(BasicTypeDefine typeDefine) {
        String columnType = typeDefine.getColumnType();
        return getTimeplusColumnName(columnType);
    }

    protected String getTimeplusColumnName(String columnType) {
        // columnType = columnType.toUpperCase(Locale.ROOT);
        int idx = columnType.indexOf("(");
        int idx2 = columnType.indexOf("<");
        if (idx != -1) {
            columnType = columnType.substring(0, idx);
        }
        if (idx2 != -1) {
            columnType = columnType.substring(0, idx2);
        }
        return columnType;
    }

    public void sampleTypeConverter(
            PhysicalColumn.PhysicalColumnBuilder builder,
            BasicTypeDefine typeDefine,
            String tpColumnType) {
        switch (tpColumnType) {
            case TIMEPLUS_NULL:
                builder.dataType(BasicType.VOID_TYPE);
                break;
            case TIMEPLUS_BOOLEAN:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case TIMEPLUS_TINYINT:
                if (typeDefine.getColumnType().equalsIgnoreCase("tinyint(1)")) {
                    builder.dataType(BasicType.BOOLEAN_TYPE);
                } else {
                    builder.dataType(BasicType.BYTE_TYPE);
                }
                break;
            case TIMEPLUS_SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case TIMEPLUS_INT:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case TIMEPLUS_BIGINT:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case TIMEPLUS_FLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case TIMEPLUS_DOUBLE:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case TIMEPLUS_STRING:
                if (typeDefine.getLength() != null && typeDefine.getLength() > 0) {
                    builder.columnLength(typeDefine.getLength());
                }
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case TIMEPLUS_LARGEINT:
                DecimalType decimalType;
                decimalType = new DecimalType(20, 0);
                builder.dataType(decimalType);
                builder.columnLength(20L);
                builder.scale(0);
                break;
            case TIMEPLUS_JSON:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(MAX_STRING_LENGTH);
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        "Timeplus", tpColumnType, typeDefine.getName());
        }
    }

    protected void sampleReconvertString(
            Column column, BasicTypeDefine.BasicTypeDefineBuilder builder) {
        if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
            builder.columnType(TIMEPLUS_STRING);
            builder.dataType(TIMEPLUS_STRING);
            return;
        }

        if (column.getColumnLength() < POWER_2_8) {
            if (column.getSourceType() != null
                    && column.getSourceType()
                            .toUpperCase(Locale.ROOT)
                            .startsWith(TIMEPLUS_VARCHAR)) {
                builder.columnType(
                        String.format("%s(%s)", TIMEPLUS_VARCHAR, column.getColumnLength()));
                builder.dataType(TIMEPLUS_VARCHAR);
            } else {
                builder.columnType(
                        String.format("%s(%s)", TIMEPLUS_CHAR, column.getColumnLength()));
                builder.dataType(TIMEPLUS_CHAR);
            }
            return;
        }

        if (column.getColumnLength() <= 65533) {
            builder.columnType(String.format("%s(%s)", TIMEPLUS_VARCHAR, column.getColumnLength()));
            builder.dataType(TIMEPLUS_VARCHAR);
            return;
        }

        if (column.getColumnLength() <= MAX_STRING_LENGTH) {
            builder.columnType(TIMEPLUS_STRING);
            builder.dataType(TIMEPLUS_STRING);
            return;
        }

        if (column.getColumnLength() > MAX_STRING_LENGTH) {
            log.warn(
                    String.format(
                            "The String type in Timeplus can only store up to 2GB bytes, and the current field [%s] length is [%s] bytes. If it is greater than the maximum length of the String in Timeplus, it may not be able to write data",
                            column.getName(), column.getColumnLength()));
            builder.columnType(TIMEPLUS_STRING);
            builder.dataType(TIMEPLUS_STRING);
            return;
        }

        throw CommonError.convertToConnectorTypeError(
                "Timeplus", column.getDataType().getSqlType().name(), column.getName());
    }

    protected BasicTypeDefine sampleReconvert(
            Column column, BasicTypeDefine.BasicTypeDefineBuilder builder) {

        switch (column.getDataType().getSqlType()) {
            case NULL:
                builder.columnType(TIMEPLUS_NULL);
                builder.dataType(TIMEPLUS_NULL);
                break;
            case BYTES:
                builder.columnType(TIMEPLUS_STRING);
                builder.dataType(TIMEPLUS_STRING);
                break;
            case BOOLEAN:
                builder.columnType(TIMEPLUS_BOOLEAN);
                builder.dataType(TIMEPLUS_BOOLEAN);
                builder.length(1L);
                break;
            case TINYINT:
                builder.columnType(TIMEPLUS_TINYINT);
                builder.dataType(TIMEPLUS_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(TIMEPLUS_SMALLINT);
                builder.dataType(TIMEPLUS_SMALLINT);
                break;
            case INT:
                builder.columnType(TIMEPLUS_INT);
                builder.dataType(TIMEPLUS_INT);
                break;
            case BIGINT:
                builder.columnType(TIMEPLUS_BIGINT);
                builder.dataType(TIMEPLUS_BIGINT);
                break;
            case FLOAT:
                builder.columnType(TIMEPLUS_FLOAT);
                builder.dataType(TIMEPLUS_FLOAT);
                break;
            case DOUBLE:
                builder.columnType(TIMEPLUS_DOUBLE);
                builder.dataType(TIMEPLUS_DOUBLE);
                break;
            case DECIMAL:
                if (column.getSourceType() != null
                        && column.getSourceType().equalsIgnoreCase(TIMEPLUS_LARGEINT)) {
                    builder.dataType(TIMEPLUS_LARGEINT);
                    builder.columnType(TIMEPLUS_LARGEINT);
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
                    builder.dataType(TIMEPLUS_VARCHAR);
                    builder.columnType(String.format("%s(%s)", TIMEPLUS_VARCHAR, 200));
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

                builder.columnType(
                        String.format("%s(%s,%s)", TIMEPLUS_DECIMALV3, precision, scale));
                builder.dataType(TIMEPLUS_DECIMALV3);
                builder.precision((long) precision);
                builder.scale(scale);
                break;
            case TIME:
                builder.length(8L);
                builder.columnType(String.format("%s(%s)", TIMEPLUS_VARCHAR, 8));
                builder.dataType(TIMEPLUS_VARCHAR);
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
                builder.columnType(TIMEPLUS_JSON);
                builder.dataType(TIMEPLUS_JSON);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        "Timeplus", column.getDataType().getSqlType().name(), column.getName());
        }
        return builder.build();
    }

    private void reconvertBuildArrayInternal(
            SeaTunnelDataType elementType,
            BasicTypeDefine.BasicTypeDefineBuilder builder,
            String columnName) {
        switch (elementType.getSqlType()) {
            case BOOLEAN:
                builder.columnType(TIMEPLUS_BOOLEAN_ARRAY);
                builder.dataType(TIMEPLUS_BOOLEAN_ARRAY);
                break;
            case TINYINT:
                builder.columnType(TIMEPLUS_TINYINT_ARRAY);
                builder.dataType(TIMEPLUS_TINYINT_ARRAY);
                break;
            case SMALLINT:
                builder.columnType(TIMEPLUS_SMALLINT_ARRAY);
                builder.dataType(TIMEPLUS_SMALLINT_ARRAY);
                break;
            case INT:
                builder.columnType(TIMEPLUS_INT_ARRAY);
                builder.dataType(TIMEPLUS_INT_ARRAY);
                break;
            case BIGINT:
                builder.columnType(TIMEPLUS_BIGINT_ARRAY);
                builder.dataType(TIMEPLUS_BIGINT_ARRAY);
                break;
            case FLOAT:
                builder.columnType(TIMEPLUS_FLOAT_ARRAY);
                builder.dataType(TIMEPLUS_FLOAT_ARRAY);
                break;
            case DOUBLE:
                builder.columnType(TIMEPLUS_DOUBLE_ARRAY);
                builder.dataType(TIMEPLUS_DOUBLE_ARRAY);
                break;
            case DECIMAL:
                int[] precisionAndScale = getPrecisionAndScale(elementType.toString());
                builder.columnType(
                        String.format(
                                TIMEPLUS_DECIMALV3_ARRAY_COLUMN_TYPE_TMP,
                                precisionAndScale[0],
                                precisionAndScale[1]));
                builder.dataType(TIMEPLUS_DECIMALV3_ARRAY);
                break;
            case STRING:
            case TIME:
                builder.columnType(TIMEPLUS_STRING_ARRAY);
                builder.dataType(TIMEPLUS_STRING_ARRAY);
                break;
            case DATE:
                builder.columnType(TIMEPLUS_DATEV2_ARRAY);
                builder.dataType(TIMEPLUS_DATEV2_ARRAY);
                break;
            case TIMESTAMP:
                builder.columnType(TIMEPLUS_DATETIMEV2_ARRAY);
                builder.dataType(TIMEPLUS_DATETIMEV2_ARRAY);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        "Timeplus", elementType.getSqlType().name(), columnName);
        }
    }

    protected static int[] getPrecisionAndScale(String decimalTypeDefinition) {
        // Remove the "DECIMALV3" part and the parentheses
        decimalTypeDefinition = decimalTypeDefinition.toUpperCase(Locale.ROOT);
        String numericPart = decimalTypeDefinition.replace("decimal(", "").replace(")", "");
        numericPart = numericPart.replace("decimal(", "").replace(")", "");

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
