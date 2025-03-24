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
package org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonError;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_ARRAY_BIGINT_INTER;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_ARRAY_BOOLEAN_INTER;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_ARRAY_DATETIMEV2_INTER;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_ARRAY_DATETIME_INTER;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_ARRAY_DATEV2_INTER;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_ARRAY_DATE_INTER;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_ARRAY_DECIMAL_PRE;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_ARRAY_INT_INTER;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_ARRAY_SMALLINT_INTER;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_ARRAY_TINYINT_INTER;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_BIGINT;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_BIGINT_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_BOOLEAN;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_BOOLEAN_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_BOOLEAN_INDENTFIER;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_CHAR;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_DATE;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_DATETIME;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_DATETIMEV2_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_DATEV2_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_DECIMAL;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_DECIMALV3;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_DECIMALV3_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_DECIMALV3_ARRAY_COLUMN_TYPE_TMP;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_DOUBLE;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_DOUBLE_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_FLOAT;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_FLOAT_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_INT;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_INT_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_JSON;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_LARGEINT;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_MAP;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_MAP_COLUMN_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_NULL;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_SMALLINT;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_SMALLINT_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_STRING;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_STRING_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_TINYINT;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_TINYINT_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_VARCHAR;

/** Starrocks type converter for catalog. */
@Slf4j
@AutoService(TypeConverter.class)
public class StarRocksTypeConverter implements TypeConverter<BasicTypeDefine<StarRocksType>> {

    public static final long MAX_STRING_LENGTH = 2147483643;
    public static final Long MAX_PRECISION = 38L;
    public static final Integer MAX_SCALE = 10;
    public static final Integer MAX_DATETIME_SCALE = 6;
    public static final long POWER_2_8 = (long) Math.pow(2, 8);

    public static final StarRocksTypeConverter INSTANCE = new StarRocksTypeConverter();

    @Override
    public String identifier() {
        return "StarRocks";
    }

    @Override
    public Column convert(BasicTypeDefine<StarRocksType> typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .sourceType(typeDefine.getColumnType())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());
        String type = getOriginalType(typeDefine);
        switch (type) {
            case SR_NULL:
                builder.dataType(BasicType.VOID_TYPE);
                break;
            case SR_BOOLEAN:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case SR_TINYINT:
                if (SR_BOOLEAN_INDENTFIER.equalsIgnoreCase(typeDefine.getColumnType())) {
                    builder.dataType(BasicType.BOOLEAN_TYPE);
                } else {
                    builder.dataType(BasicType.BYTE_TYPE);
                }
                break;
            case SR_SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case SR_INT:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case SR_BIGINT:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case SR_LARGEINT:
                DecimalType decimalType;
                decimalType = new DecimalType(20, 0);
                builder.dataType(decimalType);
                builder.columnLength(20L);
                builder.scale(0);
                break;
            case SR_FLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case SR_DOUBLE:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case SR_DECIMAL:
            case SR_DECIMALV3:
                setDecimalType(builder, typeDefine);
                break;
            case SR_CHAR:
            case SR_VARCHAR:
                if (typeDefine.getLength() != null && typeDefine.getLength() > 0) {
                    builder.columnLength(typeDefine.getLength());
                }
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case SR_STRING:
            case SR_JSON:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(MAX_STRING_LENGTH);
                break;
            case SR_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case SR_DATETIME:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale() == null ? 0 : typeDefine.getScale());
                break;
            case SR_ARRAY:
                convertArray(typeDefine.getColumnType(), builder, typeDefine.getName());
                break;
            case SR_MAP:
                convertMap(typeDefine.getColumnType(), builder, typeDefine.getName());
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        identifier(), typeDefine.getColumnType(), typeDefine.getName());
        }
        return builder.build();
    }

    @Override
    public BasicTypeDefine<StarRocksType> reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder<StarRocksType> builder =
                BasicTypeDefine.<StarRocksType>builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());
        switch (column.getDataType().getSqlType()) {
            case NULL:
                builder.columnType(SR_NULL);
                builder.dataType(SR_NULL);
                break;
            case BYTES:
                builder.columnType(SR_STRING);
                builder.dataType(SR_STRING);
                break;
            case BOOLEAN:
                builder.columnType(SR_BOOLEAN);
                builder.dataType(SR_BOOLEAN);
                builder.length(1L);
                break;
            case TINYINT:
                builder.columnType(SR_TINYINT);
                builder.dataType(SR_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(SR_SMALLINT);
                builder.dataType(SR_SMALLINT);
                break;
            case INT:
                builder.columnType(SR_INT);
                builder.dataType(SR_INT);
                break;
            case BIGINT:
                builder.columnType(SR_BIGINT);
                builder.dataType(SR_BIGINT);
                break;
            case FLOAT:
                builder.columnType(SR_FLOAT);
                builder.dataType(SR_FLOAT);
                break;
            case DOUBLE:
                builder.columnType(SR_DOUBLE);
                builder.dataType(SR_DOUBLE);
                break;
            case DECIMAL:
                // DORIS LARGEINT
                if (column.getSourceType() != null
                        && column.getSourceType().equalsIgnoreCase(SR_LARGEINT)) {
                    builder.dataType(SR_LARGEINT);
                    builder.columnType(SR_LARGEINT);
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
                    builder.dataType(SR_VARCHAR);
                    builder.columnType(String.format("%s(%s)", SR_VARCHAR, 200));
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
                builder.columnType(String.format("%s(%s,%s)", SR_DECIMALV3, precision, scale));
                builder.dataType(SR_DECIMALV3);
                builder.precision((long) precision);
                builder.scale(scale);
                break;
            case TIME:
                builder.length(8L);
                builder.columnType(String.format("%s(%s)", SR_VARCHAR, 8));
                builder.dataType(SR_VARCHAR);
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
                builder.columnType(SR_JSON);
                builder.dataType(SR_JSON);
                break;
            case STRING:
                reconvertString(column, builder);
                break;
            case DATE:
                builder.columnType(SR_DATE);
                builder.dataType(SR_DATE);
                break;
            case TIMESTAMP:
                builder.columnType(SR_DATETIME);
                builder.dataType(SR_DATETIME);
                break;
            case MAP:
                reconvertMap(column, builder);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        identifier(), column.getDataType().getSqlType().name(), column.getName());
        }

        return builder.build();
    }

    private void setDecimalType(
            PhysicalColumn.PhysicalColumnBuilder builder,
            BasicTypeDefine<StarRocksType> typeDefine) {
        Long p = 10L;
        int scale = 0;
        if (typeDefine.getPrecision() != null && typeDefine.getPrecision() > 0) {
            p = typeDefine.getPrecision();
        }

        if (typeDefine.getScale() != null && typeDefine.getScale() > 0) {
            scale = typeDefine.getScale();
        }
        DecimalType decimalType;
        decimalType = new DecimalType(p.intValue(), scale);
        builder.dataType(decimalType);
        builder.columnLength(p);
        builder.scale(scale);
    }

    private void convertArray(
            String columnType, PhysicalColumn.PhysicalColumnBuilder builder, String name) {
        String columnInterType = extractArrayType(columnType);
        if (columnInterType.equalsIgnoreCase(SR_ARRAY_BOOLEAN_INTER)) {
            builder.dataType(ArrayType.BOOLEAN_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(SR_ARRAY_TINYINT_INTER)) {
            builder.dataType(ArrayType.BYTE_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(SR_ARRAY_SMALLINT_INTER)) {
            builder.dataType(ArrayType.SHORT_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(SR_ARRAY_INT_INTER)) {
            builder.dataType(ArrayType.INT_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(SR_ARRAY_BIGINT_INTER)) {
            builder.dataType(ArrayType.LONG_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(SR_FLOAT)) {
            builder.dataType(ArrayType.FLOAT_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(SR_DOUBLE)) {
            builder.dataType(ArrayType.DOUBLE_ARRAY_TYPE);
        } else if (columnInterType.toUpperCase(Locale.ROOT).startsWith("CHAR")
                || columnInterType.toUpperCase(Locale.ROOT).startsWith("VARCHAR")
                || columnInterType.equalsIgnoreCase(SR_STRING)) {
            builder.dataType(ArrayType.STRING_ARRAY_TYPE);
        } else if (columnInterType.toUpperCase(Locale.ROOT).startsWith(SR_ARRAY_DECIMAL_PRE)) {
            int[] precisionAndScale = getPrecisionAndScale(columnInterType);
            DecimalArrayType decimalArray =
                    new DecimalArrayType(
                            new DecimalType(precisionAndScale[0], precisionAndScale[1]));
            builder.dataType(decimalArray);
        } else if (columnInterType.equalsIgnoreCase(SR_ARRAY_DATE_INTER)
                || columnInterType.equalsIgnoreCase(SR_ARRAY_DATEV2_INTER)) {
            builder.dataType(ArrayType.LOCAL_DATE_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(SR_ARRAY_DATETIME_INTER)
                || columnInterType.equalsIgnoreCase(SR_ARRAY_DATETIMEV2_INTER)) {
            builder.dataType(ArrayType.LOCAL_DATE_TIME_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(SR_LARGEINT)) {
            DecimalArrayType decimalArray = new DecimalArrayType(new DecimalType(20, 0));
            builder.dataType(decimalArray);
        } else {
            throw CommonError.convertToSeaTunnelTypeError(identifier(), columnType, name);
        }
    }

    private static String extractArrayType(String input) {
        Pattern pattern = Pattern.compile("<(.*?)>");
        Matcher matcher = pattern.matcher(input);

        return matcher.find() ? matcher.group(1) : "";
    }

    private void convertMap(
            String columnType, PhysicalColumn.PhysicalColumnBuilder builder, String name) {
        String[] keyValueType =
                Optional.ofNullable(extractMapKeyValueType(columnType))
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Invalid map type: " + columnType));
        MapType mapType =
                new MapType(
                        turnColumnTypeToSeaTunnelType(keyValueType[0], name + ".key"),
                        turnColumnTypeToSeaTunnelType(keyValueType[1], name + ".value"));
        builder.dataType(mapType);
    }

    private static String[] extractMapKeyValueType(String input) {
        String[] result = new String[2];
        input = input.replaceAll("map<", "").replaceAll("MAP<", "").replaceAll(">", "");
        String[] split = input.split(",");
        if (split.length == 4) {
            // decimal(10,2),decimal(10,2)
            result[0] = split[0] + "," + split[1];
            result[1] = split[2] + "," + split[3];
        } else if (split.length == 3) {
            // decimal(10,2), date
            // decimal(10, 2), varchar(20)
            if (split[0].contains("(") && split[1].contains(")")) {
                result[0] = split[0] + "," + split[1];
                result[1] = split[2];
            } else if (split[1].contains("(") && split[2].contains(")")) {
                // date, decimal(10, 2)
                // varchar(20), decimal(10, 2)
                result[0] = split[0];
                result[1] = split[1] + "," + split[2];
            } else {
                return null;
            }
        } else if (split.length == 2) {
            result[0] = split[0];
            result[1] = split[1];
        } else {
            return null;
        }
        return result;
    }

    private SeaTunnelDataType turnColumnTypeToSeaTunnelType(String columnType, String columnName) {
        BasicTypeDefine<StarRocksType> keyBasicTypeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .columnType(columnType)
                        .name(columnName)
                        .build();
        if (columnType.toUpperCase(Locale.ROOT).startsWith(SR_ARRAY_DECIMAL_PRE)) {
            int[] precisionAndScale = getPrecisionAndScale(columnType);
            keyBasicTypeDefine.setPrecision((long) precisionAndScale[0]);
            keyBasicTypeDefine.setScale(precisionAndScale[1]);
        }
        Column column = convert(keyBasicTypeDefine);
        return column.getDataType();
    }

    private String getOriginalType(BasicTypeDefine<StarRocksType> typeDefine) {
        String columnType = typeDefine.getColumnType().toUpperCase(Locale.ROOT);
        if (StringUtils.isBlank(columnType)) {
            throw new IllegalArgumentException("Column type is empty.");
        }

        if (columnType.contains("<") && columnType.contains(">")) {
            return columnType.substring(0, columnType.indexOf("<"));
        }

        if (columnType.contains("(") && columnType.contains(")")) {
            return columnType.substring(0, columnType.indexOf("("));
        }

        return columnType;
    }

    private static int[] getPrecisionAndScale(String decimalTypeDefinition) {
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

    private void reconvertBuildArrayInternal(
            SeaTunnelDataType elementType,
            BasicTypeDefine.BasicTypeDefineBuilder<StarRocksType> builder,
            String columnName) {
        switch (elementType.getSqlType()) {
            case BOOLEAN:
                builder.columnType(SR_BOOLEAN_ARRAY);
                builder.dataType(SR_BOOLEAN_ARRAY);
                break;
            case TINYINT:
                builder.columnType(SR_TINYINT_ARRAY);
                builder.dataType(SR_TINYINT_ARRAY);
                break;
            case SMALLINT:
                builder.columnType(SR_SMALLINT_ARRAY);
                builder.dataType(SR_SMALLINT_ARRAY);
                break;
            case INT:
                builder.columnType(SR_INT_ARRAY);
                builder.dataType(SR_INT_ARRAY);
                break;
            case BIGINT:
                builder.columnType(SR_BIGINT_ARRAY);
                builder.dataType(SR_BIGINT_ARRAY);
                break;
            case FLOAT:
                builder.columnType(SR_FLOAT_ARRAY);
                builder.dataType(SR_FLOAT_ARRAY);
                break;
            case DOUBLE:
                builder.columnType(SR_DOUBLE_ARRAY);
                builder.dataType(SR_DOUBLE_ARRAY);
                break;
            case DECIMAL:
                int[] precisionAndScale = getPrecisionAndScale(elementType.toString());
                builder.columnType(
                        String.format(
                                SR_DECIMALV3_ARRAY_COLUMN_TYPE_TMP,
                                precisionAndScale[0],
                                precisionAndScale[1]));
                builder.dataType(SR_DECIMALV3_ARRAY);
                break;
            case STRING:
            case TIME:
                builder.columnType(SR_STRING_ARRAY);
                builder.dataType(SR_STRING_ARRAY);
                break;
            case DATE:
                builder.columnType(SR_DATEV2_ARRAY);
                builder.dataType(SR_DATEV2_ARRAY);
                break;
            case TIMESTAMP:
                builder.columnType(SR_DATETIMEV2_ARRAY);
                builder.dataType(SR_DATETIMEV2_ARRAY);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        identifier(), elementType.getSqlType().name(), columnName);
        }
    }

    private void reconvertString(
            Column column, BasicTypeDefine.BasicTypeDefineBuilder<StarRocksType> builder) {
        // source is doris too.
        if (column.getSourceType() != null && column.getSourceType().equalsIgnoreCase(SR_JSON)) {
            // Compatible with Doris 1.x and Doris 2.x versions
            builder.columnType(SR_JSON);
            builder.dataType(SR_JSON);
            return;
        }

        sampleReconvertString(column, builder);
    }

    protected void sampleReconvertString(
            Column column, BasicTypeDefine.BasicTypeDefineBuilder<StarRocksType> builder) {
        if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
            builder.columnType(SR_STRING);
            builder.dataType(SR_STRING);
            return;
        }

        if (column.getColumnLength() < POWER_2_8) {
            if (column.getSourceType() != null
                    && column.getSourceType().toUpperCase(Locale.ROOT).startsWith(SR_VARCHAR)) {
                builder.columnType(String.format("%s(%s)", SR_VARCHAR, column.getColumnLength()));
                builder.dataType(SR_VARCHAR);
            } else {
                builder.columnType(String.format("%s(%s)", SR_CHAR, column.getColumnLength()));
                builder.dataType(SR_CHAR);
            }
            return;
        }

        if (column.getColumnLength() <= 65533) {
            builder.columnType(String.format("%s(%s)", SR_VARCHAR, column.getColumnLength()));
            builder.dataType(SR_VARCHAR);
            return;
        }

        if (column.getColumnLength() <= MAX_STRING_LENGTH) {
            builder.columnType(SR_STRING);
            builder.dataType(SR_STRING);
            return;
        }

        log.warn(
                String.format(
                        "The String type in StarRocks can only store up to 2GB bytes, and the current field [%s] length is [%s] bytes. If it is greater than the maximum length of the String in Doris, it may not be able to write data",
                        column.getName(), column.getColumnLength()));
        builder.columnType(SR_STRING);
        builder.dataType(SR_STRING);
    }

    private void reconvertMap(
            Column column, BasicTypeDefine.BasicTypeDefineBuilder<StarRocksType> builder) {
        MapType dataType = (MapType) column.getDataType();
        SeaTunnelDataType keyType = dataType.getKeyType();
        SeaTunnelDataType valueType = dataType.getValueType();
        Column keyColumn =
                PhysicalColumn.of(
                        column.getName() + ".key",
                        (SeaTunnelDataType<?>) keyType,
                        (Long) null,
                        true,
                        null,
                        null);
        String keyColumnType = reconvert(keyColumn).getColumnType();

        Column valueColumn =
                PhysicalColumn.of(
                        column.getName() + ".value",
                        (SeaTunnelDataType<?>) valueType,
                        (Long) null,
                        true,
                        null,
                        null);
        String valueColumnType = reconvert(valueColumn).getColumnType();

        builder.dataType(String.format(SR_MAP_COLUMN_TYPE, keyColumnType, valueColumnType));
        builder.columnType(String.format(SR_MAP_COLUMN_TYPE, keyColumnType, valueColumnType));
    }
}
