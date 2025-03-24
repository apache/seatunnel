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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.datatype;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.common.source.TypeDefineUtils;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.AbstractCharTypeInfo;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.PLUGIN_NAME;

/** Refer https://help.aliyun.com/zh/maxcompute/user-guide/maxcompute-v2-0-data-type-edition */
@Slf4j
@AutoService(TypeConverter.class)
public class MaxComputeTypeConverter implements TypeConverter<BasicTypeDefine<TypeInfo>> {

    // ============================data types=====================
    static final String BOOLEAN = "BOOLEAN";

    // -------------------------number----------------------------
    static final String TINYINT = "TINYINT";
    static final String SMALLINT = "SMALLINT";
    static final String INT = "INT";
    static final String BIGINT = "BIGINT";
    static final String DECIMAL = "DECIMAL";
    static final String FLOAT = "FLOAT";
    static final String DOUBLE = "DOUBLE";

    // -------------------------string----------------------------
    public static final String CHAR = "CHAR";
    public static final String VARCHAR = "VARCHAR";
    public static final String STRING = "STRING";

    // -------------------------complex----------------------------
    public static final String JSON = "JSON";
    public static final String ARRAY = "ARRAY";
    public static final String MAP = "MAP";
    public static final String STRUCT = "STRUCT";

    // ------------------------------time-------------------------
    public static final String DATE = "DATE";
    public static final String DATETIME = "DATETIME";
    public static final String TIMESTAMP = "TIMESTAMP";
    public static final String TIMESTAMP_NTZ = "TIMESTAMP_NTZ";

    // ------------------------------blob-------------------------
    static final String BINARY = "BINARY";

    // ------------------------------other-------------------------
    static final String INTERVAL = "INTERVAL";

    public static final int DEFAULT_PRECISION = 38;
    public static final int MAX_PRECISION = 38;
    public static final int DEFAULT_SCALE = 18;
    public static final int MAX_SCALE = 18;
    public static final int MAX_TIMESTAMP_SCALE = 9;

    // 8MB
    public static final long MAX_VARBINARY_LENGTH = (long) Math.pow(2, 23);

    public static final MaxComputeTypeConverter INSTANCE = new MaxComputeTypeConverter();

    public MaxComputeTypeConverter() {}

    @Override
    public String identifier() {
        return PLUGIN_NAME;
    }

    @Override
    public Column convert(BasicTypeDefine<TypeInfo> typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .sourceType(typeDefine.getColumnType())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());
        TypeInfo nativeType = typeDefine.getNativeType();
        if (nativeType instanceof ArrayTypeInfo) {
            typeDefine.setColumnType(
                    ((ArrayTypeInfo) nativeType).getElementTypeInfo().getTypeName());
            typeDefine.setDataType(
                    ((ArrayTypeInfo) nativeType).getElementTypeInfo().getOdpsType().name());
            typeDefine.setNativeType(((ArrayTypeInfo) nativeType).getElementTypeInfo());
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
                            PLUGIN_NAME,
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
                    "ARRAY<" + arrayColumn.getSourceType() + ">",
                    arrayColumn.getOptions());
        }
        if (nativeType instanceof StructTypeInfo) {
            List<String> names = ((StructTypeInfo) nativeType).getFieldNames();
            List<SeaTunnelDataType<?>> types = new ArrayList<>();
            for (TypeInfo typeInfo : ((StructTypeInfo) nativeType).getFieldTypeInfos()) {
                BasicTypeDefine<TypeInfo> fieldDefine = new BasicTypeDefine<>();
                fieldDefine.setName(names.get(types.size()));
                fieldDefine.setColumnType(typeInfo.getTypeName());
                fieldDefine.setDataType(typeInfo.getOdpsType().name());
                fieldDefine.setNativeType(typeInfo);
                types.add(convert(fieldDefine).getDataType());
            }
            SeaTunnelRowType rowType =
                    new SeaTunnelRowType(
                            names.toArray(new String[0]), types.toArray(new SeaTunnelDataType[0]));
            return new PhysicalColumn(
                    typeDefine.getName(),
                    rowType,
                    typeDefine.getLength(),
                    typeDefine.getScale(),
                    typeDefine.isNullable(),
                    typeDefine.getDefaultValue(),
                    typeDefine.getComment(),
                    typeDefine.getNativeType().getTypeName(),
                    new HashMap<>());
        }

        if (nativeType instanceof MapTypeInfo) {
            BasicTypeDefine<TypeInfo> keyDefine = new BasicTypeDefine<>();
            keyDefine.setName("key");
            keyDefine.setColumnType(((MapTypeInfo) nativeType).getKeyTypeInfo().getTypeName());
            keyDefine.setDataType(((MapTypeInfo) nativeType).getKeyTypeInfo().getOdpsType().name());
            keyDefine.setNativeType(((MapTypeInfo) nativeType).getKeyTypeInfo());
            Column keyColumn = convert(keyDefine);
            BasicTypeDefine<TypeInfo> valueDefine = new BasicTypeDefine<>();
            valueDefine.setName("value");
            valueDefine.setColumnType(((MapTypeInfo) nativeType).getValueTypeInfo().getTypeName());
            valueDefine.setDataType(
                    ((MapTypeInfo) nativeType).getValueTypeInfo().getOdpsType().name());
            valueDefine.setNativeType(((MapTypeInfo) nativeType).getValueTypeInfo());
            Column valueColumn = convert(valueDefine);
            MapType mapType = new MapType(keyColumn.getDataType(), valueColumn.getDataType());
            return new PhysicalColumn(
                    typeDefine.getName(),
                    mapType,
                    typeDefine.getLength(),
                    typeDefine.getScale(),
                    typeDefine.isNullable(),
                    typeDefine.getDefaultValue(),
                    typeDefine.getComment(),
                    typeDefine.getNativeType().getTypeName(),
                    new HashMap<>());
        }

        if (typeDefine.getNativeType() instanceof DecimalTypeInfo) {
            DecimalType decimalType;
            if (((DecimalTypeInfo) typeDefine.getNativeType()).getPrecision() > DEFAULT_PRECISION) {
                log.warn("{} will probably cause value overflow.", DECIMAL);
                decimalType = new DecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
            } else {
                decimalType =
                        new DecimalType(
                                ((DecimalTypeInfo) typeDefine.getNativeType()).getPrecision(),
                                ((DecimalTypeInfo) typeDefine.getNativeType()).getScale());
            }
            builder.dataType(decimalType);
            builder.columnLength((long) decimalType.getPrecision());
            builder.scale(decimalType.getScale());
        } else if (typeDefine.getNativeType() instanceof AbstractCharTypeInfo) {
            // CHAR(n) or VARCHAR(n)
            builder.columnLength(
                    TypeDefineUtils.charTo4ByteLength(
                            (long)
                                    ((AbstractCharTypeInfo) typeDefine.getNativeType())
                                            .getLength()));
            builder.dataType(BasicType.STRING_TYPE);
        } else {
            String dataType = typeDefine.getDataType().toUpperCase();
            switch (dataType) {
                case BOOLEAN:
                    builder.dataType(BasicType.BOOLEAN_TYPE);
                    break;
                case TINYINT:
                    builder.dataType(BasicType.BYTE_TYPE);
                    break;
                case SMALLINT:
                    builder.dataType(BasicType.SHORT_TYPE);
                    break;
                case INT:
                    builder.dataType(BasicType.INT_TYPE);
                    break;
                case BIGINT:
                    builder.dataType(BasicType.LONG_TYPE);
                    break;
                case FLOAT:
                    builder.dataType(BasicType.FLOAT_TYPE);
                    break;
                case DOUBLE:
                    builder.dataType(BasicType.DOUBLE_TYPE);
                    break;
                case STRING:
                    if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                        builder.columnLength(MAX_VARBINARY_LENGTH);
                    } else {
                        builder.columnLength(typeDefine.getLength());
                    }
                    builder.dataType(BasicType.STRING_TYPE);
                    break;
                case JSON:
                    builder.dataType(BasicType.STRING_TYPE);
                    break;
                case BINARY:
                    if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                        builder.columnLength(MAX_VARBINARY_LENGTH);
                    } else {
                        builder.columnLength(typeDefine.getLength());
                    }
                    builder.dataType(PrimitiveByteArrayType.INSTANCE);
                    break;
                case DATE:
                    builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                    break;
                case DATETIME:
                case TIMESTAMP:
                case TIMESTAMP_NTZ:
                    builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                    builder.scale(typeDefine.getScale());
                    break;
                case INTERVAL:
                default:
                    throw CommonError.convertToSeaTunnelTypeError(
                            PLUGIN_NAME, dataType, typeDefine.getName());
            }
        }
        return builder.build();
    }

    @Override
    public BasicTypeDefine<TypeInfo> reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder<TypeInfo> builder =
                BasicTypeDefine.<TypeInfo>builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());

        switch (column.getDataType().getSqlType()) {
            case NULL:
                builder.nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.STRING));
                builder.columnType(STRING);
                builder.dataType(STRING);
                break;
            case BOOLEAN:
                builder.nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BOOLEAN));
                builder.columnType(BOOLEAN);
                builder.dataType(BOOLEAN);
                builder.length(1L);
                break;
            case TINYINT:
                builder.nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.TINYINT));
                builder.columnType(TINYINT);
                builder.dataType(TINYINT);
                break;
            case SMALLINT:
                builder.nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.SMALLINT));
                builder.columnType(SMALLINT);
                builder.dataType(SMALLINT);
                break;
            case INT:
                builder.nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.INT));
                builder.columnType(INT);
                builder.dataType(INT);
                break;
            case BIGINT:
                builder.nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BIGINT));
                builder.columnType(BIGINT);
                builder.dataType(BIGINT);
                break;
            case FLOAT:
                builder.nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.FLOAT));
                builder.columnType(FLOAT);
                builder.dataType(FLOAT);
                break;
            case DOUBLE:
                builder.nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DOUBLE));
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

                String decimalTypeStr = String.format("%s(%s,%s)", DECIMAL, precision, scale);
                builder.nativeType(TypeInfoFactory.getDecimalTypeInfo((int) precision, scale));
                builder.columnType(decimalTypeStr);
                builder.dataType(DECIMAL);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case BYTES:
                builder.nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BINARY));
                builder.columnType(BINARY);
                builder.dataType(BINARY);
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.length(MAX_VARBINARY_LENGTH);
                } else {
                    builder.length(column.getColumnLength());
                }
                break;
            case STRING:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.STRING));
                    builder.columnType(STRING);
                    builder.dataType(STRING);
                } else if (column.getColumnLength() <= 255) {
                    builder.nativeType(
                            TypeInfoFactory.getCharTypeInfo(column.getColumnLength().intValue()));
                    builder.columnType(String.format("%s(%s)", CHAR, column.getColumnLength()));
                    builder.dataType(CHAR);
                    builder.length(column.getColumnLength());
                } else if (column.getColumnLength() <= 65535) {
                    builder.nativeType(
                            TypeInfoFactory.getVarcharTypeInfo(
                                    column.getColumnLength().intValue()));
                    builder.columnType(String.format("%s(%s)", VARCHAR, column.getColumnLength()));
                    builder.dataType(VARCHAR);
                    builder.length(column.getColumnLength());
                } else {
                    builder.nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.STRING));
                    builder.columnType(STRING);
                    builder.dataType(STRING);
                    builder.length(column.getColumnLength());
                }
                break;
            case DATE:
                builder.nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DATE));
                builder.columnType(DATE);
                builder.dataType(DATE);
                break;
            case TIMESTAMP:
                if (column.getScale() == null || column.getScale() <= 3) {
                    builder.nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DATETIME));
                    builder.dataType(DATETIME);
                    builder.columnType(DATETIME);
                } else {
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
                    builder.nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.TIMESTAMP));
                    builder.dataType(TIMESTAMP);
                    builder.columnType(TIMESTAMP);
                    builder.scale(timestampScale);
                }
                break;
            case MAP:
                MapType mapType = (MapType) column.getDataType();
                SeaTunnelDataType<?> keyType = mapType.getKeyType();
                SeaTunnelDataType<?> valueType = mapType.getValueType();
                BasicTypeDefine<TypeInfo> keyDefine =
                        reconvert(
                                new PhysicalColumn(
                                        "key", keyType, null, null, true, null, null, null, null));
                BasicTypeDefine<TypeInfo> valueDefine =
                        reconvert(
                                new PhysicalColumn(
                                        "value", valueType, null, null, true, null, null, null,
                                        null));
                builder.nativeType(
                        TypeInfoFactory.getMapTypeInfo(
                                keyDefine.getNativeType(), valueDefine.getNativeType()));
                builder.columnType(
                        String.format(
                                "MAP<%s,%s>",
                                keyDefine.getColumnType(), valueDefine.getColumnType()));
                builder.dataType(MAP);
                break;
            case ARRAY:
                ArrayType<?, ?> arrayType = (ArrayType<?, ?>) column.getDataType();
                SeaTunnelDataType<?> elementType = arrayType.getElementType();
                BasicTypeDefine<TypeInfo> elementDefine =
                        reconvert(
                                new PhysicalColumn(
                                        "element",
                                        elementType,
                                        null,
                                        null,
                                        true,
                                        null,
                                        null,
                                        null,
                                        null));

                builder.nativeType(TypeInfoFactory.getArrayTypeInfo(elementDefine.getNativeType()));
                builder.columnType(String.format("ARRAY<%s>", elementDefine.getColumnType()));
                builder.dataType(ARRAY);
                break;
            case TIME:
            default:
                throw CommonError.convertToConnectorTypeError(
                        PLUGIN_NAME, column.getDataType().getSqlType().name(), column.getName());
        }

        return builder.build();
    }
}
