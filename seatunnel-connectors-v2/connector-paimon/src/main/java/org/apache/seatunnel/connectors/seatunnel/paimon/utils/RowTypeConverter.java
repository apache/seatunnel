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

package org.apache.seatunnel.connectors.seatunnel.paimon.utils;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig;

import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Slf4j
/** The converter for converting {@link RowType} and {@link SeaTunnelRowType} */
public class RowTypeConverter {

    private static String UNKNOWN_FIELD = "UNKNOWN";

    private RowTypeConverter() {}

    /**
     * Convert Paimon row type {@link RowType} to SeaTunnel row type {@link SeaTunnelRowType}
     *
     * @param rowType Paimon row type
     * @return SeaTunnel row type {@link SeaTunnelRowType}
     */
    public static SeaTunnelRowType convert(RowType rowType, int[] projectionIndex) {
        String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
        SeaTunnelDataType<?>[] dataTypes =
                rowType.getFields().stream()
                        .map(field -> field.type().accept(PaimonToSeaTunnelTypeVisitor.INSTANCE))
                        .toArray(SeaTunnelDataType<?>[]::new);
        if (projectionIndex != null) {
            String[] projectionFieldNames =
                    Arrays.stream(projectionIndex)
                            .filter(index -> index >= 0 && index < fieldNames.length)
                            .mapToObj(index -> fieldNames[index])
                            .toArray(String[]::new);
            SeaTunnelDataType<?>[] projectionDataTypes =
                    Arrays.stream(projectionIndex)
                            .filter(index -> index >= 0 && index < fieldNames.length)
                            .mapToObj(index -> dataTypes[index])
                            .toArray(SeaTunnelDataType<?>[]::new);
            return new SeaTunnelRowType(projectionFieldNames, projectionDataTypes);
        }
        return new SeaTunnelRowType(fieldNames, dataTypes);
    }

    /**
     * Convert Paimon row type {@link DataType} to SeaTunnel row type {@link SeaTunnelDataType}
     *
     * @param typeDefine Paimon data type
     * @return SeaTunnel data type {@link SeaTunnelDataType}
     */
    public static Column convert(BasicTypeDefine<DataType> typeDefine) {

        PhysicalColumn.PhysicalColumnBuilder physicalColumnBuilder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .sourceType(typeDefine.getColumnType())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());

        DataType dataType = typeDefine.getNativeType();
        SeaTunnelDataType<?> seaTunnelDataType;
        PaimonToSeaTunnelTypeVisitor paimonToSeaTunnelTypeVisitor =
                PaimonToSeaTunnelTypeVisitor.INSTANCE;
        switch (dataType.getTypeRoot()) {
            case CHAR:
                CharType charType = (CharType) dataType;
                seaTunnelDataType = paimonToSeaTunnelTypeVisitor.visit(charType);
                physicalColumnBuilder.columnLength((long) charType.getLength());
                break;
            case VARCHAR:
                VarCharType varCharType = (VarCharType) dataType;
                seaTunnelDataType = paimonToSeaTunnelTypeVisitor.visit(varCharType);
                physicalColumnBuilder.columnLength((long) varCharType.getLength());
                break;
            case BOOLEAN:
                seaTunnelDataType = paimonToSeaTunnelTypeVisitor.visit((BooleanType) dataType);
                break;
            case BINARY:
                seaTunnelDataType = paimonToSeaTunnelTypeVisitor.visit((BinaryType) dataType);
                break;
            case VARBINARY:
                VarBinaryType varBinaryType = (VarBinaryType) dataType;
                seaTunnelDataType = paimonToSeaTunnelTypeVisitor.visit(varBinaryType);
                physicalColumnBuilder.columnLength((long) varBinaryType.getLength());
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                seaTunnelDataType = paimonToSeaTunnelTypeVisitor.visit(decimalType);
                physicalColumnBuilder.columnLength((long) decimalType.getPrecision());
                physicalColumnBuilder.scale(decimalType.getScale());
                break;
            case TINYINT:
                seaTunnelDataType = paimonToSeaTunnelTypeVisitor.visit((TinyIntType) dataType);
                break;
            case SMALLINT:
                seaTunnelDataType = paimonToSeaTunnelTypeVisitor.visit((SmallIntType) dataType);
                break;
            case INTEGER:
                seaTunnelDataType = paimonToSeaTunnelTypeVisitor.visit((IntType) dataType);
                break;
            case BIGINT:
                seaTunnelDataType = paimonToSeaTunnelTypeVisitor.visit((BigIntType) dataType);
                break;
            case FLOAT:
                seaTunnelDataType = paimonToSeaTunnelTypeVisitor.visit((FloatType) dataType);
                break;
            case DOUBLE:
                seaTunnelDataType = paimonToSeaTunnelTypeVisitor.visit((DoubleType) dataType);
                break;
            case DATE:
                seaTunnelDataType = paimonToSeaTunnelTypeVisitor.visit((DateType) dataType);
                break;
            case TIME_WITHOUT_TIME_ZONE:
                TimeType timeType = (TimeType) dataType;
                seaTunnelDataType = paimonToSeaTunnelTypeVisitor.visit(timeType);
                physicalColumnBuilder.scale(timeType.getPrecision());
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) dataType;
                seaTunnelDataType = paimonToSeaTunnelTypeVisitor.visit(timestampType);
                physicalColumnBuilder.scale(timestampType.getPrecision());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType localZonedTimestampType =
                        (LocalZonedTimestampType) dataType;
                seaTunnelDataType =
                        paimonToSeaTunnelTypeVisitor.visit((LocalZonedTimestampType) dataType);
                physicalColumnBuilder.scale(localZonedTimestampType.getPrecision());
                break;
            case ARRAY:
                seaTunnelDataType = paimonToSeaTunnelTypeVisitor.visit((ArrayType) dataType);
                if (seaTunnelDataType == null) {
                    throw CommonError.unsupportedArrayGenericType(
                            PaimonConfig.CONNECTOR_IDENTITY,
                            dataType.getTypeRoot().toString(),
                            typeDefine.getName());
                }
                break;
            case MAP:
                seaTunnelDataType = paimonToSeaTunnelTypeVisitor.visit((MapType) dataType);
                break;
            case ROW:
                seaTunnelDataType = paimonToSeaTunnelTypeVisitor.visit((RowType) dataType);
                break;
            default:
                throw CommonError.unsupportedDataType(
                        PaimonConfig.CONNECTOR_IDENTITY,
                        dataType.asSQLString(),
                        typeDefine.getName());
        }
        return physicalColumnBuilder.dataType(seaTunnelDataType).build();
    }

    /**
     * Convert SeaTunnel row type {@link SeaTunnelRowType} to Paimon row type {@link RowType}
     *
     * @param seaTunnelRowType SeaTunnel row type {@link SeaTunnelRowType}
     * @return Paimon row type {@link RowType}
     */
    public static RowType reconvert(SeaTunnelRowType seaTunnelRowType, TableSchema tableSchema) {
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        int totalFields = seaTunnelRowType.getTotalFields();
        List<DataField> fields = tableSchema.fields();
        DataField[] dataFields = new DataField[totalFields];
        for (int i = 0; i < totalFields; i++) {
            String fieldName = fieldNames[i];
            DataType dataType =
                    SeaTunnelTypeToPaimonVisitor.INSTANCE.visit(fieldName, fieldTypes[i]);
            DataTypeRoot typeRoot = dataType.getTypeRoot();
            if (typeRoot.equals(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                    || typeRoot.equals(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
                DataField dataField = SchemaUtil.getDataField(fields, fieldName);
                dataType = new TimestampType(((TimestampType) dataField.type()).getPrecision());
            }
            DataField dataField = new DataField(i, fieldName, dataType);
            dataFields[i] = dataField;
        }
        return DataTypes.ROW(dataFields);
    }

    /**
     * Mapping SeaTunnel data type of column {@link Column} to Paimon data type {@link DataType}
     *
     * @param column SeaTunnel data type {@link Column}
     * @return Paimon data type {@link DataType}
     */
    public static BasicTypeDefine<DataType> reconvert(Column column) {
        return SeaTunnelTypeToPaimonVisitor.INSTANCE.visit(column);
    }

    /**
     * Mapping SeaTunnel data type {@link SeaTunnelDataType} of fieldName to Paimon data type {@link
     * DataType}
     *
     * @param fieldName SeaTunnel field name
     * @param dataType SeaTunnel data type {@link SeaTunnelDataType}
     * @return Paimon data type {@link DataType}
     */
    public static DataType reconvert(String fieldName, SeaTunnelDataType<?> dataType) {
        return SeaTunnelTypeToPaimonVisitor.INSTANCE.visit(fieldName, dataType);
    }

    /**
     * A visitor that convert SeaTunnel data type {@link SeaTunnelDataType} to Paimon data type
     * {@link DataType}
     */
    private static class SeaTunnelTypeToPaimonVisitor {

        private static final SeaTunnelTypeToPaimonVisitor INSTANCE =
                new SeaTunnelTypeToPaimonVisitor();

        private SeaTunnelTypeToPaimonVisitor() {}

        public BasicTypeDefine<DataType> visit(Column column) {
            BasicTypeDefine.BasicTypeDefineBuilder<DataType> builder =
                    BasicTypeDefine.<DataType>builder()
                            .name(column.getName())
                            .nullable(column.isNullable())
                            .comment(column.getComment())
                            .defaultValue(column.getDefaultValue());
            SeaTunnelDataType<?> dataType = column.getDataType();
            Integer scale = column.getScale();
            switch (dataType.getSqlType()) {
                case TIMESTAMP:
                    int timestampScale =
                            Objects.isNull(scale) ? TimestampType.DEFAULT_PRECISION : scale;
                    TimestampType timestampType = DataTypes.TIMESTAMP(timestampScale);
                    builder.nativeType(timestampType);
                    builder.dataType(timestampType.getTypeRoot().name());
                    builder.columnType(timestampType.toString());
                    builder.scale(timestampScale);
                    builder.length(column.getColumnLength());
                    return builder.build();
                case TIME:
                    int timeScale = Objects.isNull(scale) ? TimeType.DEFAULT_PRECISION : scale;
                    TimeType timeType = DataTypes.TIME(timeScale);
                    builder.nativeType(timeType);
                    builder.columnType(timeType.toString());
                    builder.dataType(timeType.getTypeRoot().name());
                    builder.scale(timeScale);
                    builder.length(column.getColumnLength());
                    return builder.build();
                case DECIMAL:
                    org.apache.seatunnel.api.table.type.DecimalType seatunnelDecimalType =
                            (org.apache.seatunnel.api.table.type.DecimalType) dataType;
                    int precision = seatunnelDecimalType.getPrecision();
                    scale = seatunnelDecimalType.getScale();
                    if (precision <= 0) {
                        precision = DecimalType.DEFAULT_PRECISION;
                        scale = DecimalType.DEFAULT_SCALE;
                        log.warn(
                                "The decimal column {} type decimal({},{}) is out of range, "
                                        + "which is precision less than 0, "
                                        + "it will be converted to decimal({},{})",
                                column.getName(),
                                seatunnelDecimalType.getPrecision(),
                                seatunnelDecimalType.getScale(),
                                precision,
                                scale);
                    } else if (precision > DecimalType.MAX_PRECISION) {
                        scale = (int) Math.max(0, scale - (precision - DecimalType.MAX_PRECISION));
                        precision = DecimalType.MAX_PRECISION;
                        log.warn(
                                "The decimal column {} type decimal({},{}) is out of range, "
                                        + "which exceeds the maximum precision of {}, "
                                        + "it will be converted to decimal({},{})",
                                column.getName(),
                                seatunnelDecimalType.getPrecision(),
                                seatunnelDecimalType.getScale(),
                                DecimalType.MAX_PRECISION,
                                precision,
                                scale);
                    }
                    if (scale < 0) {
                        scale = DecimalType.DEFAULT_SCALE;
                        log.warn(
                                "The decimal column {} type decimal({},{}) is out of range, "
                                        + "which is scale less than 0, "
                                        + "it will be converted to decimal({},{})",
                                column.getName(),
                                seatunnelDecimalType.getPrecision(),
                                seatunnelDecimalType.getScale(),
                                precision,
                                scale);
                    } else if (scale > DecimalType.MAX_PRECISION) {
                        scale = DecimalType.MAX_PRECISION;
                        log.warn(
                                "The decimal column {} type decimal({},{}) is out of range, "
                                        + "which exceeds the maximum scale of {}, "
                                        + "it will be converted to decimal({},{})",
                                column.getName(),
                                seatunnelDecimalType.getPrecision(),
                                seatunnelDecimalType.getScale(),
                                DecimalType.MAX_PRECISION,
                                precision,
                                scale);
                    }

                    DecimalType paimonDecimalType = DataTypes.DECIMAL(precision, scale);
                    builder.nativeType(paimonDecimalType);
                    builder.columnType(paimonDecimalType.toString());
                    builder.dataType(paimonDecimalType.getTypeRoot().name());
                    builder.scale(scale);
                    builder.precision((long) precision);
                    builder.length(column.getColumnLength());
                    return builder.build();
                default:
                    builder.nativeType(visit(column.getName(), dataType));
                    builder.columnType(dataType.toString());
                    builder.length(column.getColumnLength());
                    builder.dataType(dataType.getSqlType().name());
                    return builder.build();
            }
        }

        public DataType visit(String fieldName, SeaTunnelDataType<?> dataType) {
            switch (dataType.getSqlType()) {
                case TINYINT:
                    return DataTypes.TINYINT();
                case SMALLINT:
                    return DataTypes.SMALLINT();
                case INT:
                    return DataTypes.INT();
                case BIGINT:
                    return DataTypes.BIGINT();
                case FLOAT:
                    return DataTypes.FLOAT();
                case DOUBLE:
                    return DataTypes.DOUBLE();
                case DECIMAL:
                    return DataTypes.DECIMAL(
                            ((org.apache.seatunnel.api.table.type.DecimalType) dataType)
                                    .getPrecision(),
                            ((org.apache.seatunnel.api.table.type.DecimalType) dataType)
                                    .getScale());
                case STRING:
                    return DataTypes.STRING();
                case BYTES:
                    return DataTypes.BYTES();
                case BOOLEAN:
                    return DataTypes.BOOLEAN();
                case DATE:
                    return DataTypes.DATE();
                case TIME:
                    return DataTypes.TIME(TimeType.MAX_PRECISION);
                case TIMESTAMP:
                    return DataTypes.TIMESTAMP(TimestampType.MAX_PRECISION);
                case MAP:
                    SeaTunnelDataType<?> keyType =
                            ((org.apache.seatunnel.api.table.type.MapType<?, ?>) dataType)
                                    .getKeyType();
                    SeaTunnelDataType<?> valueType =
                            ((org.apache.seatunnel.api.table.type.MapType<?, ?>) dataType)
                                    .getValueType();
                    return DataTypes.MAP(visit(fieldName, keyType), visit(fieldName, valueType));
                case ARRAY:
                    SeaTunnelDataType<?> elementType =
                            ((org.apache.seatunnel.api.table.type.ArrayType<?, ?>) dataType)
                                    .getElementType();
                    return DataTypes.ARRAY(visit(fieldName, elementType));
                case ROW:
                    SeaTunnelRowType row = (SeaTunnelRowType) dataType;
                    SeaTunnelDataType<?>[] fieldTypes = row.getFieldTypes();
                    String[] fieldNames = row.getFieldNames();
                    int totalFields = row.getTotalFields();
                    DataType[] dataTypes = new DataType[totalFields];
                    for (int i = 0; i < totalFields; i++) {
                        dataTypes[i] =
                                SeaTunnelTypeToPaimonVisitor.INSTANCE.visit(
                                        fieldNames[i], fieldTypes[i]);
                    }
                    return DataTypes.ROW(dataTypes);
                default:
                    throw CommonError.unsupportedDataType(
                            PaimonConfig.CONNECTOR_IDENTITY,
                            dataType.getSqlType().toString(),
                            fieldName);
            }
        }
    }

    /**
     * A visitor that convert Paimon data type {@link DataType} to SeaTunnel data type {@link
     * SeaTunnelDataType}
     */
    private static class PaimonToSeaTunnelTypeVisitor
            extends DataTypeDefaultVisitor<SeaTunnelDataType> {

        private static final PaimonToSeaTunnelTypeVisitor INSTANCE =
                new PaimonToSeaTunnelTypeVisitor();

        @Override
        public SeaTunnelDataType<?> visit(CharType charType) {
            return BasicType.STRING_TYPE;
        }

        @Override
        public SeaTunnelDataType<?> visit(VarCharType varCharType) {
            return BasicType.STRING_TYPE;
        }

        @Override
        public SeaTunnelDataType<?> visit(BooleanType booleanType) {
            return BasicType.BOOLEAN_TYPE;
        }

        @Override
        public SeaTunnelDataType<?> visit(BinaryType binaryType) {
            return PrimitiveByteArrayType.INSTANCE;
        }

        @Override
        public SeaTunnelDataType<?> visit(VarBinaryType varBinaryType) {
            return PrimitiveByteArrayType.INSTANCE;
        }

        @Override
        public SeaTunnelDataType<?> visit(DecimalType decimalType) {
            return new org.apache.seatunnel.api.table.type.DecimalType(
                    decimalType.getPrecision(), decimalType.getScale());
        }

        @Override
        public SeaTunnelDataType<?> visit(TinyIntType tinyIntType) {
            return BasicType.BYTE_TYPE;
        }

        @Override
        public SeaTunnelDataType<?> visit(SmallIntType smallIntType) {
            return BasicType.SHORT_TYPE;
        }

        @Override
        public SeaTunnelDataType<?> visit(IntType intType) {
            return BasicType.INT_TYPE;
        }

        @Override
        public SeaTunnelDataType<?> visit(BigIntType bigIntType) {
            return BasicType.LONG_TYPE;
        }

        @Override
        public SeaTunnelDataType<?> visit(FloatType floatType) {
            return BasicType.FLOAT_TYPE;
        }

        @Override
        public SeaTunnelDataType<?> visit(DoubleType doubleType) {
            return BasicType.DOUBLE_TYPE;
        }

        @Override
        public SeaTunnelDataType<?> visit(DateType dateType) {
            // TODO the data type in flink is int, so it should be converted to LocalDate
            return LocalTimeType.LOCAL_DATE_TYPE;
        }

        @Override
        public SeaTunnelDataType<?> visit(TimestampType timestampType) {
            return LocalTimeType.LOCAL_DATE_TIME_TYPE;
        }

        @Override
        public SeaTunnelDataType<?> visit(LocalZonedTimestampType localZonedTimestampType) {
            return LocalTimeType.LOCAL_DATE_TIME_TYPE;
        }

        @Override
        public SeaTunnelDataType<?> visit(ArrayType arrayType) {
            DataType elementType = arrayType.getElementType();
            SeaTunnelDataType<?> seaTunnelArrayType = elementType.accept(this);
            switch (seaTunnelArrayType.getSqlType()) {
                case STRING:
                    return org.apache.seatunnel.api.table.type.ArrayType.STRING_ARRAY_TYPE;
                case BOOLEAN:
                    return org.apache.seatunnel.api.table.type.ArrayType.BOOLEAN_ARRAY_TYPE;
                case TINYINT:
                    return org.apache.seatunnel.api.table.type.ArrayType.BYTE_ARRAY_TYPE;
                case SMALLINT:
                    return org.apache.seatunnel.api.table.type.ArrayType.SHORT_ARRAY_TYPE;
                case INT:
                    return org.apache.seatunnel.api.table.type.ArrayType.INT_ARRAY_TYPE;
                case BIGINT:
                    return org.apache.seatunnel.api.table.type.ArrayType.LONG_ARRAY_TYPE;
                case FLOAT:
                    return org.apache.seatunnel.api.table.type.ArrayType.FLOAT_ARRAY_TYPE;
                case DOUBLE:
                    return org.apache.seatunnel.api.table.type.ArrayType.DOUBLE_ARRAY_TYPE;
                default:
                    return null;
            }
        }

        @Override
        public SeaTunnelDataType<?> visit(MapType mapType) {
            SeaTunnelDataType<?> keyType = mapType.getKeyType().accept(this);
            SeaTunnelDataType<?> valueType = mapType.getValueType().accept(this);
            return new org.apache.seatunnel.api.table.type.MapType<>(keyType, valueType);
        }

        @Override
        public SeaTunnelDataType<?> visit(RowType rowType) {
            String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
            SeaTunnelDataType<?>[] fieldTypes =
                    rowType.getFields().stream()
                            .map(field -> field.type().accept(this))
                            .toArray(SeaTunnelDataType<?>[]::new);
            return new SeaTunnelRowType(fieldNames, fieldTypes);
        }

        @Override
        protected SeaTunnelDataType defaultMethod(DataType dataType) {
            throw CommonError.unsupportedDataType(
                    PaimonConfig.CONNECTOR_IDENTITY, dataType.getTypeRoot().name(), UNKNOWN_FIELD);
        }
    }
}
