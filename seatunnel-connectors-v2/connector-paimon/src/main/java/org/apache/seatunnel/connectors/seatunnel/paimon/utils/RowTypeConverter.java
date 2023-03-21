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

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorException;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
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
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import java.util.Arrays;

/** The converter for converting {@link RowType} and {@link SeaTunnelRowType} */
public class RowTypeConverter {

    private RowTypeConverter() {}

    /**
     * Convert Paimon row type {@link RowType} to SeaTunnel row type {@link SeaTunnelRowType}
     *
     * @param rowType Paimon row type
     * @return SeaTunnel row type {@link SeaTunnelRowType}
     */
    public static SeaTunnelRowType convert(RowType rowType) {
        String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
        SeaTunnelDataType<?>[] dataTypes =
                rowType.getFields().stream()
                        .map(field -> field.type().accept(PaimonToSeaTunnelTypeVisitor.INSTANCE))
                        .toArray(SeaTunnelDataType<?>[]::new);
        return new SeaTunnelRowType(fieldNames, dataTypes);
    }

    /**
     * Convert SeaTunnel row type {@link SeaTunnelRowType} to Paimon row type {@link RowType}
     *
     * @param seaTunnelRowType SeaTunnel row type {@link SeaTunnelRowType}
     * @return Paimon row type {@link RowType}
     */
    public static RowType convert(SeaTunnelRowType seaTunnelRowType) {
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        DataType[] dataTypes =
                Arrays.stream(fieldTypes)
                        .map(SeaTunnelTypeToPaimonVisitor.INSTANCE::visit)
                        .toArray(DataType[]::new);
        DataField[] dataFields = new DataField[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            DataField dataField = new DataField(i, seaTunnelRowType.getFieldName(i), dataTypes[i]);
            dataFields[i] = dataField;
        }
        return DataTypes.ROW(dataFields);
    }

    /**
     * Mapping SeaTunnel data type {@link SeaTunnelDataType} to Paimon data type {@link DataType}
     *
     * @param dataType SeaTunnel data type {@link SeaTunnelDataType}
     * @return Paimon data type {@link DataType}
     */
    public static DataType convert(SeaTunnelDataType<?> dataType) {
        return SeaTunnelTypeToPaimonVisitor.INSTANCE.visit(dataType);
    }

    /**
     * A visitor that convert SeaTunnel data type {@link SeaTunnelDataType} to Paimon data type
     * {@link DataType}
     */
    private static class SeaTunnelTypeToPaimonVisitor {

        private static final SeaTunnelTypeToPaimonVisitor INSTANCE =
                new SeaTunnelTypeToPaimonVisitor();

        private SeaTunnelTypeToPaimonVisitor() {}

        public DataType visit(SeaTunnelDataType<?> dataType) {
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
                case TIMESTAMP:
                    return DataTypes.TIMESTAMP(6);
                case MAP:
                    SeaTunnelDataType<?> keyType =
                            ((org.apache.seatunnel.api.table.type.MapType<?, ?>) dataType)
                                    .getKeyType();
                    SeaTunnelDataType<?> valueType =
                            ((org.apache.seatunnel.api.table.type.MapType<?, ?>) dataType)
                                    .getValueType();
                    return DataTypes.MAP(visit(keyType), visit(valueType));
                case ARRAY:
                    BasicType<?> elementType =
                            ((org.apache.seatunnel.api.table.type.ArrayType<?, ?>) dataType)
                                    .getElementType();
                    return DataTypes.ARRAY(visit(elementType));
                case ROW:
                    SeaTunnelDataType<?>[] fieldTypes =
                            ((SeaTunnelRowType) dataType).getFieldTypes();
                    DataType[] dataTypes =
                            Arrays.stream(fieldTypes).map(this::visit).toArray(DataType[]::new);
                    return DataTypes.ROW(dataTypes);
                default:
                    throw new PaimonConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unsupported data type: " + dataType.getSqlType());
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
                    String errorMsg =
                            String.format(
                                    "Array type not support this genericType [%s]",
                                    seaTunnelArrayType);
                    throw new PaimonConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
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
            throw new PaimonConnectorException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE, "Unsupported data type: " + dataType);
        }
    }
}
