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

package org.apache.seatunnel.connectors.seatunnel.fts.utils;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.fts.exception.FlinkTableStoreConnectorException;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;

/** The converter for converting {@link RowType} and {@link SeaTunnelRowType} */
public class RowTypeConverter {

    private RowTypeConverter() {}

    public static SeaTunnelRowType convert(RowType rowType) {
        String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
        SeaTunnelDataType<?>[] dataTypes =
                rowType.getFields().stream()
                        .map(field -> field.getType().accept(FlinkToSeaTunnelTypeVisitor.INSTANCE))
                        .toArray(SeaTunnelDataType<?>[]::new);
        return new SeaTunnelRowType(fieldNames, dataTypes);
    }

    public static RowType convert(SeaTunnelRowType seaTunnelRowType) {
        return null;
    }

    private static class FlinkToSeaTunnelTypeVisitor
            extends LogicalTypeDefaultVisitor<SeaTunnelDataType> {

        private static final FlinkToSeaTunnelTypeVisitor INSTANCE =
                new FlinkToSeaTunnelTypeVisitor();

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
            LogicalType elementType = arrayType.getElementType();
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
                    throw new FlinkTableStoreConnectorException(
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
                            .map(field -> field.getType().accept(this))
                            .toArray(SeaTunnelDataType<?>[]::new);
            return new SeaTunnelRowType(fieldNames, fieldTypes);
        }

        @Override
        protected SeaTunnelDataType<?> defaultMethod(LogicalType logicalType) {
            throw new FlinkTableStoreConnectorException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE, "Unsupported data type: " + logicalType);
        }
    }
}
