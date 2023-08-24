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

package org.apache.seatunnel.translation.flink.utils;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.LocalTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.runtime.typeutils.BigDecimalTypeInfo;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TypeConverterUtils {

    private static final Map<Class<?>, BridgedType> BRIDGED_TYPES = new HashMap<>(32);

    static {
        // basic types
        BRIDGED_TYPES.put(
                String.class,
                BridgedType.of(BasicType.STRING_TYPE, BasicTypeInfo.STRING_TYPE_INFO));
        BRIDGED_TYPES.put(
                Boolean.class,
                BridgedType.of(BasicType.BOOLEAN_TYPE, BasicTypeInfo.BOOLEAN_TYPE_INFO));
        BRIDGED_TYPES.put(
                Byte.class, BridgedType.of(BasicType.BYTE_TYPE, BasicTypeInfo.BYTE_TYPE_INFO));
        BRIDGED_TYPES.put(
                Short.class, BridgedType.of(BasicType.SHORT_TYPE, BasicTypeInfo.SHORT_TYPE_INFO));
        BRIDGED_TYPES.put(
                Integer.class, BridgedType.of(BasicType.INT_TYPE, BasicTypeInfo.INT_TYPE_INFO));
        BRIDGED_TYPES.put(
                Long.class, BridgedType.of(BasicType.LONG_TYPE, BasicTypeInfo.LONG_TYPE_INFO));
        BRIDGED_TYPES.put(
                Float.class, BridgedType.of(BasicType.FLOAT_TYPE, BasicTypeInfo.FLOAT_TYPE_INFO));
        BRIDGED_TYPES.put(
                Double.class,
                BridgedType.of(BasicType.DOUBLE_TYPE, BasicTypeInfo.DOUBLE_TYPE_INFO));
        BRIDGED_TYPES.put(
                Void.class, BridgedType.of(BasicType.VOID_TYPE, BasicTypeInfo.VOID_TYPE_INFO));
        // TODO: there is a still an unresolved issue that the BigDecimal type will lose the
        // precision and scale
        BRIDGED_TYPES.put(
                BigDecimal.class,
                BridgedType.of(new DecimalType(38, 18), BasicTypeInfo.BIG_DEC_TYPE_INFO));

        // data time types
        BRIDGED_TYPES.put(
                LocalDate.class,
                BridgedType.of(LocalTimeType.LOCAL_DATE_TYPE, LocalTimeTypeInfo.LOCAL_DATE));
        BRIDGED_TYPES.put(
                LocalTime.class,
                BridgedType.of(LocalTimeType.LOCAL_TIME_TYPE, LocalTimeTypeInfo.LOCAL_TIME));
        BRIDGED_TYPES.put(
                LocalDateTime.class,
                BridgedType.of(
                        LocalTimeType.LOCAL_DATE_TIME_TYPE, LocalTimeTypeInfo.LOCAL_DATE_TIME));
        // basic array types
        BRIDGED_TYPES.put(
                byte[].class,
                BridgedType.of(
                        PrimitiveByteArrayType.INSTANCE,
                        PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
        BRIDGED_TYPES.put(
                String[].class,
                BridgedType.of(
                        ArrayType.STRING_ARRAY_TYPE, BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO));
        BRIDGED_TYPES.put(
                Boolean[].class,
                BridgedType.of(
                        ArrayType.BOOLEAN_ARRAY_TYPE, BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO));
        BRIDGED_TYPES.put(
                Byte[].class,
                BridgedType.of(ArrayType.BYTE_ARRAY_TYPE, BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO));
        BRIDGED_TYPES.put(
                Short[].class,
                BridgedType.of(
                        ArrayType.SHORT_ARRAY_TYPE, BasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO));
        BRIDGED_TYPES.put(
                Integer[].class,
                BridgedType.of(ArrayType.INT_ARRAY_TYPE, BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO));
        BRIDGED_TYPES.put(
                Long[].class,
                BridgedType.of(ArrayType.LONG_ARRAY_TYPE, BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO));
        BRIDGED_TYPES.put(
                Float[].class,
                BridgedType.of(
                        ArrayType.FLOAT_ARRAY_TYPE, BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO));
        BRIDGED_TYPES.put(
                Double[].class,
                BridgedType.of(
                        ArrayType.DOUBLE_ARRAY_TYPE, BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO));
    }

    private TypeConverterUtils() {
        throw new UnsupportedOperationException(
                "TypeConverterUtils is a utility class and cannot be instantiated");
    }

    public static SeaTunnelDataType<?> convert(TypeInformation<?> dataType) {
        BridgedType bridgedType = BRIDGED_TYPES.get(dataType.getTypeClass());
        if (bridgedType != null) {
            return bridgedType.getSeaTunnelType();
        }
        if (dataType instanceof BigDecimalTypeInfo) {
            BigDecimalTypeInfo decimalType = (BigDecimalTypeInfo) dataType;
            return new DecimalType(decimalType.precision(), decimalType.scale());
        }
        if (dataType instanceof MapTypeInfo) {
            MapTypeInfo<?, ?> mapTypeInfo = (MapTypeInfo<?, ?>) dataType;
            return new MapType<>(
                    convert(mapTypeInfo.getKeyTypeInfo()), convert(mapTypeInfo.getValueTypeInfo()));
        }
        if (dataType instanceof RowTypeInfo) {
            RowTypeInfo typeInformation = (RowTypeInfo) dataType;
            String[] fieldNames = typeInformation.getFieldNames();
            SeaTunnelDataType<?>[] seaTunnelDataTypes =
                    Arrays.stream(typeInformation.getFieldTypes())
                            .map(TypeConverterUtils::convert)
                            .toArray(SeaTunnelDataType[]::new);
            return new SeaTunnelRowType(fieldNames, seaTunnelDataTypes);
        }
        throw new IllegalArgumentException("Unsupported Flink's data type: " + dataType);
    }

    public static TypeInformation<?> convert(SeaTunnelDataType<?> dataType) {
        BridgedType bridgedType = BRIDGED_TYPES.get(dataType.getTypeClass());
        if (bridgedType != null) {
            return bridgedType.getFlinkType();
        }
        if (dataType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) dataType;
            return new BigDecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale());
        }
        if (dataType instanceof MapType) {
            MapType<?, ?> mapType = (MapType<?, ?>) dataType;
            return new MapTypeInfo<>(
                    convert(mapType.getKeyType()), convert(mapType.getValueType()));
        }
        if (dataType instanceof SeaTunnelRowType) {
            SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
            TypeInformation<?>[] types =
                    Arrays.stream(rowType.getFieldTypes())
                            .map(TypeConverterUtils::convert)
                            .toArray(TypeInformation[]::new);
            return new RowTypeInfo(types, rowType.getFieldNames());
        }
        throw new IllegalArgumentException("Unsupported SeaTunnel's data type: " + dataType);
    }

    public static class BridgedType {
        private final SeaTunnelDataType<?> seaTunnelType;
        private final TypeInformation<?> flinkType;

        private BridgedType(SeaTunnelDataType<?> seaTunnelType, TypeInformation<?> flinkType) {
            this.seaTunnelType = seaTunnelType;
            this.flinkType = flinkType;
        }

        public static BridgedType of(
                SeaTunnelDataType<?> seaTunnelType, TypeInformation<?> flinkType) {
            return new BridgedType(seaTunnelType, flinkType);
        }

        public TypeInformation<?> getFlinkType() {
            return flinkType;
        }

        public SeaTunnelDataType<?> getSeaTunnelType() {
            return seaTunnelType;
        }
    }
}
