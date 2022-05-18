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
import org.apache.seatunnel.api.table.type.EnumType;
import org.apache.seatunnel.api.table.type.ListType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PojoType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.TimestampType;
import org.apache.seatunnel.translation.flink.types.ArrayTypeConverter;
import org.apache.seatunnel.translation.flink.types.BasicTypeConverter;
import org.apache.seatunnel.translation.flink.types.PojoTypeConverter;
import org.apache.seatunnel.translation.flink.types.TimestampTypeConverter;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.EnumTypeInfo;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class TypeConverterUtils {

    private TypeConverterUtils() {
        throw new UnsupportedOperationException("TypeConverterUtils is a utility class and cannot be instantiated");
    }

    public static <T1, T2> SeaTunnelDataType<T2> convertType(TypeInformation<T1> dataType) {
        if (dataType instanceof BasicTypeInfo) {
            return (SeaTunnelDataType<T2>) convertBasicType((BasicTypeInfo<T1>) dataType);
        }
        // todo:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }

    @SuppressWarnings("unchecked")
    public static <T1, T2> TypeInformation<T2> convertType(SeaTunnelDataType<T1> dataType) {
        if (dataType instanceof BasicType) {
            return (TypeInformation<T2>) convertBasicType((BasicType<T1>) dataType);
        }
        if (dataType instanceof TimestampType) {
            return (TypeInformation<T2>)
                TimestampTypeConverter.INSTANCE.convert((TimestampType) dataType);
        }
        if (dataType instanceof ArrayType) {
            return (TypeInformation<T2>) convertArrayType((ArrayType<T1>) dataType);
        }
        if (dataType instanceof ListType) {
            return (TypeInformation<T2>) covertListType((ListType<T1>) dataType);
        }
        if (dataType instanceof EnumType) {

            return (TypeInformation<T2>) coverEnumType((EnumType<?>) dataType);
        }
        if (dataType instanceof MapType) {
            return (TypeInformation<T2>) convertMapType((MapType<?, ?>) dataType);
        }
        if (dataType instanceof PojoType) {
            return (TypeInformation<T2>) convertPojoType((PojoType<T1>) dataType);
        }
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }

    @SuppressWarnings("unchecked")
    public static <T> TypeInformation<T> convertBasicType(BasicType<T> basicType) {
        Class<T> physicalTypeClass = basicType.getPhysicalTypeClass();
        if (physicalTypeClass == Boolean.class) {
            BasicType<Boolean> booleanBasicType = (BasicType<Boolean>) basicType;
            return (TypeInformation<T>)
                BasicTypeConverter.BOOLEAN_CONVERTER.convert(booleanBasicType);
        }
        if (physicalTypeClass == String.class) {
            BasicType<String> stringBasicType = (BasicType<String>) basicType;
            return (TypeInformation<T>)
                BasicTypeConverter.STRING_CONVERTER.convert(stringBasicType);
        }
        if (physicalTypeClass == Date.class) {
            BasicType<Date> dateBasicType = (BasicType<Date>) basicType;
            return (TypeInformation<T>)
                BasicTypeConverter.DATE_CONVERTER.convert(dateBasicType);
        }
        if (physicalTypeClass == Double.class) {
            BasicType<Double> doubleBasicType = (BasicType<Double>) basicType;
            return (TypeInformation<T>)
                BasicTypeConverter.DOUBLE_CONVERTER.convert(doubleBasicType);
        }
        if (physicalTypeClass == Integer.class) {
            BasicType<Integer> integerBasicType = (BasicType<Integer>) basicType;
            return (TypeInformation<T>)
                BasicTypeConverter.INTEGER_CONVERTER.convert(integerBasicType);
        }
        if (physicalTypeClass == Long.class) {
            BasicType<Long> longBasicType = (BasicType<Long>) basicType;
            return (TypeInformation<T>)
                BasicTypeConverter.LONG_CONVERTER.convert(longBasicType);
        }
        if (physicalTypeClass == Float.class) {
            BasicType<Float> floatBasicType = (BasicType<Float>) basicType;
            return (TypeInformation<T>)
                BasicTypeConverter.FLOAT_CONVERTER.convert(floatBasicType);
        }
        if (physicalTypeClass == Byte.class) {
            BasicType<Byte> byteBasicType = (BasicType<Byte>) basicType;
            return (TypeInformation<T>)
                BasicTypeConverter.BYTE_CONVERTER.convert(byteBasicType);
        }
        if (physicalTypeClass == Short.class) {
            BasicType<Short> shortBasicType = (BasicType<Short>) basicType;
            return (TypeInformation<T>)
                BasicTypeConverter.SHORT_CONVERTER.convert(shortBasicType);
        }
        if (physicalTypeClass == Character.class) {
            BasicType<Character> characterBasicType = (BasicType<Character>) basicType;
            return (TypeInformation<T>)
                BasicTypeConverter.CHARACTER_CONVERTER.convert(characterBasicType);
        }
        if (physicalTypeClass == BigInteger.class) {
            BasicType<BigInteger> bigIntegerBasicType = (BasicType<BigInteger>) basicType;
            return (TypeInformation<T>)
                BasicTypeConverter.BIG_INTEGER_CONVERTER.convert(bigIntegerBasicType);
        }
        if (physicalTypeClass == BigDecimal.class) {
            BasicType<BigDecimal> bigDecimalBasicType = (BasicType<BigDecimal>) basicType;
            return (TypeInformation<T>)
                BasicTypeConverter.BIG_DECIMAL_CONVERTER.convert(bigDecimalBasicType);
        }
        if (physicalTypeClass == Void.class) {
            BasicType<Void> voidBasicType = (BasicType<Void>) basicType;
            return (TypeInformation<T>)
                BasicTypeConverter.NULL_CONVERTER.convert(voidBasicType);
        }
        throw new IllegalArgumentException("Unsupported basic type: " + basicType);
    }

    @SuppressWarnings("unchecked")
    private static <T1> SeaTunnelDataType<T1> convertBasicType(BasicTypeInfo<T1> flinkDataType) {
        Class<T1> physicalTypeClass = flinkDataType.getTypeClass();
        if (physicalTypeClass == Boolean.class) {
            TypeInformation<Boolean> booleanTypeInformation = (TypeInformation<Boolean>) flinkDataType;
            return (SeaTunnelDataType<T1>)
                BasicTypeConverter.BOOLEAN_CONVERTER.reconvert(booleanTypeInformation);
        }
        if (physicalTypeClass == String.class) {
            TypeInformation<String> stringBasicType = (TypeInformation<String>) flinkDataType;
            return (SeaTunnelDataType<T1>)
                BasicTypeConverter.STRING_CONVERTER.reconvert(stringBasicType);
        }
        if (physicalTypeClass == Date.class) {
            TypeInformation<Date> dateBasicType = (TypeInformation<Date>) flinkDataType;
            return (SeaTunnelDataType<T1>)
                BasicTypeConverter.DATE_CONVERTER.reconvert(dateBasicType);
        }
        if (physicalTypeClass == Double.class) {
            TypeInformation<Double> doubleBasicType = (TypeInformation<Double>) flinkDataType;
            return (SeaTunnelDataType<T1>)
                BasicTypeConverter.DOUBLE_CONVERTER.reconvert(doubleBasicType);
        }
        if (physicalTypeClass == Integer.class) {
            TypeInformation<Integer> integerBasicType = (TypeInformation<Integer>) flinkDataType;
            return (SeaTunnelDataType<T1>)
                BasicTypeConverter.INTEGER_CONVERTER.reconvert(integerBasicType);
        }
        if (physicalTypeClass == Long.class) {
            TypeInformation<Long> longBasicType = (TypeInformation<Long>) flinkDataType;
            return (SeaTunnelDataType<T1>)
                BasicTypeConverter.LONG_CONVERTER.reconvert(longBasicType);
        }
        if (physicalTypeClass == Float.class) {
            TypeInformation<Float> floatBasicType = (TypeInformation<Float>) flinkDataType;
            return (SeaTunnelDataType<T1>)
                BasicTypeConverter.FLOAT_CONVERTER.reconvert(floatBasicType);
        }
        if (physicalTypeClass == Byte.class) {
            TypeInformation<Byte> byteBasicType = (TypeInformation<Byte>) flinkDataType;
            return (SeaTunnelDataType<T1>)
                BasicTypeConverter.BYTE_CONVERTER.reconvert(byteBasicType);
        }
        if (physicalTypeClass == Short.class) {
            TypeInformation<Short> shortBasicType = (TypeInformation<Short>) flinkDataType;
            return (SeaTunnelDataType<T1>)
                BasicTypeConverter.SHORT_CONVERTER.reconvert(shortBasicType);
        }
        if (physicalTypeClass == Character.class) {
            TypeInformation<Character> characterBasicType = (TypeInformation<Character>) flinkDataType;
            return (SeaTunnelDataType<T1>)
                BasicTypeConverter.CHARACTER_CONVERTER.reconvert(characterBasicType);
        }
        if (physicalTypeClass == BigInteger.class) {
            TypeInformation<BigInteger> bigIntegerBasicType = (TypeInformation<BigInteger>) flinkDataType;
            return (SeaTunnelDataType<T1>)
                BasicTypeConverter.BIG_INTEGER_CONVERTER.reconvert(bigIntegerBasicType);
        }
        if (physicalTypeClass == BigDecimal.class) {
            TypeInformation<BigDecimal> bigDecimalBasicType = (TypeInformation<BigDecimal>) flinkDataType;
            return (SeaTunnelDataType<T1>)
                BasicTypeConverter.BIG_DECIMAL_CONVERTER.reconvert(bigDecimalBasicType);
        }
        if (physicalTypeClass == Void.class) {
            TypeInformation<Void> voidBasicType = (TypeInformation<Void>) flinkDataType;
            return (SeaTunnelDataType<T1>)
                BasicTypeConverter.NULL_CONVERTER.reconvert(voidBasicType);
        }
        throw new IllegalArgumentException("Unsupported flink type: " + flinkDataType);
    }

    public static <T1, T2> BasicArrayTypeInfo<T1, T2> convertArrayType(ArrayType<T1> arrayType) {
        ArrayTypeConverter<T1, T2> arrayTypeConverter = new ArrayTypeConverter<>();
        return arrayTypeConverter.convert(arrayType);
    }

    public static <T> ListTypeInfo<T> covertListType(ListType<T> listType) {
        SeaTunnelDataType<T> elementType = listType.getElementType();
        return new ListTypeInfo<>(convertType(elementType));
    }

    public static <T extends Enum<T>> EnumTypeInfo<T> coverEnumType(EnumType<T> enumType) {
        Class<T> enumClass = enumType.getEnumClass();
        return new EnumTypeInfo<>(enumClass);
    }

    public static <K, V> MapTypeInfo<K, V> convertMapType(MapType<K, V> mapType) {
        SeaTunnelDataType<K> keyType = mapType.getKeyType();
        SeaTunnelDataType<V> valueType = mapType.getValueType();
        return new MapTypeInfo<>(convertType(keyType), convertType(valueType));
    }

    public static <T> PojoTypeInfo<T> convertPojoType(PojoType<T> pojoType) {
        PojoTypeConverter<T> pojoTypeConverter = new PojoTypeConverter<>();
        return pojoTypeConverter.convert(pojoType);
    }
}
