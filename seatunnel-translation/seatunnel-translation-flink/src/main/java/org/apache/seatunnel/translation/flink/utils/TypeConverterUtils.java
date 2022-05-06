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
import org.apache.seatunnel.api.table.type.DataType;
import org.apache.seatunnel.api.table.type.EnumType;
import org.apache.seatunnel.api.table.type.ListType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PojoType;
import org.apache.seatunnel.api.table.type.TimestampType;
import org.apache.seatunnel.translation.flink.types.ArrayTypeConverter;
import org.apache.seatunnel.translation.flink.types.BasicTypeConverter;
import org.apache.seatunnel.translation.flink.types.PojoTypeConverter;
import org.apache.seatunnel.translation.flink.types.TimestampTypeConverter;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.EnumTypeInfo;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class TypeConverterUtils {

    @SuppressWarnings("unchecked")
    public static <T1, T2> TypeInformation<T2> convertType(DataType<T1> dataType) {
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
                BasicTypeConverter.BIG_DECIMAL.convert(bigDecimalBasicType);
        }
        if (physicalTypeClass == Void.class) {
            BasicType<Void> voidBasicType = (BasicType<Void>) basicType;
            return (TypeInformation<T>)
                BasicTypeConverter.NULL_CONVERTER.convert(voidBasicType);
        }
        throw new IllegalArgumentException("Unsupported basic type: " + basicType);
    }

    public static <T1, T2> BasicArrayTypeInfo<T1, T2> convertArrayType(ArrayType<T1> arrayType) {
        ArrayTypeConverter<T1, T2> arrayTypeConverter = new ArrayTypeConverter<>();
        return arrayTypeConverter.convert(arrayType);
    }

    public static <T> ListTypeInfo<T> covertListType(ListType<T> listType) {
        DataType<T> elementType = listType.getElementType();
        return new ListTypeInfo<>(convertType(elementType));
    }

    public static <T extends Enum<T>> EnumTypeInfo<T> coverEnumType(EnumType<T> enumType) {
        Class<T> enumClass = enumType.getEnumClass();
        return new EnumTypeInfo<>(enumClass);
    }

    public static <K, V> MapTypeInfo<K, V> convertMapType(MapType<K, V> mapType) {
        DataType<K> keyType = mapType.getKeyType();
        DataType<V> valueType = mapType.getValueType();
        return new MapTypeInfo<>(convertType(keyType), convertType(valueType));
    }

    public static <T> PojoTypeInfo<T> convertPojoType(PojoType<T> pojoType) {
        PojoTypeConverter<T> pojoTypeConverter = new PojoTypeConverter<>();
        return pojoTypeConverter.convert(pojoType);
    }
}
