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

package org.apache.seatunnel.translation.spark.utils;

import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.translation.spark.types.ArrayTypeConverter;
import org.apache.seatunnel.translation.spark.types.BasicTypeConverter;
import org.apache.seatunnel.translation.spark.types.PojoTypeConverter;
import org.apache.seatunnel.translation.spark.types.TimestampTypeConverter;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.ObjectType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;

public class TypeConverterUtils {

    private TypeConverterUtils() {
        throw new UnsupportedOperationException("TypeConverterUtils is a utility class and cannot be instantiated");
    }

    public static <T> DataType convert(SeaTunnelDataType<T> seaTunnelDataType) {
        if (seaTunnelDataType instanceof BasicType) {
            return convertBasicType((BasicType<T>) seaTunnelDataType);
        }
        if (seaTunnelDataType instanceof TimestampType) {
            return TimestampTypeConverter.INSTANCE.convert((TimestampType) seaTunnelDataType);
        }
        if (seaTunnelDataType instanceof ArrayType) {
            return convertArrayType((ArrayType<T>) seaTunnelDataType);
        }
        if (seaTunnelDataType instanceof PojoType) {
            return convertPojoType((PojoType<T>) seaTunnelDataType);
        }

        if (seaTunnelDataType instanceof LocalTimeType) {
            return convertLocalTimeType((LocalTimeType<T>) seaTunnelDataType);
        }

        throw new IllegalArgumentException("Unsupported data type: " + seaTunnelDataType);
    }

    private static <T> DataType convertLocalTimeType(LocalTimeType<T> localTimeType) {
        Class<T> physicalTypeClass = localTimeType.getPhysicalTypeClass();
        if (physicalTypeClass.equals(LocalDate.class)) {

        } else if (physicalTypeClass.equals(LocalDateTime.class)) {

        } else if (physicalTypeClass.equals(LocalTime.class)) {

        }
        throw new IllegalArgumentException("Unsupported local time type: " + physicalTypeClass);
    }

    @SuppressWarnings("unchecked")
    public static <T> DataType convertBasicType(BasicType<T> basicType) {
        Class<T> physicalTypeClass = basicType.getPhysicalTypeClass();
        if (physicalTypeClass == Boolean.class) {
            BasicType<Boolean> booleanBasicType = (BasicType<Boolean>) basicType;
            return BasicTypeConverter.BOOLEAN_CONVERTER.convert(booleanBasicType);
        }
        if (physicalTypeClass == String.class) {
            BasicType<String> stringBasicType = (BasicType<String>) basicType;
            return BasicTypeConverter.STRING_CONVERTER.convert(stringBasicType);
        }
        if (physicalTypeClass == Date.class) {
            BasicType<Date> dateBasicType = (BasicType<Date>) basicType;
            return BasicTypeConverter.DATE_CONVERTER.convert(dateBasicType);
        }
        if (physicalTypeClass == Double.class) {
            BasicType<Double> doubleBasicType = (BasicType<Double>) basicType;
            return BasicTypeConverter.DOUBLE_CONVERTER.convert(doubleBasicType);
        }
        if (physicalTypeClass == Integer.class) {
            BasicType<Integer> integerBasicType = (BasicType<Integer>) basicType;
            return BasicTypeConverter.INTEGER_CONVERTER.convert(integerBasicType);
        }
        if (physicalTypeClass == Long.class) {
            BasicType<Long> longBasicType = (BasicType<Long>) basicType;
            return BasicTypeConverter.LONG_CONVERTER.convert(longBasicType);
        }
        if (physicalTypeClass == Float.class) {
            BasicType<Float> floatBasicType = (BasicType<Float>) basicType;
            return BasicTypeConverter.FLOAT_CONVERTER.convert(floatBasicType);
        }
        if (physicalTypeClass == Byte.class) {
            BasicType<Byte> byteBasicType = (BasicType<Byte>) basicType;
            return BasicTypeConverter.BYTE_CONVERTER.convert(byteBasicType);
        }
        if (physicalTypeClass == Short.class) {
            BasicType<Short> shortBasicType = (BasicType<Short>) basicType;
            return BasicTypeConverter.SHORT_CONVERTER.convert(shortBasicType);
        }
        if (physicalTypeClass == Character.class) {
            BasicType<Character> characterBasicType = (BasicType<Character>) basicType;
            return BasicTypeConverter.CHARACTER_CONVERTER.convert(characterBasicType);
        }
        if (physicalTypeClass == BigInteger.class) {
            BasicType<BigInteger> bigIntegerBasicType = (BasicType<BigInteger>) basicType;
            return BasicTypeConverter.BIG_INTEGER_CONVERTER.convert(bigIntegerBasicType);
        }
        if (physicalTypeClass == BigDecimal.class) {
            BasicType<BigDecimal> bigDecimalBasicType = (BasicType<BigDecimal>) basicType;
            return BasicTypeConverter.BID_DECIMAL_CONVERTER.convert(bigDecimalBasicType);
        }
        if (physicalTypeClass == Void.class) {
            BasicType<Void> voidBasicType = (BasicType<Void>) basicType;
            return BasicTypeConverter.NULL_CONVERTER.convert(voidBasicType);
        }
        throw new IllegalArgumentException("Unsupported basic type: " + basicType);
    }

    public static <T1> org.apache.spark.sql.types.ArrayType convertArrayType(ArrayType<T1> arrayType) {
        ArrayTypeConverter<T1> arrayTypeConverter = new ArrayTypeConverter<>();
        return arrayTypeConverter.convert(arrayType);
    }

    public static <T> ObjectType convertPojoType(PojoType<T> pojoType) {
        PojoTypeConverter<T> pojoTypeConverter = new PojoTypeConverter<>();
        return pojoTypeConverter.convert(pojoType);
    }

    public static StructType convertRow(SeaTunnelRowTypeInfo typeInfo) {
        StructField[] fields = new StructField[typeInfo.getFieldNames().length];
        for (int i = 0; i < typeInfo.getFieldNames().length; i++) {
            fields[i] = new StructField(typeInfo.getFieldNames()[i],
                    convert(typeInfo.getSeaTunnelDataTypes()[i]), true, Metadata.empty());
        }
        return new StructType(fields);
    }

}
