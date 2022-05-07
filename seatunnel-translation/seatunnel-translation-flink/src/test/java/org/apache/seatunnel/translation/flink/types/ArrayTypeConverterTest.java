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

package org.apache.seatunnel.translation.flink.types;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.junit.Assert;
import org.junit.Test;

public class ArrayTypeConverterTest {

    @Test
    public void convertBooleanArrayType() {
        ArrayType<Boolean> booleanArrayType = new ArrayType<>(BasicType.BOOLEAN);
        ArrayTypeConverter<Boolean, Boolean> booleanArrayTypeConverter = new ArrayTypeConverter<>();
        Assert.assertEquals(
            BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO,
            booleanArrayTypeConverter.convert(booleanArrayType));
    }

    @Test
    public void convertStringArrayType() {
        ArrayType<String> stringArrayType = new ArrayType<>(BasicType.STRING);
        ArrayTypeConverter<String, String> stringArrayTypeConverter = new ArrayTypeConverter<>();
        Assert.assertEquals(
            BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO,
            stringArrayTypeConverter.convert(stringArrayType));
    }

    @Test
    public void convertDoubleArrayType() {
        ArrayType<Double> doubleArrayType = new ArrayType<>(BasicType.DOUBLE);
        ArrayTypeConverter<Double, Double> doubleArrayTypeConverter = new ArrayTypeConverter<>();
        Assert.assertEquals(
            BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO,
            doubleArrayTypeConverter.convert(doubleArrayType));
    }

    @Test
    public void convertIntegerArrayType() {
        ArrayType<Integer> integerArrayType = new ArrayType<>(BasicType.INTEGER);
        ArrayTypeConverter<Integer, Integer> integerArrayTypeConverter = new ArrayTypeConverter<>();
        Assert.assertEquals(
            BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO,
            integerArrayTypeConverter.convert(integerArrayType));
    }

    @Test
    public void convertLongArrayType() {
        ArrayType<Long> longArrayType = new ArrayType<>(BasicType.LONG);
        ArrayTypeConverter<Long, Long> longArrayTypeConverter = new ArrayTypeConverter<>();
        Assert.assertEquals(
            BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO,
            longArrayTypeConverter.convert(longArrayType));
    }

    @Test
    public void convertFloatArrayType() {
        ArrayType<Float> floatArrayType = new ArrayType<>(BasicType.FLOAT);
        ArrayTypeConverter<Float, Float> floatArrayTypeConverter = new ArrayTypeConverter<>();
        Assert.assertEquals(
            BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO,
            floatArrayTypeConverter.convert(floatArrayType));
    }

    @Test
    public void convertByteArrayType() {
        ArrayType<Byte> byteArrayType = new ArrayType<>(BasicType.BYTE);
        ArrayTypeConverter<Byte, Byte> byteArrayTypeConverter = new ArrayTypeConverter<>();
        Assert.assertEquals(
            BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO,
            byteArrayTypeConverter.convert(byteArrayType));
    }

    @Test
    public void convertShortArrayType() {
        ArrayType<Short> shortArrayType = new ArrayType<>(BasicType.SHORT);
        ArrayTypeConverter<Short, Short> shortArrayTypeConverter = new ArrayTypeConverter<>();
        Assert.assertEquals(
            BasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO,
            shortArrayTypeConverter.convert(shortArrayType));
    }

    @Test
    public void convertCharacterArrayType() {
        ArrayType<Character> characterArrayType = new ArrayType<>(BasicType.CHARACTER);
        ArrayTypeConverter<Character, Character> characterArrayTypeConverter = new ArrayTypeConverter<>();
        Assert.assertEquals(
            BasicArrayTypeInfo.CHAR_ARRAY_TYPE_INFO,
            characterArrayTypeConverter.convert(characterArrayType));
    }

}
