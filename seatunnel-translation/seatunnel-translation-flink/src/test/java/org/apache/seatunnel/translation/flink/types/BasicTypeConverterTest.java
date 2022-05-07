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

import org.apache.seatunnel.api.table.type.BasicType;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Date;

public class BasicTypeConverterTest {

    @Test
    public void convertStringType() {
        BasicType<String> stringBasicType = BasicType.STRING;
        TypeInformation<String> stringTypeInformation =
            BasicTypeConverter.STRING_CONVERTER.convert(stringBasicType);
        Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, stringTypeInformation);
    }

    @Test
    public void convertIntegerType() {
        BasicType<Integer> integerBasicType = BasicType.INTEGER;
        TypeInformation<Integer> integerTypeInformation =
            BasicTypeConverter.INTEGER_CONVERTER.convert(integerBasicType);
        Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, integerTypeInformation);
    }

    @Test
    public void convertBooleanType() {
        BasicType<Boolean> booleanBasicType = BasicType.BOOLEAN;
        TypeInformation<Boolean> booleanTypeInformation =
            BasicTypeConverter.BOOLEAN_CONVERTER.convert(booleanBasicType);
        Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, booleanTypeInformation);
    }

    @Test
    public void convertDateType() {
        BasicType<Date> dateBasicType = BasicType.DATE;
        TypeInformation<Date> dateTypeInformation =
            BasicTypeConverter.DATE_CONVERTER.convert(dateBasicType);
        Assert.assertEquals(BasicTypeInfo.DATE_TYPE_INFO, dateTypeInformation);
    }

    @Test
    public void convertDoubleType() {
        BasicType<Double> doubleBasicType = BasicType.DOUBLE;
        TypeInformation<Double> doubleTypeInformation =
            BasicTypeConverter.DOUBLE_CONVERTER.convert(doubleBasicType);
        Assert.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, doubleTypeInformation);
    }

    @Test
    public void convertLongType() {
        BasicType<Long> longBasicType = BasicType.LONG;
        TypeInformation<Long> longTypeInformation =
            BasicTypeConverter.LONG_CONVERTER.convert(longBasicType);
        Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, longTypeInformation);
    }

    @Test
    public void convertFloatType() {
        BasicType<Float> floatBasicType = BasicType.FLOAT;
        TypeInformation<Float> floatTypeInformation =
            BasicTypeConverter.FLOAT_CONVERTER.convert(floatBasicType);
        Assert.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, floatTypeInformation);
    }

    @Test
    public void convertByteType() {
        BasicType<Byte> byteBasicType = BasicType.BYTE;
        TypeInformation<Byte> byteTypeInformation =
            BasicTypeConverter.BYTE_CONVERTER.convert(byteBasicType);
        Assert.assertEquals(BasicTypeInfo.BYTE_TYPE_INFO, byteTypeInformation);
    }

    @Test
    public void convertShortType() {
        BasicType<Short> shortBasicType = BasicType.SHORT;
        TypeInformation<Short> shortTypeInformation =
            BasicTypeConverter.SHORT_CONVERTER.convert(shortBasicType);
        Assert.assertEquals(BasicTypeInfo.SHORT_TYPE_INFO, shortTypeInformation);
    }

    @Test
    public void convertCharacterType() {
        BasicType<Character> characterBasicType = BasicType.CHARACTER;
        TypeInformation<Character> characterTypeInformation =
            BasicTypeConverter.CHARACTER_CONVERTER.convert(characterBasicType);
        Assert.assertEquals(BasicTypeInfo.CHAR_TYPE_INFO, characterTypeInformation);
    }

    @Test
    public void convertBigIntegerType() {
        BasicType<BigInteger> bigIntegerBasicType = BasicType.BIG_INTEGER;
        TypeInformation<BigInteger> bigIntegerTypeInformation =
            BasicTypeConverter.BIG_INTEGER_CONVERTER.convert(bigIntegerBasicType);
        Assert.assertEquals(BasicTypeInfo.BIG_INT_TYPE_INFO, bigIntegerTypeInformation);
    }

    @Test
    public void convertBigDecimalType() {
        BasicType<BigDecimal> bigDecimalBasicType = BasicType.BIG_DECIMAL;
        TypeInformation<BigDecimal> bigDecimalTypeInformation =
            BasicTypeConverter.BIG_DECIMAL.convert(bigDecimalBasicType);
        Assert.assertEquals(BasicTypeInfo.BIG_DEC_TYPE_INFO, bigDecimalTypeInformation);
    }

    @Test
    public void convertInstantType() {
        BasicType<Instant> instantBasicType = BasicType.INSTANT;
        TypeInformation<Instant> instantTypeInformation =
            BasicTypeConverter.INSTANT_CONVERTER.convert(instantBasicType);
        Assert.assertEquals(BasicTypeInfo.INSTANT_TYPE_INFO, instantTypeInformation);
    }

    @Test
    public void convertNullType() {
        BasicType<Void> nullBasicType = BasicType.NULL;
        TypeInformation<Void> nullTypeInformation =
            BasicTypeConverter.NULL_CONVERTER.convert(nullBasicType);
        Assert.assertEquals(BasicTypeInfo.VOID_TYPE_INFO, nullTypeInformation);
    }
}
