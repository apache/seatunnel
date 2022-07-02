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

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.table.runtime.typeutils.BigDecimalTypeInfo;
import org.junit.Assert;
import org.junit.Test;

public class TypeConverterUtilsTest {
    //--------------------------------------------------------------
    // basic types test
    //--------------------------------------------------------------

    @Test
    public void convertStringType() {
        Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, TypeConverterUtils.convert(BasicType.STRING_TYPE));
    }

    @Test
    public void convertIntegerType() {
        Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, TypeConverterUtils.convert(BasicType.INT_TYPE));
    }

    @Test
    public void convertBooleanType() {
        Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, TypeConverterUtils.convert(BasicType.BOOLEAN_TYPE));
    }

    @Test
    public void convertDoubleType() {
        Assert.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, TypeConverterUtils.convert(BasicType.DOUBLE_TYPE));
    }

    @Test
    public void convertLongType() {
        Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, TypeConverterUtils.convert(BasicType.LONG_TYPE));
    }

    @Test
    public void convertFloatType() {
        Assert.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, TypeConverterUtils.convert(BasicType.FLOAT_TYPE));
    }

    @Test
    public void convertByteType() {
        Assert.assertEquals(BasicTypeInfo.BYTE_TYPE_INFO, TypeConverterUtils.convert(BasicType.BYTE_TYPE));
    }

    @Test
    public void convertShortType() {
        Assert.assertEquals(BasicTypeInfo.SHORT_TYPE_INFO, TypeConverterUtils.convert(BasicType.SHORT_TYPE));
    }

    @Test
    @SuppressWarnings("MagicNumber")
    public void convertBigDecimalType() {
        Assert.assertEquals(new BigDecimalTypeInfo(30, 2), TypeConverterUtils.convert(new DecimalType(30, 2)));
    }

    @Test
    public void convertNullType() {
        Assert.assertEquals(BasicTypeInfo.VOID_TYPE_INFO, TypeConverterUtils.convert(BasicType.VOID_TYPE));
    }

    //--------------------------------------------------------------
    // array types test
    //--------------------------------------------------------------

    @Test
    public void convertBooleanArrayType() {
        Assert.assertEquals(BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO, TypeConverterUtils.convert(ArrayType.BOOLEAN_ARRAY_TYPE));
    }

    @Test
    public void convertStringArrayType() {
        Assert.assertEquals(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO, TypeConverterUtils.convert(ArrayType.STRING_ARRAY_TYPE));
    }

    @Test
    public void convertDoubleArrayType() {
        Assert.assertEquals(BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO, TypeConverterUtils.convert(ArrayType.DOUBLE_ARRAY_TYPE));
    }

    @Test
    public void convertIntegerArrayType() {
        Assert.assertEquals(BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO, TypeConverterUtils.convert(ArrayType.INT_ARRAY_TYPE));
    }

    @Test
    public void convertLongArrayType() {
        Assert.assertEquals(BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO, TypeConverterUtils.convert(ArrayType.LONG_ARRAY_TYPE));
    }

    @Test
    public void convertFloatArrayType() {
        Assert.assertEquals(BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO, TypeConverterUtils.convert(ArrayType.FLOAT_ARRAY_TYPE));
    }

    @Test
    public void convertByteArrayType() {
        Assert.assertEquals(BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO, TypeConverterUtils.convert(ArrayType.BYTE_ARRAY_TYPE));
    }

    @Test
    public void convertShortArrayType() {
        Assert.assertEquals(BasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO, TypeConverterUtils.convert(ArrayType.SHORT_ARRAY_TYPE));
    }
}
