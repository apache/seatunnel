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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Date;

public class BasicTypeConverter<T1>
    implements FlinkTypeConverter<BasicType<T1>, TypeInformation<T1>> {

    public static final BasicTypeConverter<String> STRING_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.STRING,
            BasicTypeInfo.STRING_TYPE_INFO);

    public static final BasicTypeConverter<Integer> INTEGER_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.INTEGER,
            BasicTypeInfo.INT_TYPE_INFO);

    public static final BasicTypeConverter<Boolean> BOOLEAN_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.BOOLEAN,
            BasicTypeInfo.BOOLEAN_TYPE_INFO);

    public static final BasicTypeConverter<Date> DATE_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.DATE,
            BasicTypeInfo.DATE_TYPE_INFO);

    public static final BasicTypeConverter<Double> DOUBLE_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.DOUBLE,
            BasicTypeInfo.DOUBLE_TYPE_INFO);

    public static final BasicTypeConverter<Long> LONG_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.LONG,
            BasicTypeInfo.LONG_TYPE_INFO);

    public static final BasicTypeConverter<Float> FLOAT_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.FLOAT,
            BasicTypeInfo.FLOAT_TYPE_INFO);

    public static final BasicTypeConverter<Byte> BYTE_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.BYTE,
            BasicTypeInfo.BYTE_TYPE_INFO);

    public static final BasicTypeConverter<Short> SHORT_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.SHORT,
            BasicTypeInfo.SHORT_TYPE_INFO);

    public static final BasicTypeConverter<Character> CHARACTER_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.CHARACTER,
            BasicTypeInfo.CHAR_TYPE_INFO);

    public static final BasicTypeConverter<BigInteger> BIG_INTEGER_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.BIG_INTEGER,
            BasicTypeInfo.BIG_INT_TYPE_INFO);

    public static final BasicTypeConverter<BigDecimal> BIG_DECIMAL_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.BIG_DECIMAL,
            BasicTypeInfo.BIG_DEC_TYPE_INFO);

    public static final BasicTypeConverter<Instant> INSTANT_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.INSTANT,
            BasicTypeInfo.INSTANT_TYPE_INFO);

    public static final BasicTypeConverter<Void> NULL_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.NULL,
            BasicTypeInfo.VOID_TYPE_INFO);

    private final BasicType<T1> seaTunnelDataType;
    private final TypeInformation<T1> flinkTypeInformation;

    public BasicTypeConverter(BasicType<T1> seaTunnelDataType, TypeInformation<T1> flinkTypeInformation) {
        this.seaTunnelDataType = seaTunnelDataType;
        this.flinkTypeInformation = flinkTypeInformation;
    }

    @Override
    public TypeInformation<T1> convert(BasicType<T1> seaTunnelDataType) {
        return flinkTypeInformation;
    }

    @Override
    public BasicType<T1> reconvert(TypeInformation<T1> dataType) {
        return seaTunnelDataType;
    }

}
