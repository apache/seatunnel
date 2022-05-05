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
import org.apache.seatunnel.api.table.type.DataType;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Date;

public class BasicTypeConverter<T1, T2> implements FlinkTypeConverter<T1, T2> {

    public static final BasicTypeConverter<String, String> STRING_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.STRING,
            BasicTypeInfo.STRING_TYPE_INFO);

    public static final BasicTypeConverter<Integer, Integer> INTEGER_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.INTEGER,
            BasicTypeInfo.INT_TYPE_INFO);

    public static final BasicTypeConverter<Boolean, Boolean> BOOLEAN_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.BOOLEAN,
            BasicTypeInfo.BOOLEAN_TYPE_INFO);

    public static final BasicTypeConverter<Date, Date> DATE_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.DATE,
            BasicTypeInfo.DATE_TYPE_INFO);

    public static final BasicTypeConverter<Double, Double> DOUBLE_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.DOUBLE,
            BasicTypeInfo.DOUBLE_TYPE_INFO);

    public static final BasicTypeConverter<Long, Long> LONG_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.LONG,
            BasicTypeInfo.LONG_TYPE_INFO);

    public static final BasicTypeConverter<Float, Float> FLOAT_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.FLOAT,
            BasicTypeInfo.FLOAT_TYPE_INFO);

    public static final BasicTypeConverter<Byte, Byte> BYTE_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.BYTE,
            BasicTypeInfo.BYTE_TYPE_INFO);

    public static final BasicTypeConverter<Void, Void> NULL_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.NULL,
            BasicTypeInfo.VOID_TYPE_INFO);

    private final DataType<T1> dataType;
    private final TypeInformation<T2> typeInformation;

    public BasicTypeConverter(DataType<T1> dataType, TypeInformation<T2> typeInformation) {
        this.dataType = dataType;
        this.typeInformation = typeInformation;
    }

    @Override
    public TypeInformation<T2> convert(DataType<T1> dataType) {
        return typeInformation;
    }
}
