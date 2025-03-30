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

package org.apache.seatunnel.api.table.type;

import java.util.Objects;

public class ArrayType<T, E> implements SeaTunnelDataType<T> {
    private static final long serialVersionUID = 2L;

    public static final ArrayType<String[], String> STRING_ARRAY_TYPE =
            new ArrayType<>(String[].class, BasicType.STRING_TYPE);
    public static final ArrayType<Boolean[], Boolean> BOOLEAN_ARRAY_TYPE =
            new ArrayType<>(Boolean[].class, BasicType.BOOLEAN_TYPE);
    public static final ArrayType<Byte[], Byte> BYTE_ARRAY_TYPE =
            new ArrayType<>(Byte[].class, BasicType.BYTE_TYPE);
    public static final ArrayType<Short[], Short> SHORT_ARRAY_TYPE =
            new ArrayType<>(Short[].class, BasicType.SHORT_TYPE);
    public static final ArrayType<Integer[], Integer> INT_ARRAY_TYPE =
            new ArrayType<>(Integer[].class, BasicType.INT_TYPE);
    public static final ArrayType<Long[], Long> LONG_ARRAY_TYPE =
            new ArrayType<>(Long[].class, BasicType.LONG_TYPE);
    public static final ArrayType<Float[], Float> FLOAT_ARRAY_TYPE =
            new ArrayType<>(Float[].class, BasicType.FLOAT_TYPE);
    public static final ArrayType<Double[], Double> DOUBLE_ARRAY_TYPE =
            new ArrayType<>(Double[].class, BasicType.DOUBLE_TYPE);

    public static final ArrayType<LocalTimeType[], LocalTimeType> LOCAL_DATE_ARRAY_TYPE =
            new ArrayType(LocalTimeType[].class, LocalTimeType.LOCAL_DATE_TYPE);

    public static final ArrayType<LocalTimeType[], LocalTimeType> LOCAL_TIME_ARRAY_TYPE =
            new ArrayType(LocalTimeType[].class, LocalTimeType.LOCAL_TIME_TYPE);

    public static final ArrayType<LocalTimeType[], LocalTimeType> LOCAL_DATE_TIME_ARRAY_TYPE =
            new ArrayType(LocalTimeType[].class, LocalTimeType.LOCAL_DATE_TIME_TYPE);

    public static final ArrayType<LocalTimeType[], LocalTimeType> OFFSET_DATE_TIME_ARRAY_TYPE =
            new ArrayType(LocalTimeType[].class, LocalTimeType.OFFSET_DATE_TIME_TYPE);

    // --------------------------------------------------------------------------------------------

    private final Class<T> arrayClass;
    private final SeaTunnelDataType<E> elementType;

    public ArrayType(Class<T> arrayClass, SeaTunnelDataType<E> elementType) {
        this.arrayClass = arrayClass;
        this.elementType = elementType;
    }

    public SeaTunnelDataType<E> getElementType() {
        return elementType;
    }

    @Override
    public Class<T> getTypeClass() {
        return arrayClass;
    }

    @Override
    public SqlType getSqlType() {
        return SqlType.ARRAY;
    }

    @Override
    public int hashCode() {
        return Objects.hash(arrayClass, elementType);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof ArrayType)) {
            return false;
        }
        ArrayType<?, ?> that = (ArrayType<?, ?>) obj;
        return Objects.equals(arrayClass, that.arrayClass)
                && Objects.equals(elementType, that.elementType);
    }

    @Override
    public String toString() {
        return String.format("ARRAY<%s>", elementType);
    }
}
