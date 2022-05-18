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

public class ArrayTypeConverter<T1, T2> implements FlinkTypeConverter<ArrayType<T1>, BasicArrayTypeInfo<T1, T2>> {

    @Override
    @SuppressWarnings("unchecked")
    public BasicArrayTypeInfo<T1, T2> convert(ArrayType<T1> arrayType) {
        // todo: now we only support basic array types
        BasicType<T1> elementType = arrayType.getElementType();
        if (BasicType.BOOLEAN.equals(elementType)) {
            return (BasicArrayTypeInfo<T1, T2>) BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO;
        }
        if (BasicType.STRING.equals(elementType)) {
            return (BasicArrayTypeInfo<T1, T2>) BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO;
        }
        if (BasicType.DOUBLE.equals(elementType)) {
            return (BasicArrayTypeInfo<T1, T2>) BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO;
        }
        if (BasicType.INTEGER.equals(elementType)) {
            return (BasicArrayTypeInfo<T1, T2>) BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO;
        }
        if (BasicType.LONG.equals(elementType)) {
            return (BasicArrayTypeInfo<T1, T2>) BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO;
        }
        if (BasicType.FLOAT.equals(elementType)) {
            return (BasicArrayTypeInfo<T1, T2>) BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO;
        }
        if (BasicType.BYTE.equals(elementType)) {
            return (BasicArrayTypeInfo<T1, T2>) BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO;
        }
        if (BasicType.SHORT.equals(elementType)) {
            return (BasicArrayTypeInfo<T1, T2>) BasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO;
        }
        if (BasicType.CHARACTER.equals(elementType)) {
            return (BasicArrayTypeInfo<T1, T2>) BasicArrayTypeInfo.CHAR_ARRAY_TYPE_INFO;
        }
        throw new IllegalArgumentException("Unsupported basic type: " + elementType);
    }

    @Override
    public ArrayType<T1> reconvert(BasicArrayTypeInfo<T1, T2> typeInformation) {
        return null;
    }
}
