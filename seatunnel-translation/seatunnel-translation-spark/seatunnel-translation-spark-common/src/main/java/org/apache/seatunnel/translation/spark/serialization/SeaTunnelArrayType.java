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

package org.apache.seatunnel.translation.spark.serialization;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.lang.reflect.Array;

/** Allocates Java arrays for Spark values using the declared SeaTunnel element types. */
final class SeaTunnelArrayType {

    private SeaTunnelArrayType() {}

    /** Returns a typed array, deriving nested array classes from their runtime element classes. */
    static Object[] newArray(ArrayType<?, ?> type, int length) {
        return (Object[]) Array.newInstance(runtimeClass(type.getElementType()), length);
    }

    /** Stores a converted element and reports type mismatches without including its value. */
    static void setElement(Object[] array, int index, Object value, ArrayType<?, ?> type) {
        try {
            array[index] = value;
        } catch (ArrayStoreException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot store array element at index %s for declared type %s: "
                                    + "expected %s but got %s",
                            index,
                            type,
                            array.getClass().getComponentType().getName(),
                            value.getClass().getName()),
                    e);
        }
    }

    private static Class<?> runtimeClass(SeaTunnelDataType<?> type) {
        // Do not trust ArrayType.getTypeClass(): parseArrayType in SeaTunnelDataTypeConvertorUtil
        // uses MapType.class for arrays of maps, and LOCAL_*_ARRAY_TYPE constants use
        // LocalTimeType[].
        if (type instanceof ArrayType) {
            return Array.newInstance(runtimeClass(((ArrayType<?, ?>) type).getElementType()), 0)
                    .getClass();
        }
        return type.getTypeClass();
    }
}
