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

final class SeaTunnelArrayType {

    private SeaTunnelArrayType() {}

    static Object[] newArray(ArrayType<?, ?> type, int length) {
        return (Object[]) Array.newInstance(runtimeClass(type.getElementType()), length);
    }

    private static Class<?> runtimeClass(SeaTunnelDataType<?> type) {
        // Parsed array-of-map types carry MapType.class, not the runtime Map[].class.
        if (type instanceof ArrayType) {
            return Array.newInstance(runtimeClass(((ArrayType<?, ?>) type).getElementType()), 0)
                    .getClass();
        }
        return type.getTypeClass();
    }
}
