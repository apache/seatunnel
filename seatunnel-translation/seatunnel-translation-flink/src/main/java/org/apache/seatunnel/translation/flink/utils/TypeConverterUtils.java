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
import org.apache.seatunnel.api.table.type.DataType;
import org.apache.seatunnel.api.table.type.ListType;
import org.apache.seatunnel.api.table.type.TimestampType;
import org.apache.seatunnel.translation.flink.types.TimestampTypeConverter;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class TypeConverterUtils {

    @SuppressWarnings("unchecked")
    public static <T1, T2> TypeInformation<T2> convertType(DataType<T1> dataType) {
        if (dataType instanceof BasicType) {
            return (TypeInformation<T2>) convertBasicType((BasicType<T1>) dataType);
        }
        if (dataType instanceof TimestampType) {
            return (TypeInformation<T2>)
                TimestampTypeConverter.INSTANCE.convert((TimestampType) dataType);
        }
        if (dataType instanceof ArrayType) {
            return convertBasicArrayType((ArrayType<T1>) dataType);
        }
        if (dataType instanceof ListType) {
            // todo:
        }
        // todo:
        return null;
    }

    @SuppressWarnings("unchecked")
    public static <T> TypeInformation<T> convertBasicType(BasicType<T> basicType) {
        Class<T> physicalTypeClass = basicType.getPhysicalTypeClass();
        if (physicalTypeClass == Boolean.class) {
            return (TypeInformation<T>) BasicTypeInfo.BOOLEAN_TYPE_INFO;
        }
        if (physicalTypeClass == String.class) {
            return (TypeInformation<T>) BasicTypeInfo.STRING_TYPE_INFO;
        }
        if (physicalTypeClass == Date.class) {
            return (TypeInformation<T>) BasicTypeInfo.DATE_TYPE_INFO;
        }
        if (physicalTypeClass == Double.class) {
            return (TypeInformation<T>) BasicTypeInfo.DOUBLE_TYPE_INFO;
        }
        if (physicalTypeClass == Integer.class) {
            return (TypeInformation<T>) BasicTypeInfo.INT_TYPE_INFO;
        }
        if (physicalTypeClass == Long.class) {
            return (TypeInformation<T>) BasicTypeInfo.LONG_TYPE_INFO;
        }
        if (physicalTypeClass == Float.class) {
            return (TypeInformation<T>) BasicTypeInfo.FLOAT_TYPE_INFO;
        }
        if (physicalTypeClass == Byte.class) {
            return (TypeInformation<T>) BasicTypeInfo.BYTE_TYPE_INFO;
        }
        if (physicalTypeClass == Short.class) {
            return (TypeInformation<T>) BasicTypeInfo.SHORT_TYPE_INFO;
        }
        if (physicalTypeClass == Character.class) {
            return (TypeInformation<T>) BasicTypeInfo.CHAR_TYPE_INFO;
        }
        if (physicalTypeClass == BigInteger.class) {
            return (TypeInformation<T>) BasicTypeInfo.BIG_INT_TYPE_INFO;
        }
        if (physicalTypeClass == BigDecimal.class) {
            return (TypeInformation<T>) BasicTypeInfo.BIG_DEC_TYPE_INFO;
        }
        if (physicalTypeClass == Void.class) {
            return (TypeInformation<T>) BasicTypeInfo.VOID_TYPE_INFO;
        }
        throw new IllegalArgumentException("Unsupported basic type: " + basicType);
    }

    @SuppressWarnings("unchecked")
    public static <T, C> BasicArrayTypeInfo<T, C> convertBasicArrayType(ArrayType<C> arrayType) {
        BasicType<C> elementType = (BasicType<C>) arrayType.getElementType();
        Class<C> physicalTypeClass = elementType.getPhysicalTypeClass();
        if (physicalTypeClass == Boolean.class) {
            return (BasicArrayTypeInfo<T, C>) BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO;
        }
        throw new IllegalArgumentException("Unsupported basic type: " + elementType);
    }
}
