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

/**
 * The sql type of {@link SeaTunnelDataType}.
 */
public enum SqlType {
    ARRAY,
    STRING,
    BOOLEAN,
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    CHAR,
    DECIMAL,
    NULL,
    BYTES,
    DATE,
    TIME,
    TIMESTAMP,
    ROW;

    /**
     * Get the logical type of {@link SeaTunnelDataType}.
     */
    public static SqlType of(SeaTunnelDataType<?> dataType) {
        if (SeaTunnelRowType.class.equals(dataType.getClass())) {
            return ROW;
        }
        if (BasicType.class.equals(dataType.getClass())) {
            return toLogicalType((BasicType<?>) dataType);
        }
        if (LocalTimeType.class.equals(dataType.getClass())) {
            return toLogicalType((LocalTimeType<?>) dataType);
        }
        if (ArrayType.class.equals(dataType.getClass())) {
            return ARRAY;
        }
        if (PrimitiveArrayType.class.equals(dataType.getTypeClass())) {
            if (PrimitiveArrayType.PRIMITIVE_BYTE_ARRAY_TYPE.equals(dataType)) {
                return BYTES;
            }
        }
        // TODO: MAP, LIST
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }

    private static SqlType toLogicalType(BasicType<?> basicType) {
        if (BasicType.STRING_TYPE.equals(basicType)) {
            return STRING;
        }
        if (BasicType.BOOLEAN_TYPE.equals(basicType)) {
            return BOOLEAN;
        }
        if (BasicType.BYTE_TYPE.equals(basicType)) {
            return TINYINT;
        }
        if (BasicType.SHORT_TYPE.equals(basicType)) {
            return SMALLINT;
        }
        if (BasicType.INT_TYPE.equals(basicType)) {
            return INT;
        }
        if (BasicType.LONG_TYPE.equals(basicType)) {
            return BIGINT;
        }
        if (BasicType.FLOAT_TYPE.equals(basicType)) {
            return FLOAT;
        }
        if (BasicType.DOUBLE_TYPE.equals(basicType)) {
            return DOUBLE;
        }
        if (BasicType.BIG_DECIMAL_TYPE.equals(basicType)) {
            return DECIMAL;
        }
        if (BasicType.VOID_TYPE.equals(basicType)) {
            return NULL;
        }
        throw new UnsupportedOperationException("Unsupported basic type: " + basicType);
    }

    private static SqlType toLogicalType(LocalTimeType<?> basicType) {
        if (LocalTimeType.LOCAL_DATE_TYPE.equals(basicType)) {
            return DATE;
        }
        if (LocalTimeType.LOCAL_TIME_TYPE.equals(basicType)) {
            return TIME;
        }
        if (LocalTimeType.LOCAL_DATE_TIME_TYPE.equals(basicType)) {
            return TIMESTAMP;
        }
        throw new UnsupportedOperationException("Unsupported basic type: " + basicType);
    }
}
