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

package org.apache.seatunnel.transform.sql.zeta.functions;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public enum SystemFunctionEnum {

    /**
     * CAST(value as dataType)
     *
     * <p>Converts a value to another data type.
     *
     * <p>Supported data types: STRING | VARCHAR, INT | INTEGER, LONG | BIGINT, BYTE, FLOAT, DOUBLE,
     * DECIMAL(p,s), TIMESTAMP, DATE, TIME
     *
     * <p>Example:
     *
     * <p>CONVERT(NAME AS INT)
     */
    CAST {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() < 2 || objects.size() > 4) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require between 2 and 4");
            }
            SeaTunnelDataType<?> type = types.get(0);
            Object val = null;

            String origin = FunctionUtils.convertToString(objects.get(0), "");
            String typeStr = FunctionUtils.convertToString(objects.get(1), "");
            LocalDateTime dateTime;
            try {
                switch (typeStr.toUpperCase()) {
                    case "INT":
                    case "INTEGER":
                        type = BasicType.INT_TYPE;
                        val = Integer.parseInt(origin);
                        break;
                    case "BIGINT":
                    case "LONG":
                        type = BasicType.LONG_TYPE;
                        val = Long.parseLong(origin);
                        break;
                    case "BYTE":
                        type = BasicType.BYTE_TYPE;
                        val = Byte.parseByte(origin);
                        break;
                    case "DOUBLE":
                        type = BasicType.DOUBLE_TYPE;
                        val = Double.parseDouble(origin);
                        break;
                    case "FLOAT":
                        type = BasicType.FLOAT_TYPE;
                        val = Float.parseFloat(origin);
                        break;
                    case "TIMESTAMP":
                    case "DATETIME":
                        type = LocalTimeType.LOCAL_DATE_TIME_TYPE;
                        val = FunctionUtils.parseDate(origin);
                        break;
                    case "DATE":
                        type = LocalTimeType.LOCAL_DATE_TYPE;
                        dateTime = FunctionUtils.parseDate(origin);
                        if (dateTime != null) {
                            val = dateTime.toLocalDate();
                        }
                        break;
                    case "TIME":
                        type = LocalTimeType.LOCAL_TIME_TYPE;
                        dateTime = FunctionUtils.parseDate(origin);
                        if (dateTime != null) {
                            val = dateTime.toLocalTime();
                        }
                        break;
                    case "DECIMAL":
                        int precision = 0;
                        int scale = 0;
                        if (objects.size() >= 3) {
                            precision =
                                    Optional.ofNullable(objects.get(2))
                                            .map(item -> NumberUtils.toInt(item.toString()))
                                            .orElse(precision);
                        }
                        if (objects.size() >= 4) {
                            scale =
                                    Optional.ofNullable(objects.get(3))
                                            .map(item -> NumberUtils.toInt(item.toString()))
                                            .orElse(scale);
                        }
                        type = new DecimalType(precision, scale);
                        BigDecimal bigDecimal = new BigDecimal(origin);
                        val = bigDecimal.setScale(scale, RoundingMode.CEILING);
                        break;
                    case "VARCHAR":
                    case "STRING":
                    default:
                        type = BasicType.STRING_TYPE;
                        val = origin;
                }
            } catch (Exception e) {
                // no op
            }
            return Pair.of(type, val);
        }
    },
    /**
     * COALESCE(aValue, bValue [,...])
     *
     * <p>Returns the first value that is not null.
     *
     * <p>Example:
     *
     * <p>COALESCE(A, B, C)
     */
    COALESCE {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() == 0) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require gt 0");
            }
            return FunctionUtils.getNotNullFirst(types, objects);
        }
    },
    /**
     * IFNULL(aValue, bValue)
     *
     * <p>Returns the first value that is not null.
     *
     * <p>Example:
     *
     * <p>IFNULL(A, B)
     */
    IFNULL {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 2) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 2");
            }
            return FunctionUtils.getNotNullFirst(types, objects);
        }
    },
    /**
     * NULLIF(aValue, bValue)
     *
     * <p>Returns NULL if 'a' is equal to 'b', otherwise 'a'.
     *
     * <p>Example:
     *
     * <p>NULLIF(A, B)
     */
    NULLIF {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 2) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 2");
            }
            SeaTunnelDataType<?> type = types.get(0);
            Object val = null;
            Object a = objects.get(0);
            Object b = objects.get(1);
            if (a != null) {
                if (!a.equals(b)) {
                    val = a;
                }
            }
            return Pair.of(type, val);
        }
    };

    public abstract Pair<SeaTunnelDataType<?>, Object> execute(
            List<SeaTunnelDataType<?>> types, List<Object> objects);

    public static Pair<SeaTunnelDataType<?>, Object> execute(
            String funcName, List<SeaTunnelDataType<?>> types, List<Object> objects) {
        for (SystemFunctionEnum value : values()) {
            if (value.name().equals(funcName.toUpperCase())) {
                return value.execute(types, objects);
            }
        }
        return null;
    }
}
