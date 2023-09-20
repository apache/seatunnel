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

package org.apache.seatunnel.transform.sql.zeta.visit;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.transform.sql.zeta.functions.FunctionUtils;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.math.RoundingMode;

public enum CalculateEnum {
    // +
    ADD {
        @Override
        public BigDecimal evaluate(
                SeaTunnelDataType<?> returnType, BigDecimal left, BigDecimal right) {
            return left.add(right);
        }
    },
    // -
    SUB {
        @Override
        public BigDecimal evaluate(
                SeaTunnelDataType<?> returnType, BigDecimal left, BigDecimal right) {
            return left.subtract(right);
        }
    },
    // *
    MUL {
        @Override
        public BigDecimal evaluate(
                SeaTunnelDataType<?> returnType, BigDecimal left, BigDecimal right) {
            return left.multiply(right);
        }
    },
    // /
    DIV {
        @Override
        public BigDecimal evaluate(
                SeaTunnelDataType<?> returnType, BigDecimal left, BigDecimal right) {
            int s1 = left.scale();
            int s2 = right.scale();
            int maxScale = Math.max(s1, s2);
            if (returnType instanceof DecimalType) {
                return left.divide(right, maxScale, RoundingMode.UP);
            }
            if (s1 > 0 || s2 > 0) {
                return BigDecimal.valueOf(left.doubleValue() / right.doubleValue());
            }
            if (s1 == 0 && s2 == 0) {
                return BigDecimal.valueOf(left.longValue() / right.longValue());
            }
            return null;
        }
    },
    // %
    MOD {
        @Override
        public BigDecimal evaluate(
                SeaTunnelDataType<?> returnType, BigDecimal left, BigDecimal right) {
            int s1 = left.scale();
            int s2 = right.scale();
            if (returnType instanceof DecimalType) {
                return left.remainder(right);
            }
            if (s1 > 0 || s2 > 0) {
                return BigDecimal.valueOf(left.doubleValue() % right.doubleValue());
            }
            if (s1 == 0 && s2 == 0) {
                return BigDecimal.valueOf(left.longValue() % right.longValue());
            }
            return null;
        }
    };

    public abstract BigDecimal evaluate(
            SeaTunnelDataType<?> returnType, BigDecimal left, BigDecimal right);

    public static BigDecimal evaluate(
            String name, SeaTunnelDataType<?> returnType, BigDecimal left, BigDecimal right) {
        if (left != null && right != null) {
            for (CalculateEnum value : values()) {
                if (value.name().equals(name.toUpperCase())) {
                    return value.evaluate(returnType, left, right);
                }
            }
        }
        return null;
    }

    public static Pair<SeaTunnelDataType<?>, Object> evaluate(
            String name,
            SeaTunnelDataType<?> leftType,
            SeaTunnelDataType<?> rightType,
            Object leftVal,
            Object rightVal) {

        Object val = null;
        String leftStr = FunctionUtils.convertToString(leftVal, "");
        String rightStr = FunctionUtils.convertToString(rightVal, "");

        BigDecimal left = null;
        if (NumberUtils.isCreatable(leftStr)) {
            left = new BigDecimal(leftStr);
        }

        BigDecimal right = null;
        if (NumberUtils.isCreatable(rightStr)) {
            right = new BigDecimal(rightStr);
        }

        if (leftType instanceof DecimalType || rightType instanceof DecimalType) {
            DecimalType type = null;
            if (leftType instanceof DecimalType && rightType instanceof DecimalType) {
                DecimalType leftDecimalType = (DecimalType) leftType;
                DecimalType rightDecimalType = (DecimalType) rightType;
                int precision =
                        Math.max(leftDecimalType.getPrecision(), rightDecimalType.getPrecision());
                int scale = Math.max(leftDecimalType.getScale(), rightDecimalType.getScale());
                type = new DecimalType(precision, scale);
            }
            if (leftType instanceof DecimalType) {
                type = (DecimalType) leftType;
            }
            if (rightType instanceof DecimalType) {
                type = (DecimalType) rightType;
            }
            val = evaluate(name, type, left, right);
            return Pair.of(type, val);
        }

        if (leftType.getSqlType() == SqlType.DOUBLE || rightType.getSqlType() == SqlType.DOUBLE) {
            SeaTunnelDataType<?> type = BasicType.DOUBLE_TYPE;
            BigDecimal bigDecimal = evaluate(name, type, left, right);
            if (bigDecimal != null) {
                val = bigDecimal.doubleValue();
            }
            return Pair.of(type, val);
        }

        if (leftType.getSqlType() == SqlType.FLOAT || rightType.getSqlType() == SqlType.FLOAT) {
            SeaTunnelDataType<?> type = BasicType.FLOAT_TYPE;
            BigDecimal bigDecimal = evaluate(name, type, left, right);
            if (bigDecimal != null) {
                val = bigDecimal.floatValue();
            }
            return Pair.of(type, val);
        }

        if (leftType.getSqlType() == SqlType.BIGINT || rightType.getSqlType() == SqlType.BIGINT) {
            SeaTunnelDataType<?> type = BasicType.LONG_TYPE;
            BigDecimal bigDecimal = evaluate(name, type, left, right);
            if (bigDecimal != null) {
                val = bigDecimal.longValue();
            }
            return Pair.of(type, val);
        }

        if (leftType.getSqlType() == SqlType.INT || rightType.getSqlType() == SqlType.INT) {
            SeaTunnelDataType<?> type = BasicType.INT_TYPE;
            BigDecimal bigDecimal = evaluate(name, type, left, right);
            if (bigDecimal != null) {
                val = bigDecimal.intValue();
            }
            return Pair.of(type, val);
        }

        if (leftType.getSqlType() == SqlType.SMALLINT
                || rightType.getSqlType() == SqlType.SMALLINT) {
            SeaTunnelDataType<?> type = BasicType.SHORT_TYPE;
            BigDecimal bigDecimal = evaluate(name, type, left, right);
            if (bigDecimal != null) {
                val = bigDecimal.shortValue();
            }
            return Pair.of(type, val);
        }

        if (leftType.getSqlType() == SqlType.TINYINT || rightType.getSqlType() == SqlType.TINYINT) {
            SeaTunnelDataType<?> type = BasicType.BYTE_TYPE;
            BigDecimal bigDecimal = evaluate(name, type, left, right);
            if (bigDecimal != null) {
                val = bigDecimal.byteValue();
            }
            return Pair.of(type, val);
        }

        BasicType<String> stringType = BasicType.STRING_TYPE;
        BigDecimal bigDecimal = evaluate(name, stringType, left, right);
        if (bigDecimal != null) {
            val = bigDecimal.toString();
        }
        return Pair.of(stringType, val);
    }
}
