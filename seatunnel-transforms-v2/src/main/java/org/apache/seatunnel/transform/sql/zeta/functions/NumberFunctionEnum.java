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
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.transform.sql.zeta.visit.CalculateEnum;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Optional;
import java.util.Random;

public enum NumberFunctionEnum {
    /**
     * ABS(numeric)
     *
     * <p>Returns the absolute value of a specified value. The returned value is of the same data
     * type as the parameter.
     *
     * <p>Note that TINYINT, SMALLINT, INT, and BIGINT data types cannot represent absolute values
     * of their minimum negative values, because they have more negative values than positive. For
     * example, for INT data type allowed values are from -2147483648 to 2147483647.
     * ABS(-2147483648) should be 2147483648, but this value is not allowed for this data type. It
     * leads to an exception. To avoid it cast argument of this function to a higher data type.
     *
     * <p>Example:
     */
    ABS {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            SeaTunnelDataType<?> type = types.get(0);
            Object val = objects.get(0);
            if (val != null && NumberUtils.isCreatable(val.toString())) {
                BigDecimal bigDecimal = new BigDecimal(val.toString());
                if (type.getSqlType() == SqlType.INT) {
                    val = Math.abs(bigDecimal.intValue());
                }
                if (type.getSqlType() == SqlType.BIGINT) {
                    val = Math.abs(bigDecimal.longValue());
                }
                if (type.getSqlType() == SqlType.FLOAT) {
                    val = Math.abs(bigDecimal.floatValue());
                }
                if (type.getSqlType() == SqlType.DOUBLE) {
                    val = Math.abs(bigDecimal.doubleValue());
                }
                if (type instanceof DecimalType) {
                    val = bigDecimal.abs();
                }
                return Pair.of(type, val);
            }
            return Pair.of(type, null);
        }
    },
    /**
     * ACOS(numeric)
     *
     * <p>Calculate the arc cosine. See also Java Math.acos. This method returns a double.
     *
     * <p>Example:
     *
     * <p>ACOS(D)
     */
    ACOS {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            BasicType<Double> type = BasicType.DOUBLE_TYPE;
            Object val = objects.get(0);
            if (val != null && NumberUtils.isCreatable(val.toString())) {
                double d = NumberUtils.toDouble(val.toString());
                val = Math.acos(d);
                return Pair.of(type, val);
            }
            return Pair.of(type, null);
        }
    },
    /**
     * ASIN(numeric)
     *
     * <p>Calculate the arc sine. See also Java Math.asin. This method returns a double.
     *
     * <p>Example:
     *
     * <p>ASIN(D)
     */
    ASIN {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            BasicType<Double> type = BasicType.DOUBLE_TYPE;
            Object val = objects.get(0);
            if (val != null && NumberUtils.isCreatable(val.toString())) {
                double d = NumberUtils.toDouble(val.toString());
                val = Math.asin(d);
                return Pair.of(type, val);
            }
            return Pair.of(type, null);
        }
    },
    /**
     * ATAN(numeric)
     *
     * <p>Calculate the arc tangent. See also Java Math.atan. This method returns a double.
     *
     * <p>Example:
     *
     * <p>ATAN(D)
     */
    ATAN {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            BasicType<Double> type = BasicType.DOUBLE_TYPE;
            Object val = objects.get(0);
            if (val != null && NumberUtils.isCreatable(val.toString())) {
                double d = NumberUtils.toDouble(val.toString());
                val = Math.atan(d);
                return Pair.of(type, val);
            }
            return Pair.of(type, null);
        }
    },
    /**
     * COS(numeric)
     *
     * <p>Calculate the trigonometric cosine. See also Java Math.cos. This method returns a double.
     *
     * <p>Example:
     *
     * <p>COS(ANGLE)
     */
    COS {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            BasicType<Double> type = BasicType.DOUBLE_TYPE;
            Object val = objects.get(0);
            if (val != null && NumberUtils.isCreatable(val.toString())) {
                double d = NumberUtils.toDouble(val.toString());
                val = Math.cos(d);
                return Pair.of(type, val);
            }
            return Pair.of(type, null);
        }
    },
    /**
     * COSH(numeric)
     *
     * <p>Calculate the hyperbolic cosine. See also Java Math.cosh. This method returns a double.
     *
     * <p>Example:
     *
     * <p>COSH(X)
     */
    COSH {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            BasicType<Double> type = BasicType.DOUBLE_TYPE;
            Object val = objects.get(0);
            if (val != null && NumberUtils.isCreatable(val.toString())) {
                double d = NumberUtils.toDouble(val.toString());
                val = Math.cosh(d);
                return Pair.of(type, val);
            }
            return Pair.of(type, null);
        }
    },
    /**
     * COT(numeric)
     *
     * <p>Calculate the trigonometric cotangent (1/TAN(ANGLE)). See also Java Math.* functions. This
     * method returns a double.
     *
     * <p>Example:
     *
     * <p>COT(ANGLE)
     */
    COT {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            Object val = objects.get(0);
            BasicType<Double> type = BasicType.DOUBLE_TYPE;
            if (val != null && NumberUtils.isCreatable(val.toString())) {
                double d = NumberUtils.toDouble(val.toString());
                d = Math.tan(d);
                if (d != 0) {
                    val = 1d / d;
                    return Pair.of(type, val);
                }
            }
            return Pair.of(type, null);
        }
    },
    /**
     * SIN(numeric)
     *
     * <p>Calculate the trigonometric sine. See also Java Math.sin. This method returns a double.
     *
     * <p>Example:
     *
     * <p>SIN(ANGLE)
     */
    SIN {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            BasicType<Double> type = BasicType.DOUBLE_TYPE;
            Object val = objects.get(0);
            if (val != null && NumberUtils.isCreatable(val.toString())) {
                double d = NumberUtils.toDouble(val.toString());
                val = Math.sin(d);
                return Pair.of(type, val);
            }
            return Pair.of(type, null);
        }
    },
    /**
     * SINH(numeric)
     *
     * <p>Calculate the hyperbolic sine. See also Java Math.sinh. This method returns a double.
     *
     * <p>Example:
     *
     * <p>SINH(ANGLE)
     */
    SINH {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            BasicType<Double> type = BasicType.DOUBLE_TYPE;
            Object val = objects.get(0);
            if (val != null && NumberUtils.isCreatable(val.toString())) {
                double d = NumberUtils.toDouble(val.toString());
                val = Math.sinh(d);
                return Pair.of(type, val);
            }
            return Pair.of(type, null);
        }
    },
    /**
     * TAN(numeric)
     *
     * <p>Calculate the trigonometric tangent. See also Java Math.tan. This method returns a double.
     *
     * <p>Example:
     *
     * <p>TAN(ANGLE)
     */
    TAN {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            BasicType<Double> type = BasicType.DOUBLE_TYPE;
            Object val = objects.get(0);
            if (val != null && NumberUtils.isCreatable(val.toString())) {
                double d = NumberUtils.toDouble(val.toString());
                val = Math.tan(d);
                return Pair.of(type, val);
            }
            return Pair.of(type, null);
        }
    },
    /**
     * TANH(numeric)
     *
     * <p>Calculate the hyperbolic tangent. See also Java Math.tanh. This method returns a double.
     *
     * <p>Example:
     *
     * <p>TANH(X)
     */
    TANH {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            BasicType<Double> type = BasicType.DOUBLE_TYPE;
            Object val = objects.get(0);
            if (val != null && NumberUtils.isCreatable(val.toString())) {
                double d = NumberUtils.toDouble(val.toString());
                val = Math.tanh(d);
                return Pair.of(type, val);
            }
            return Pair.of(type, null);
        }
    },
    ATAN2 {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 2) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 2");
            }
            Object a = objects.get(0);
            Object b = objects.get(1);
            if (a == null
                    || b == null
                    || !NumberUtils.isCreatable(a.toString())
                    || !NumberUtils.isCreatable(b.toString())) {
                return Pair.of(BasicType.DOUBLE_TYPE, null);
            }
            double d1 = NumberUtils.toDouble(a.toString());
            double d2 = NumberUtils.toDouble(b.toString());

            return Pair.of(BasicType.DOUBLE_TYPE, Math.atan2(d1, d2));
        }
    },
    /**
     * MOD(dividendNumeric, divisorNumeric )
     *
     * <p>The modulus expression.
     *
     * <p>Result has the same type as divisor. Result is NULL if either of arguments is NULL. If
     * divisor is 0, an exception is raised. Result has the same sign as dividend or is equal to 0.
     *
     * <p>Usually arguments should have scale 0, but it isn't required by H2.
     *
     * <p>Example:
     *
     * <p>MOD(A, B)
     */
    MOD {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 2) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 2");
            }
            return CalculateEnum.evaluate(
                    "mod", types.get(0), types.get(1), objects.get(0), objects.get(1));
        }
    },
    /**
     * CEIL | CEILING (numeric)
     *
     * <p>Returns the smallest integer value that is greater than or equal to the argument. This
     * method returns value of the same type as argument, but with scale set to 0 and adjusted
     * precision, if applicable.
     *
     * <p>Example:
     *
     * <p>CEIL(A)
     */
    CEIL {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            BasicType<Integer> type = BasicType.INT_TYPE;
            Object val = objects.get(0);
            return FunctionUtils.round(val, null, type, RoundingMode.CEILING);
        }
    },
    CEILING {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            return CEIL.execute(types, objects);
        }
    },
    /**
     * EXP(numeric)
     *
     * <p>See also Java Math.exp. This method returns a double.
     *
     * <p>Example:
     *
     * <p>EXP(A)
     */
    EXP {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            BasicType<Double> type = BasicType.DOUBLE_TYPE;
            Object val = objects.get(0);
            if (val != null && NumberUtils.isCreatable(val.toString())) {
                double d = NumberUtils.toDouble(val.toString());
                val = Math.exp(d);
                return Pair.of(type, val);
            }
            return Pair.of(type, null);
        }
    },
    /**
     * FLOOR(numeric)
     *
     * <p>Returns the largest integer value that is less than or equal to the argument. This method
     * returns value of the same type as argument, but with scale set to 0 and adjusted precision,
     * if applicable.
     *
     * <p>Example:
     *
     * <p>FLOOR(A)
     */
    FLOOR {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            BasicType<Integer> type = BasicType.INT_TYPE;
            Object val = objects.get(0);
            return FunctionUtils.round(val, null, type, RoundingMode.FLOOR);
        }
    },
    /**
     * LN(numeric)
     *
     * <p>Calculates the natural (base e) logarithm as a double value. Argument must be a positive
     * numeric value.
     *
     * <p>Example:
     *
     * <p>LN(A)
     */
    LN {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            BasicType<Double> type = BasicType.DOUBLE_TYPE;
            Object val = objects.get(0);
            if (val != null && NumberUtils.isCreatable(val.toString())) {
                double d = NumberUtils.toDouble(val.toString());
                if (d > 0) {
                    val = Math.log(d);
                    return Pair.of(type, val);
                }
            }
            return Pair.of(type, null);
        }
    },
    /**
     * LOG(baseNumeric, numeric)
     *
     * <p>Calculates the logarithm with specified base as a double value. Argument and base must be
     * positive numeric values. Base cannot be equal to 1.
     *
     * <p>The default base is e (natural logarithm), in the PostgresSQL mode the default base is
     * base 10. In MSSQLServer mode the optional base is specified after the argument.
     *
     * <p>Single-argument variant of LOG function is deprecated, use LN or LOG10 instead.
     *
     * <p>Example:
     *
     * <p>LOG(2, A)
     */
    LOG {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 2) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 2");
            }
            Object val = null;
            double a =
                    Optional.ofNullable(objects.get(0))
                            .map(item -> NumberUtils.toDouble(item.toString()))
                            .orElse(0d);
            double b =
                    Optional.ofNullable(objects.get(1))
                            .map(item -> NumberUtils.toDouble(item.toString()))
                            .orElse(0d);
            if (a > 0 && b > 0) {
                if (a == Math.E) {
                    val = Math.log(b);
                } else if (a == 10) {
                    val = Math.log10(b);
                } else {
                    val = Math.log(b) / Math.log(a);
                }
            }
            return Pair.of(BasicType.DOUBLE_TYPE, val);
        }
    },
    LOG10 {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 2");
            }
            BasicType<Double> type = BasicType.DOUBLE_TYPE;
            Object val = objects.get(0);
            if (val != null && NumberUtils.isCreatable(val.toString())) {
                double d = NumberUtils.toDouble(val.toString());
                if (d > 0) {
                    val = Math.log10(d);
                    return Pair.of(type, val);
                }
            }
            return Pair.of(type, null);
        }
    },
    /**
     * RADIANS(numeric)
     *
     * <p>See also Java Math.toRadians. This method returns a double.
     *
     * <p>Example:
     *
     * <p>RADIANS(A)
     */
    RADIANS {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            BasicType<Double> type = BasicType.DOUBLE_TYPE;
            Object val = objects.get(0);
            if (val != null && NumberUtils.isCreatable(val.toString())) {
                double d = NumberUtils.toDouble(val.toString());
                val = Math.toRadians(d);
                return Pair.of(type, val);
            }
            return Pair.of(type, null);
        }
    },
    /**
     * SQRT(numeric)
     *
     * <p>See also Java Math.sqrt. This method returns a double.
     *
     * <p>Example:
     *
     * <p>SQRT(A)
     */
    SQRT {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            BasicType<Double> type = BasicType.DOUBLE_TYPE;
            Object val = objects.get(0);
            if (val != null && NumberUtils.isCreatable(val.toString())) {
                double d = NumberUtils.toDouble(val.toString());
                val = Math.sqrt(d);
                return Pair.of(type, val);
            }
            return Pair.of(type, null);
        }
    },
    /**
     * PI()
     *
     * <p>See also Java Math.PI. This method returns a double.
     *
     * <p>Example:
     *
     * <p>PI()
     */
    PI {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 0) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 0");
            }
            return Pair.of(BasicType.DOUBLE_TYPE, Math.PI);
        }
    },
    /**
     * POWER(numeric, numeric)
     *
     * <p>See also Java Math.pow. This method returns a double.
     *
     * <p>Example:
     *
     * <p>POWER(A, B)
     */
    POWER {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 2) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 2");
            }
            Object val;
            Object a = objects.get(0);
            Object b = objects.get(1);

            if (a == null
                    || b == null
                    || !NumberUtils.isCreatable(a.toString())
                    || !NumberUtils.isCreatable(b.toString())) {
                return Pair.of(BasicType.DOUBLE_TYPE, null);
            }
            double d1 = NumberUtils.toDouble(a.toString());
            double d2 = NumberUtils.toDouble(b.toString());
            val = Math.pow(d1, d2);
            return Pair.of(BasicType.DOUBLE_TYPE, val);
        }
    },
    /**
     * RAND | RANDOM([ int ])
     *
     * <p>Calling The " +name()+ " function without parameter returns the next a pseudo random
     * number. Calling it with a parameter seeds the session's random number generator. This method
     * returns a double between 0 (including) and 1 (excluding).
     *
     * <p>Example:
     *
     * <p>RAND()
     */
    RAND {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() > 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require between 0 and 1");
            }
            Random random = new Random();
            if (objects.size() == 1) {
                Object p2 = objects.get(0);
                if (p2 != null && NumberUtils.isCreatable(p2.toString())) {
                    int seed = NumberUtils.toInt(p2.toString());
                    random.setSeed(seed);
                }
            }
            return Pair.of(BasicType.DOUBLE_TYPE, random.nextDouble());
        }
    },
    /**
     * ROUND(numeric[, digitsInt])
     *
     * <p>Rounds to a number of fractional digits. This method returns value of the same type as
     * argument, but with adjusted precision and scale, if applicable.
     *
     * <p>Example:
     *
     * <p>ROUND(N, 2)
     */
    RANDOM {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            return RAND.execute(types, objects);
        }
    },
    ROUND {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() < 1 || objects.size() > 2) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require between 1 and 2");
            }
            SeaTunnelDataType<?> type = types.get(0);
            Object p1 = objects.get(0);
            Object p2 = null;
            if (objects.size() == 2) {
                p2 = objects.get(1);
            }
            return FunctionUtils.round(p1, p2, type, RoundingMode.HALF_UP);
        }
    },
    /**
     * SIGN(numeric)
     *
     * <p>Returns -1 if the value is smaller than 0, 0 if zero or NaN, and otherwise 1.
     *
     * <p>Example:
     *
     * <p>SIGN(N)
     */
    SIGN {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 1");
            }
            SeaTunnelDataType<?> type = BasicType.INT_TYPE;
            Object val = objects.get(0);
            if (val != null && NumberUtils.isCreatable(val.toString())) {
                BigDecimal bigDecimal = new BigDecimal(val.toString());
                if (type.getSqlType() == SqlType.INT) {
                    val = Integer.signum(bigDecimal.intValue());
                }
                if (type.getSqlType() == SqlType.BIGINT) {
                    val = Long.signum(bigDecimal.longValue());
                }
                if (type.getSqlType() == SqlType.FLOAT) {
                    float num = bigDecimal.floatValue();
                    val = num == 0 || Float.isNaN(num) ? 0 : num < 0 ? -1 : 1;
                }
                if (type.getSqlType() == SqlType.DOUBLE) {
                    double num = bigDecimal.doubleValue();
                    val = num == 0 || Double.isNaN(num) ? 0 : num < 0 ? -1 : 1;
                }
                if (type instanceof DecimalType) {
                    double num = bigDecimal.doubleValue();
                    num = num == 0 || Double.isNaN(num) ? 0 : num < 0 ? -1 : 1;
                    val = new BigDecimal(num);
                }
            }
            return Pair.of(type, val);
        }
    },
    /**
     * TRUNC | TRUNCATE(numeric[, digitsInt])
     *
     * <p>When a numeric argument is specified, truncates it to a number of digits (to the next
     * value closer to 0) and returns value of the same type as argument, but with adjusted
     * precision and scale, if applicable.
     *
     * <p>Example:
     *
     * <p>TRUNC(N, 2)
     */
    TRUNC {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() < 1 || objects.size() > 2) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require between 1 and 2");
            }
            SeaTunnelDataType<?> type = types.get(0);
            Object p1 = objects.get(0);
            Object p2 = null;
            if (objects.size() == 2) {
                p2 = objects.get(1);
            }
            return FunctionUtils.round(p1, p2, type, RoundingMode.DOWN);
        }
    };

    public abstract Pair<SeaTunnelDataType<?>, Object> execute(
            List<SeaTunnelDataType<?>> types, List<Object> objects);

    public static Pair<SeaTunnelDataType<?>, Object> execute(
            String funcName, List<SeaTunnelDataType<?>> types, List<Object> objects) {
        for (NumberFunctionEnum value : values()) {
            if (value.name().equals(funcName.toUpperCase())) {
                return value.execute(types, objects);
            }
        }
        return null;
    }
}
