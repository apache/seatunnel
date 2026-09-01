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

import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.sql.zeta.ZetaSQLFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Locale;
import java.util.Random;

public class NumericFunction {
    public static Number abs(List<Object> args) {
        Number arg = (Number) args.get(0);
        if (arg == null) {
            return null;
        }
        if (arg instanceof Byte) {
            byte value = arg.byteValue();
            if (value == Byte.MIN_VALUE) {
                throw absOverflow("TINYINT", value, Byte.MIN_VALUE, Byte.MAX_VALUE);
            }
            return (byte) Math.abs(value);
        }
        if (arg instanceof Short) {
            short value = arg.shortValue();
            if (value == Short.MIN_VALUE) {
                throw absOverflow("SMALLINT", value, Short.MIN_VALUE, Short.MAX_VALUE);
            }
            return (short) Math.abs(value);
        }
        if (arg instanceof Integer) {
            int value = arg.intValue();
            if (value == Integer.MIN_VALUE) {
                throw absOverflow("INT", value, Integer.MIN_VALUE, Integer.MAX_VALUE);
            }
            return Math.abs(value);
        }
        if (arg instanceof Long) {
            long value = arg.longValue();
            if (value == Long.MIN_VALUE) {
                throw absOverflow("BIGINT", value, Long.MIN_VALUE, Long.MAX_VALUE);
            }
            return Math.abs(value);
        }
        if (arg instanceof Float) {
            return Math.abs(arg.floatValue());
        }
        if (arg instanceof Double) {
            return Math.abs(arg.doubleValue());
        }
        if (arg instanceof BigDecimal) {
            return ((BigDecimal) arg).abs();
        }

        throw new TransformException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format(
                        "Unsupported arg type %s of function %s",
                        arg.getClass().getName(), ZetaSQLFunction.ABS));
    }

    public static Double acos(List<Object> args) {
        Number arg = (Number) args.get(0);
        if (arg == null) {
            return null;
        }
        return Math.acos(arg.doubleValue());
    }

    public static Double asin(List<Object> args) {
        Number arg = (Number) args.get(0);
        if (arg == null) {
            return null;
        }
        return Math.asin(arg.doubleValue());
    }

    public static Double atan(List<Object> args) {
        Number arg = (Number) args.get(0);
        if (arg == null) {
            return null;
        }
        return Math.atan(arg.doubleValue());
    }

    public static Double cos(List<Object> args) {
        Number arg = (Number) args.get(0);
        if (arg == null) {
            return null;
        }
        return Math.cos(arg.doubleValue());
    }

    public static Double cosh(List<Object> args) {
        Number arg = (Number) args.get(0);
        if (arg == null) {
            return null;
        }
        return Math.cosh(arg.doubleValue());
    }

    public static Double cot(List<Object> args) {
        Number arg = (Number) args.get(0);
        if (arg == null) {
            return null;
        }
        double d = Math.tan(arg.doubleValue());
        if (d == 0) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION, "Division by zero");
        }
        return 1d / d;
    }

    public static Double sin(List<Object> args) {
        Number arg = (Number) args.get(0);
        if (arg == null) {
            return null;
        }
        return Math.sin(arg.doubleValue());
    }

    public static Double sinh(List<Object> args) {
        Number arg = (Number) args.get(0);
        if (arg == null) {
            return null;
        }
        return Math.sinh(arg.doubleValue());
    }

    public static Double tan(List<Object> args) {
        Number arg = (Number) args.get(0);
        if (arg == null) {
            return null;
        }
        return Math.tan(arg.doubleValue());
    }

    public static Double tanh(List<Object> args) {
        Number arg = (Number) args.get(0);
        if (arg == null) {
            return null;
        }
        return Math.tanh(arg.doubleValue());
    }

    public static Double atan2(List<Object> args) {
        Number arg = (Number) args.get(0);
        if (arg == null) {
            return null;
        }
        Number arg2 = (Number) args.get(1);
        if (arg2 == null) {
            return null;
        }
        return Math.atan2(arg.doubleValue(), arg2.doubleValue());
    }

    /**
     * Converts a numeric value to {@link BigDecimal} without routing it through {@code double}.
     *
     * <p>{@link BigDecimal} values are used as-is and integral types are widened through {@code
     * longValue()}. Reading them through {@code doubleValue()} instead would collapse the value to
     * a {@code double} first, discarding everything beyond ~17 significant digits before any
     * arithmetic runs, which defeats the purpose of the DECIMAL type.
     *
     * <p>Shared by the numeric functions in this class and by the DECIMAL branch of {@code
     * ZetaSQLFunction#executeBinaryExpr}, so that a value is converted the same way regardless of
     * which of the two evaluates it.
     *
     * @param value the numeric value to convert
     * @return the value as an exact BigDecimal
     */
    public static BigDecimal toBigDecimal(Number value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long) {
            return BigDecimal.valueOf(value.longValue());
        }
        // Float/Double have no exact decimal form; valueOf uses the canonical shortest
        // representation, which is the closest thing to the value the user wrote.
        return BigDecimal.valueOf(value.doubleValue());
    }

    public static Number mod(List<Object> args) {
        Number leftValue = (Number) args.get(0);
        if (leftValue == null) {
            return null;
        }
        Number rightValue = (Number) args.get(1);
        if (rightValue == null) {
            return null;
        }
        BigDecimal leftBD = toBigDecimal(leftValue);
        BigDecimal rightBD = toBigDecimal(rightValue);
        if (rightBD.signum() == 0) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION, "Mod by zero");
        }
        BigDecimal[] res = leftBD.divideAndRemainder(rightBD);
        if (rightValue instanceof Integer) {
            return res[1].intValue();
        }
        if (rightValue instanceof Long) {
            return res[1].longValue();
        }
        if (rightValue instanceof Float) {
            return res[1].floatValue();
        }
        if (rightValue instanceof Double) {
            return res[1].doubleValue();
        }
        if (rightValue instanceof BigDecimal) {
            return res[1];
        }
        throw new TransformException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format(
                        "Unsupported arg type %s of function %s",
                        rightValue.getClass().getName(), ZetaSQLFunction.MOD));
    }

    /**
     * Returns the smallest value greater than or equal to the argument.
     *
     * <p>As documented for {@code CEIL}, the result keeps the data type of the argument. Narrowing
     * the result to {@code int} would silently corrupt {@code BIGINT}, {@code DOUBLE} and {@code
     * DECIMAL} inputs whose value does not fit into 32 bits.
     */
    public static Number ceil(List<Object> args) {
        Number v1 = (Number) args.get(0);
        if (v1 == null) {
            return null;
        }
        Number v2 = null;
        if (args.size() >= 2) {
            v2 = (Number) args.get(1);
        }
        return round(v1, v2, RoundingMode.CEILING, ZetaSQLFunction.CEIL);
    }

    private static Number round(Number v1, Number v2, RoundingMode roundingMode, String function) {
        int scale = v2 != null ? v2.intValue() : 0;
        String t = v1.getClass().getSimpleName();
        c:
        // Locale.ROOT: under a Turkish default locale the lowercase 'i's in
        // "BigDecimal" uppercase to '\u0130', so the label would miss its case and
        // DECIMAL rounding would fall into the default branch below.
        switch (t.toUpperCase(Locale.ROOT)) {
            case "BYTE":
            case "INTEGER":
            case "SHORT":
            case "LONG":
                {
                    if (scale < 0) {
                        // Round in BigDecimal and narrow only after checking the result fits.
                        // BigDecimal.longValue() keeps just the low-order 64 bits, and
                        // intValue()/shortValue() truncate the same way, so narrowing an
                        // out-of-range result used to turn a positive value negative.
                        BigDecimal rounded =
                                BigDecimal.valueOf(v1.longValue()).setScale(scale, roundingMode);
                        v1 = convertTo(t, rounded, function);
                    }
                    break;
                }
            case "BIGDECIMAL":
                {
                    // Must not round-trip through double: a DECIMAL carries more digits than a
                    // double can hold, so doubleValue() would silently drop the low-order ones.
                    v1 = ((BigDecimal) v1).setScale(scale, roundingMode);
                    break;
                }
            case "DOUBLE":
            case "FLOAT":
                {
                    l:
                    if (scale == 0) {
                        double d;
                        switch (roundingMode) {
                            case DOWN:
                                d = v1.doubleValue();
                                d = d < 0 ? Math.ceil(d) : Math.floor(d);
                                break;
                            case CEILING:
                                d = Math.ceil(v1.doubleValue());
                                break;
                            case FLOOR:
                                d = Math.floor(v1.doubleValue());
                                break;
                            default:
                                break l;
                        }
                        v1 = t.equals("FLOAT") ? (float) d : d;
                        break c;
                    }
                    BigDecimal bd =
                            BigDecimal.valueOf(v1.doubleValue()).setScale(scale, roundingMode);
                    v1 = t.equals("FLOAT") ? bd.floatValue() : bd.doubleValue();
                    break;
                }
            default:
                // Without this, an unhandled numeric type fell through the switch and was
                // returned unrounded, with no exception and no log line.
                throw new TransformException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        String.format(
                                "Unsupported arg type %s of function %s",
                                v1.getClass().getName(), function));
        }
        return v1;
    }

    /**
     * Narrows an already-rounded integral value back to the argument's own type.
     *
     * @param valueType simple class name of the original argument
     * @param value the rounded value, exact
     * @param function SQL function name, used only for the error message
     * @return the value in the original type
     * @throws TransformException if the value does not fit that type
     */
    private static Number convertTo(String valueType, BigDecimal value, String function) {
        switch (valueType.toUpperCase(Locale.ROOT)) {
            case "BYTE":
                return (byte)
                        checkIntegralRange(
                                value, Byte.MIN_VALUE, Byte.MAX_VALUE, "TINYINT", function);
            case "INTEGER":
                return (int)
                        checkIntegralRange(
                                value, Integer.MIN_VALUE, Integer.MAX_VALUE, "INT", function);
            case "SHORT":
                return (short)
                        checkIntegralRange(
                                value, Short.MIN_VALUE, Short.MAX_VALUE, "SMALLINT", function);
            case "LONG":
                return checkIntegralRange(
                        value, Long.MIN_VALUE, Long.MAX_VALUE, "BIGINT", function);
            default:
                throw new IllegalArgumentException();
        }
    }

    /**
     * Fails loudly when a rounded result cannot be represented in the argument's own type, rather
     * than silently truncating it to a wrong (often negative) value.
     */
    private static long checkIntegralRange(
            BigDecimal value, long min, long max, String sqlType, String function) {
        if (value.compareTo(BigDecimal.valueOf(min)) < 0
                || value.compareTo(BigDecimal.valueOf(max)) > 0) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    String.format(
                            "Function %s produced %s, which is out of range for %s (%s to %s). "
                                    + "Cast the argument to a wider data type to avoid this.",
                            function, value.toPlainString(), sqlType, min, max));
        }
        return value.longValue();
    }

    /** Mirrors the documented ABS contract: the minimum negative value has no absolute value. */
    private static TransformException absOverflow(String sqlType, long value, long min, long max) {
        return new TransformException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format(
                        "Function %s cannot represent the absolute value of %s: %s allows %s to "
                                + "%s, so the result does not fit. Cast the argument to a wider "
                                + "data type to avoid this.",
                        ZetaSQLFunction.ABS, value, sqlType, min, max));
    }

    public static Double exp(List<Object> args) {
        Number v1 = (Number) args.get(0);
        if (v1 == null) {
            return null;
        }
        return Math.exp(v1.doubleValue());
    }

    /**
     * Returns the largest value less than or equal to the argument.
     *
     * <p>As documented for {@code FLOOR}, the result keeps the data type of the argument. See
     * {@link #ceil(List)} for why the result must not be narrowed to {@code int}.
     */
    public static Number floor(List<Object> args) {
        Number v1 = (Number) args.get(0);
        if (v1 == null) {
            return null;
        }
        Number v2 = null;
        if (args.size() >= 2) {
            v2 = (Number) args.get(1);
        }
        return round(v1, v2, RoundingMode.FLOOR, ZetaSQLFunction.FLOOR);
    }

    public static Double ln(List<Object> args) {
        Number v1 = (Number) args.get(0);
        if (v1 == null) {
            return null;
        }
        if (v1.doubleValue() <= 0) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    String.format("Unsupported function LN() argument: %s", v1));
        }
        return Math.log(v1.doubleValue());
    }

    public static Double log(List<Object> args) {
        Number v1 = (Number) args.get(0);
        if (v1 == null) {
            return null;
        }
        if (v1.doubleValue() <= 0) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    String.format("Unsupported function LOG() base: %s", v1));
        }
        Number v2 = (Number) args.get(1);
        if (v2 == null) {
            return null;
        }
        if (v2.doubleValue() <= 0) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    String.format("Unsupported function LOG() argument: %s", v1));
        }
        if (v1.doubleValue() == Math.E) {
            return Math.log(v2.doubleValue());
        } else if (v1.doubleValue() == 10d) {
            return Math.log10(v2.doubleValue());
        } else {
            return Math.log(v2.doubleValue()) / Math.log(v1.doubleValue());
        }
    }

    public static Double log10(List<Object> args) {
        Number v1 = (Number) args.get(0);
        if (v1 == null) {
            return null;
        }
        if (v1.doubleValue() <= 0) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    String.format("Unsupported function LOG10() argument: %s", v1));
        }
        return Math.log10(v1.doubleValue());
    }

    public static Double radians(List<Object> args) {
        Number v1 = (Number) args.get(0);
        if (v1 == null) {
            return null;
        }
        return Math.toRadians(v1.doubleValue());
    }

    public static Double sqrt(List<Object> args) {
        Number v1 = (Number) args.get(0);
        if (v1 == null) {
            return null;
        }
        return Math.sqrt(v1.doubleValue());
    }

    public static Double pi(List<Object> args) {
        return Math.PI;
    }

    public static Double power(List<Object> args) {
        Number v1 = (Number) args.get(0);
        if (v1 == null) {
            return null;
        }
        Number v2 = (Number) args.get(1);
        if (v2 == null) {
            return null;
        }
        return Math.pow(v1.doubleValue(), v2.doubleValue());
    }

    public static Double random(List<Object> args) {
        Random random = new Random();
        if (!args.isEmpty()) {
            Number v1 = (Number) args.get(0);
            if (v1 != null) {
                random.setSeed(v1.intValue());
            }
        }
        return random.nextDouble();
    }

    public static Number round(List<Object> args) {
        Number v1 = (Number) args.get(0);
        if (v1 == null) {
            return null;
        }
        Number v2 = null;
        if (args.size() >= 2) {
            v2 = (Number) args.get(1);
        }
        return round(v1, v2, RoundingMode.HALF_UP, ZetaSQLFunction.ROUND);
    }

    public static Integer sign(List<Object> args) {
        Number v1 = (Number) args.get(0);
        if (v1 == null) {
            return null;
        }
        if (v1 instanceof Byte || v1 instanceof Short) {
            return Integer.signum(v1.intValue());
        }
        if (v1 instanceof Integer) {
            return Integer.signum((Integer) v1);
        }
        if (v1 instanceof Long) {
            return Long.signum((Long) v1);
        }
        if (v1 instanceof Double) {
            double value = (Double) v1;
            return value == 0 || Double.isNaN(value) ? 0 : value < 0 ? -1 : 1;
        }
        if (v1 instanceof Float) {
            float value = (Float) v1;
            return value == 0 || Float.isNaN(value) ? 0 : value < 0 ? -1 : 1;
        }
        if (v1 instanceof BigDecimal) {
            // signum() is exact: doubleValue() underflows to 0.0 for magnitudes below
            // Double.MIN_VALUE, which reported a non-zero decimal as zero.
            return ((BigDecimal) v1).signum();
        }
        throw new TransformException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format(
                        "Unsupported function SIGN() argument type: %s", v1.getClass().getName()));
    }

    public static Number trunc(List<Object> args) {
        Number v1 = (Number) args.get(0);
        if (v1 == null) {
            return null;
        }
        Number v2 = null;
        if (args.size() >= 2) {
            v2 = (Number) args.get(1);
        }
        return round(v1, v2, RoundingMode.DOWN, ZetaSQLFunction.TRUNC);
    }

    public static String trimScale(List<Object> args) {
        Number v1 = (Number) args.get(0);
        if (v1 == null) {
            return null;
        }
        BigDecimal bd;
        if (v1 instanceof BigDecimal) {
            bd = (BigDecimal) v1;
        } else {
            bd = new BigDecimal(v1.toString());
        }
        bd = bd.stripTrailingZeros();
        return bd.toPlainString();
    }
}
