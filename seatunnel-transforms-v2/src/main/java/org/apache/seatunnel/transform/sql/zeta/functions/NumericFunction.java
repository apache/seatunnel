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

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.sql.zeta.ZetaSQLFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Random;

public class NumericFunction {
    public static Number abs(List<Object> args) {
        Number arg = (Number) args.get(0);
        if (arg == null) {
            return null;
        }
        if (arg instanceof Integer) {
            return Math.abs(arg.intValue());
        }
        if (arg instanceof Long) {
            return Math.abs(arg.longValue());
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
                CommonErrorCode.UNSUPPORTED_OPERATION,
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
            throw new TransformException(CommonErrorCode.UNSUPPORTED_OPERATION, "Division by zero");
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

    public static Number mod(List<Object> args) {
        Number leftValue = (Number) args.get(0);
        if (leftValue == null) {
            return null;
        }
        Number rightValue = (Number) args.get(1);
        if (rightValue == null) {
            return null;
        }
        if (rightValue.doubleValue() == 0) {
            throw new TransformException(CommonErrorCode.UNSUPPORTED_OPERATION, "Mod by zero");
        }
        BigDecimal leftBD = BigDecimal.valueOf(leftValue.doubleValue());
        BigDecimal rightBD = BigDecimal.valueOf(rightValue.doubleValue());
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
                CommonErrorCode.UNSUPPORTED_OPERATION,
                String.format(
                        "Unsupported arg type %s of function %s",
                        rightValue.getClass().getName(), ZetaSQLFunction.MOD));
    }

    public static Integer ceil(List<Object> args) {
        Number v1 = (Number) args.get(0);
        if (v1 == null) {
            return null;
        }
        Number v2 = null;
        if (args.size() >= 2) {
            v2 = (Number) args.get(1);
        }
        return round(v1, v2, RoundingMode.CEILING).intValue();
    }

    private static Number round(Number v1, Number v2, RoundingMode roundingMode) {
        int scale = v2 != null ? v2.intValue() : 0;
        String t = v1.getClass().getSimpleName();
        c:
        switch (t.toUpperCase()) {
            case "INTEGER":
            case "SHOT":
            case "LONG":
                {
                    if (scale < 0) {
                        long original = v1.longValue();
                        long scaled =
                                BigDecimal.valueOf(original)
                                        .setScale(scale, roundingMode)
                                        .longValue();
                        if (original != scaled) {
                            v1 = convertTo(t, scaled);
                        }
                    }
                    break;
                }
            case "BIGDECIMAL":
                {
                    BigDecimal bd = BigDecimal.valueOf(v1.doubleValue());
                    v1 = bd.setScale(scale, roundingMode);
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
        }
        return v1;
    }

    private static Number convertTo(String valueType, Number column) {
        switch (valueType) {
            case "INTEGER":
                return column.intValue();
            case "SHOT":
                return column.shortValue();
            case "LONG":
                return column.longValue();
            default:
                throw new IllegalArgumentException();
        }
    }

    public static Double exp(List<Object> args) {
        Number v1 = (Number) args.get(0);
        if (v1 == null) {
            return null;
        }
        return Math.exp(v1.doubleValue());
    }

    public static Integer floor(List<Object> args) {
        Number v1 = (Number) args.get(0);
        if (v1 == null) {
            return null;
        }
        Number v2 = null;
        if (args.size() >= 2) {
            v2 = (Number) args.get(1);
        }
        return round(v1, v2, RoundingMode.FLOOR).intValue();
    }

    public static Double ln(List<Object> args) {
        Number v1 = (Number) args.get(0);
        if (v1 == null) {
            return null;
        }
        if (v1.doubleValue() <= 0) {
            throw new TransformException(
                    CommonErrorCode.UNSUPPORTED_OPERATION,
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
                    CommonErrorCode.UNSUPPORTED_OPERATION,
                    String.format("Unsupported function LOG() base: %s", v1));
        }
        Number v2 = (Number) args.get(1);
        if (v2 == null) {
            return null;
        }
        if (v2.doubleValue() <= 0) {
            throw new TransformException(
                    CommonErrorCode.UNSUPPORTED_OPERATION,
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
                    CommonErrorCode.UNSUPPORTED_OPERATION,
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
        if (args.size() >= 1) {
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
        return round(v1, v2, RoundingMode.HALF_UP);
    }

    public static int sign(List<Object> args) {
        Number v1 = (Number) args.get(0);
        if (v1 == null) {
            return 0;
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
            double value = v1.doubleValue();
            return value == 0 || Double.isNaN(value) ? 0 : value < 0 ? -1 : 1;
        }
        throw new TransformException(
                CommonErrorCode.UNSUPPORTED_OPERATION,
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
        return round(v1, v2, RoundingMode.DOWN);
    }
}
