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

import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.transform.exception.TransformException;

import org.apache.commons.collections4.CollectionUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SystemFunction {
    /**
     * Enhanced version of coalesce function that takes a target type parameter. This ensures that
     * the result is always converted to the expected type regardless of which argument is non-null.
     *
     * @param args Function arguments
     * @param targetType The target type that the result should be converted to
     * @return The first non-null value converted to the target type
     */
    public static Object coalesce(List<Object> args, SeaTunnelDataType<?> targetType) {
        Object result = coalesce(args);
        return castAs(result, targetType);
    }

    private static Object coalesce(List<Object> args) {
        for (Object arg : args) {
            if (arg != null) {
                return arg;
            }
        }
        return null;
    }

    public static Object ifnull(List<Object> args, SeaTunnelDataType<?> targetType) {
        if (args.size() != 2) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    String.format("Unsupported function IFNULL() arguments: %s", args));
        }
        return coalesce(args, targetType);
    }

    public static Object nullif(List<Object> args) {
        Object v1 = args.get(0);
        Object v2 = args.get(1);
        if (v1 == null) {
            return null;
        }
        if (v1.equals(v2)) {
            return null;
        }
        return v1;
    }

    public static String[] array(List<Object> args) {
        if (CollectionUtils.isNotEmpty(args)) {
            return args.stream()
                    .map(obj -> obj == null ? null : obj.toString())
                    .toArray(String[]::new);
        }
        return new String[0];
    }

    public static Object castAs(Object arg, SeaTunnelDataType<?> type) {
        final ArrayList<Object> args = new ArrayList<>(4);
        args.add(arg);
        args.add(type.getSqlType().toString());
        if (DecimalType.class.equals(type.getClass())) {
            final DecimalType decimalType = (DecimalType) type;
            args.add(decimalType.getPrecision());
            args.add(decimalType.getScale());
        }
        return castAs(args);
    }

    private static final BigInteger INT_MIN_BIG = BigInteger.valueOf(Integer.MIN_VALUE);
    private static final BigInteger INT_MAX_BIG = BigInteger.valueOf(Integer.MAX_VALUE);

    /**
     * Narrows a numeric value to {@code int}, throwing when it cannot be represented. See <a
     * href="https://github.com/apache/seatunnel/issues/12571">#12571</a>.
     *
     * <p>Each numeric family is truncated towards zero first and the result is range-checked,
     * rather than widening everything through {@link Number#longValue()}, which is itself lossy: it
     * keeps only the low-order 64 bits of a {@link BigDecimal} or {@link BigInteger} and maps
     * {@code NaN} to zero, so the check would inspect an already-corrupted value.
     *
     * <p>Truncation of a fractional source is unchanged, so a value whose truncation fits still
     * converts. {@code NaN} and the infinities are rejected.
     *
     * @param value the numeric value being converted, never null at the call site
     * @param targetType the SQL type name, used in the error message
     * @return the value as an int
     * @throws TransformException if the value cannot be represented as an int
     */
    private static int numberToInt(Number value, String targetType) {
        if (value instanceof BigDecimal) {
            return intFromExact(((BigDecimal) value).toBigInteger(), value, targetType);
        }
        if (value instanceof BigInteger) {
            return intFromExact((BigInteger) value, value, targetType);
        }
        if (value instanceof Double || value instanceof Float) {
            double d = value.doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                throw outOfIntRange(value, targetType);
            }
            // Truncate before range-checking, not after: narrowing a double to long rounds
            // towards zero, so a fractional value whose truncation fits, such as 2147483647.5,
            // must still convert. Comparing the raw double against the bounds would reject it.
            long truncated = (long) d;
            if (truncated < Integer.MIN_VALUE || truncated > Integer.MAX_VALUE) {
                throw outOfIntRange(value, targetType);
            }
            return (int) truncated;
        }
        // Byte, Short, Integer and Long widen to long exactly.
        long widened = value.longValue();
        if (widened < Integer.MIN_VALUE || widened > Integer.MAX_VALUE) {
            throw outOfIntRange(value, targetType);
        }
        return (int) widened;
    }

    /** Range-checks an already-integral value, reporting the original in any failure. */
    private static int intFromExact(BigInteger truncated, Number original, String targetType) {
        if (truncated.compareTo(INT_MIN_BIG) < 0 || truncated.compareTo(INT_MAX_BIG) > 0) {
            throw outOfIntRange(original, targetType);
        }
        return truncated.intValue();
    }

    /**
     * Builds the out-of-range failure for a numeric conversion.
     *
     * <p>The wording names the conversion rather than the {@code CAST} keyword, because this is
     * also reached from {@code COALESCE} and {@code IFNULL}, where the user wrote no cast and the
     * target type was inferred. The surrounding expression is supplied by the engine's error
     * wrapper.
     */
    private static TransformException outOfIntRange(Number value, String targetType) {
        return new TransformException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format(
                        "Value %s cannot be converted to %s: out of range [%d, %d]",
                        value, targetType, Integer.MIN_VALUE, Integer.MAX_VALUE));
    }

    public static Object castAs(List<Object> args) {
        Object v1 = args.get(0);
        String v2 = (String) args.get(1);
        if (v1 == null) {
            return null;
        }
        switch (v2) {
            case "VARCHAR":
            case "STRING":
                return v1.toString();
            case "TINYINT":
                return Byte.parseByte(v1.toString());
            case "SMALLINT":
                return Short.parseShort(v1.toString());
            case "INT":
            case "INTEGER":
                if (v1 instanceof String) {
                    return Integer.parseInt(v1.toString());
                } else if (v1 instanceof Number) {
                    return numberToInt((Number) v1, v2);
                } else {
                    throw new TransformException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                            String.format("Unsupported CAST %s to INTEGER", v1));
                }
            case "BIGINT":
            case "LONG":
                if (v1 instanceof String) {
                    return Long.parseLong(v1.toString());
                } else if (v1 instanceof OffsetDateTime) {
                    return ((OffsetDateTime) v1).toInstant().toEpochMilli();
                } else if (v1 instanceof Number) {
                    return ((Number) v1).longValue();
                } else {
                    throw new TransformException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                            String.format("Unsupported CAST %s to LONG", v1));
                }
            case "BYTE":
                return Byte.parseByte(v1.toString());
            case "BYTES":
            case "BINARY":
                return v1.toString().getBytes(StandardCharsets.UTF_8);
            case "DOUBLE":
                return Double.parseDouble(v1.toString());
            case "FLOAT":
                return Float.parseFloat(v1.toString());
            case "TIMESTAMP":
            case "DATETIME":
                if (v1 instanceof LocalDateTime) {
                    return v1;
                }
                if (v1 instanceof OffsetDateTime) {
                    return ((OffsetDateTime) v1).toLocalDateTime();
                }
                if (v1 instanceof Long) {
                    Instant instant = Instant.ofEpochMilli(((Long) v1).longValue());
                    ZoneId zone = ZoneId.systemDefault();
                    return LocalDateTime.ofInstant(instant, zone);
                }
                throw new TransformException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        String.format("Unsupported CAST AS type: %s", v2));
            case "TIMESTAMP_TZ":
                if (v1 instanceof OffsetDateTime) {
                    return v1;
                }
                if (v1 instanceof LocalDateTime) {
                    return ((LocalDateTime) v1).atOffset(ZoneOffset.UTC);
                }
                if (v1 instanceof Long) {
                    return OffsetDateTime.ofInstant(
                            Instant.ofEpochMilli(((Long) v1).longValue()), ZoneOffset.UTC);
                }
                if (v1 instanceof String) {
                    try {
                        return OffsetDateTime.parse(
                                (String) v1, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                    } catch (DateTimeParseException ignored) {
                        return LocalDateTime.parse(
                                        (String) v1, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                                .atOffset(ZoneOffset.UTC);
                    }
                }
                throw new TransformException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        String.format("Unsupported CAST AS type: %s", v2));
            case "DATE":
                if (v1 instanceof LocalDateTime) {
                    return ((LocalDateTime) v1).toLocalDate();
                }
                if (v1 instanceof OffsetDateTime) {
                    return ((OffsetDateTime) v1).toLocalDate();
                }
                if (v1 instanceof LocalDate) {
                    return v1;
                }
                if (v1 instanceof Integer) {
                    int dateValue = ((Integer) v1).intValue();
                    int year = dateValue / 10000;
                    int month = (dateValue / 100) % 100;
                    int day = dateValue % 100;
                    return LocalDate.of(year, month, day);
                }
                throw new TransformException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        String.format("Unsupported CAST AS type: %s", v2));
            case "TIME":
                if (v1 instanceof LocalDateTime) {
                    return ((LocalDateTime) v1).toLocalTime();
                }
                if (v1 instanceof OffsetDateTime) {
                    return ((OffsetDateTime) v1).toLocalTime();
                }
                if (v1 instanceof LocalTime) {
                    return v1;
                }
                if (v1 instanceof Integer) {
                    int intTime = ((Integer) v1).intValue();
                    int hour = intTime / 10000;
                    int minute = (intTime / 100) % 100;
                    int second = intTime % 100;
                    return LocalTime.of(hour, minute, second);
                }
                throw new TransformException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        String.format("Unsupported CAST AS type: %s", v2));
            case "DECIMAL":
                BigDecimal bigDecimal;
                RoundingMode roundingMode;
                if (v1 instanceof BigDecimal) {
                    bigDecimal = (BigDecimal) v1;
                    roundingMode = RoundingMode.CEILING;
                } else if (v1 instanceof Float) {
                    // Translate the exact binary value, mirroring CAST semantics in
                    // databases (e.g. MySQL). Float.toString() returns the shortest
                    // round-trip representation and drops the hidden binary digits
                    // (126.752251f -> "126.75225"), which would turn CAST(... AS
                    // DECIMAL(20,10)) into 126.7522500000 instead of the exact
                    // 126.7522506714 (issue #10198). Round half away from zero to
                    // match MySQL CAST semantics; CEILING would otherwise push the
                    // hidden binary tail of 0.1f (0.10000000149...) up to 0.11
                    // at scale 2 and silently corrupt ordinary DECIMAL casts.
                    bigDecimal = new BigDecimal((Float) v1);
                    roundingMode = RoundingMode.HALF_UP;
                } else if (v1 instanceof Double) {
                    // Same reasoning as for Float above; the exact binary tail of
                    // 0.1d (0.10000000000000000555...) plus CEILING at scale 2
                    // would also become 0.11 instead of 0.10.
                    bigDecimal = new BigDecimal((Double) v1);
                    roundingMode = RoundingMode.HALF_UP;
                } else {
                    bigDecimal = new BigDecimal(v1.toString());
                    roundingMode = RoundingMode.CEILING;
                }
                Integer scale = (Integer) args.get(3);
                return bigDecimal.setScale(scale, roundingMode);
            case "BOOLEAN":
                if (v1 instanceof Number) {
                    if (Arrays.asList(1, 0).contains(((Number) v1).intValue())) {
                        return ((Number) v1).intValue() == 1;
                    } else {
                        throw new TransformException(
                                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                                String.format("Unsupported CAST AS Boolean: %s", v1));
                    }
                } else if (v1 instanceof String) {
                    if (Arrays.asList("TRUE", "FALSE").contains(v1.toString().toUpperCase())) {
                        return Boolean.parseBoolean(v1.toString());
                    } else {
                        throw new TransformException(
                                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                                String.format("Unsupported CAST AS Boolean: %s", v1));
                    }
                } else if (v1 instanceof Boolean) {
                    return v1;
                }
        }
        throw new TransformException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format("Unsupported CAST AS type: %s", v2));
    }
}
