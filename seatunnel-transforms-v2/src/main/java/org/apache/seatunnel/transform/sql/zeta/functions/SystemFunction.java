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
import java.util.Locale;

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

    /**
     * The bounds of an integral SQL target, carried in both forms the range check needs: as
     * primitives for the common truncation path, and as {@link BigInteger} for a {@code DECIMAL} or
     * {@link BigInteger} source. Enum constants so neither form is allocated per row.
     */
    private enum IntegralBounds {
        INT(Integer.MIN_VALUE, Integer.MAX_VALUE),
        LONG(Long.MIN_VALUE, Long.MAX_VALUE);

        /**
         * 2^63 as a double. Long.MAX_VALUE is 2^63-1, which no double can represent, so this is the
         * smallest double magnitude that a narrowing cast to long saturates on.
         */
        private static final double SATURATION_MAGNITUDE = 9.223372036854775808E18;

        private final long min;
        private final long max;
        private final BigInteger minBig;
        private final BigInteger maxBig;

        IntegralBounds(long min, long max) {
            this.min = min;
            this.max = max;
            this.minBig = BigInteger.valueOf(min);
            this.maxBig = BigInteger.valueOf(max);
        }
    }

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
        return (int) narrowToIntegral(value, targetType, IntegralBounds.INT);
    }

    /**
     * Narrows a numeric value to {@code long}, throwing when it cannot be represented. See <a
     * href="https://github.com/apache/seatunnel/issues/12612">#12612</a>.
     *
     * <p>The same shape as {@link #numberToInt}, against the long bounds and through the same
     * helper rather than a long-specific copy of it. {@link Number#longValue()} keeps only the
     * low-order 64 bits of a wide {@link BigDecimal} or {@link BigInteger}, maps {@code NaN} to
     * zero, and saturates an out-of-range {@code double} at {@link Long#MAX_VALUE}, so each of
     * those arrived as a wrong number rather than as a failure.
     *
     * <p>No explicit {@code CAST} reaches this with a value that cannot be represented: {@code
     * CastFunction.LONG_CAST_TYPES} admits only TINYINT, SMALLINT, INT, BIGINT and STRING, the four
     * integral families widen to long exactly, and a STRING goes through {@link Long#parseLong},
     * which reports its own overflow. The reachable route is {@code COALESCE} and {@code IFNULL},
     * whose result type is inferred from the first non-null argument, so a {@code DECIMAL} or
     * {@code DOUBLE} argument is narrowed here against a BIGINT target.
     *
     * @param value the numeric value being converted, never null at the call site
     * @param targetType the SQL type name, used in the error message
     * @return the value as a long
     * @throws TransformException if the value cannot be represented as a long
     */
    private static long numberToLong(Number value, String targetType) {
        return narrowToIntegral(value, targetType, IntegralBounds.LONG);
    }

    /**
     * Truncates a numeric value towards zero and range-checks the result against the target's
     * bounds, reporting the original value in any failure.
     */
    private static long narrowToIntegral(Number value, String targetType, IntegralBounds bounds) {
        if (value instanceof BigDecimal) {
            return fromExact(((BigDecimal) value).toBigInteger(), value, targetType, bounds);
        }
        if (value instanceof BigInteger) {
            return fromExact((BigInteger) value, value, targetType, bounds);
        }
        if (value instanceof Double || value instanceof Float) {
            double d = value.doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                throw outOfRange(value, targetType, bounds);
            }
            // A narrowing cast to long saturates rather than overflowing, so a magnitude at or
            // beyond 2^63 would arrive as Long.MAX_VALUE and pass a long range check: that is how
            // 1e30 previously converted to Long.MAX_VALUE. Rejected here, before the cast.
            // -2^63 is exactly Long.MIN_VALUE, so it is representable and is not rejected.
            if (d >= IntegralBounds.SATURATION_MAGNITUDE
                    || d < -IntegralBounds.SATURATION_MAGNITUDE) {
                throw outOfRange(value, targetType, bounds);
            }
            // Truncate before range-checking, not after: narrowing rounds towards zero, so a
            // fractional value whose truncation fits, such as 2147483647.5 for INT, must still
            // convert. Comparing the raw double against the bounds would reject it.
            return checkRange((long) d, value, targetType, bounds);
        }
        // Byte, Short, Integer and Long widen to long exactly.
        return checkRange(value.longValue(), value, targetType, bounds);
    }

    /** Range-checks an already-integral value, reporting the original in any failure. */
    private static long fromExact(
            BigInteger truncated, Number original, String targetType, IntegralBounds bounds) {
        if (truncated.compareTo(bounds.minBig) < 0 || truncated.compareTo(bounds.maxBig) > 0) {
            throw outOfRange(original, targetType, bounds);
        }
        return truncated.longValue();
    }

    /** Range-checks a value already truncated to a long, reporting the original in any failure. */
    private static long checkRange(
            long truncated, Number original, String targetType, IntegralBounds bounds) {
        if (truncated < bounds.min || truncated > bounds.max) {
            throw outOfRange(original, targetType, bounds);
        }
        return truncated;
    }

    /**
     * Builds the out-of-range failure for a numeric conversion.
     *
     * <p>The wording names the conversion rather than the {@code CAST} keyword, because this is
     * also reached from {@code COALESCE} and {@code IFNULL}, where the user wrote no cast and the
     * target type was inferred. The surrounding expression is supplied by the engine's error
     * wrapper.
     */
    private static TransformException outOfRange(
            Number value, String targetType, IntegralBounds bounds) {
        return new TransformException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format(
                        "Value %s cannot be converted to %s: out of range [%d, %d]",
                        value, targetType, bounds.min, bounds.max));
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
                    return numberToLong((Number) v1, v2);
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
                if (v1 instanceof BigDecimal) {
                    bigDecimal = (BigDecimal) v1;
                } else if (v1 instanceof Float) {
                    // Translate the exact binary value, mirroring CAST semantics in
                    // databases (e.g. MySQL). Float.toString() returns the shortest
                    // round-trip representation and drops the hidden binary digits
                    // (126.752251f -> "126.75225"), which would turn CAST(... AS
                    // DECIMAL(20,10)) into 126.7522500000 instead of the exact
                    // 126.7522506714 (issue #10198).
                    bigDecimal = new BigDecimal((Float) v1);
                } else if (v1 instanceof Double) {
                    // Same reasoning as for Float above.
                    bigDecimal = new BigDecimal((Double) v1);
                } else {
                    bigDecimal = new BigDecimal(v1.toString());
                }
                Integer scale = (Integer) args.get(3);
                // Round half away from zero for every source type: 1.21 -> 1.2,
                // 1.25 -> 1.3, -1.25 -> -1.3. FLOAT and DOUBLE are rounded from their exact
                // binary value, so near a boundary they can differ from the same digits as
                // STRING or DECIMAL (1.15d is 1.1499999..., which gives 1.1 at scale 1).
                return bigDecimal.setScale(scale, RoundingMode.HALF_UP);
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
                    if (Arrays.asList("TRUE", "FALSE")
                            .contains(v1.toString().toUpperCase(Locale.ROOT))) {
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
