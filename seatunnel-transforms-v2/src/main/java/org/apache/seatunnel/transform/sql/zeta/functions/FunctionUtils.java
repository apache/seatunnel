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
import org.apache.seatunnel.api.table.type.SqlType;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalField;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

public final class FunctionUtils {

    // region common function
    public static String convertToString(Object origin, String defaultValue) {
        return Optional.ofNullable(origin).map(Object::toString).orElse(defaultValue);
    }
    // endregion

    // region string function
    public static final byte[] SOUNDEX_INDEX =
            "71237128722455712623718272\000\000\000\000\000\00071237128722455712623718272"
                    .getBytes(StandardCharsets.ISO_8859_1);

    public static byte[] getSoundex(String s) {
        byte[] chars = {'0', '0', '0', '0'};
        byte lastDigit = '0';
        for (int i = 0, j = 0, l = s.length(); i < l && j < 4; i++) {
            char c = s.charAt(i);
            if (c >= 'A' && c <= 'z') {
                byte newDigit = SOUNDEX_INDEX[c - 'A'];
                if (newDigit != 0) {
                    if (j == 0) {
                        chars[j++] = (byte) (c & 0xdf); // Converts a-z to A-Z
                        lastDigit = newDigit;
                    } else if (newDigit <= '6') {
                        if (newDigit != lastDigit) {
                            chars[j++] = lastDigit = newDigit;
                        }
                    } else if (newDigit == '7') {
                        lastDigit = newDigit;
                    }
                }
            }
        }
        return chars;
    }

    public static Pair<SeaTunnelDataType<?>, Object> lowerOrUpper(
            List<SeaTunnelDataType<?>> types, List<Object> objects, boolean isLower) {
        if (objects.size() != 1) {
            throw new IllegalArgumentException("The function arguments size require eq 1");
        }

        Object val = objects.get(0);
        if (val != null) {
            String origin = val.toString();
            if (isLower) {
                val = StringUtils.lowerCase(origin);
            } else {
                val = StringUtils.upperCase(origin);
            }
        }
        return Pair.of(BasicType.STRING_TYPE, val);
    }

    public static Pair<SeaTunnelDataType<?>, Object> positionCommon(
            List<SeaTunnelDataType<?>> types, List<Object> objects, boolean isInstr) {
        if (objects.size() < 2 || objects.size() > 3) {
            throw new IllegalArgumentException(
                    "The function arguments size require between 2 and 3");
        }

        // Avoiding null pointer exceptions
        String origin = FunctionUtils.convertToString(objects.get(1), "");
        String search = FunctionUtils.convertToString(objects.get(0), "");

        if (isInstr) {
            String temp = origin;
            origin = search;
            search = temp;
        }

        int start = 0;
        if (objects.size() == 3) {
            start =
                    Optional.ofNullable(objects.get(2))
                            .map(item -> NumberUtils.toInt(item.toString()))
                            .orElse(0);
        }
        int val;
        if (start < 0) {
            val = StringUtils.lastIndexOf(origin, search, origin.length() + start);
        } else {
            val = StringUtils.indexOf(origin, search, start);
        }

        // When search="", return 1, expected return 0
        if (StringUtils.isNotEmpty(search)) {
            ++val;
        }

        return Pair.of(BasicType.INT_TYPE, val);
    }

    public static Pair<SeaTunnelDataType<?>, Object> leftOrRight(
            List<SeaTunnelDataType<?>> types, List<Object> objects, boolean isLeft) {
        if (objects.size() != 2) {
            throw new IllegalArgumentException("The function arguments size require eq 2");
        }
        Object val = objects.get(0);
        if (val != null) {
            String origin = val.toString();
            int len =
                    Optional.ofNullable(objects.get(1))
                            .map(item -> NumberUtils.toInt(item.toString()))
                            .orElse(0);
            if (isLeft) {
                val = StringUtils.left(origin, len);
            } else {
                val = StringUtils.right(origin, len);
            }
        }
        return Pair.of(BasicType.STRING_TYPE, val);
    }

    public static int makeRegexpFlags(String stringFlags) {
        int flags = Pattern.UNICODE_CASE;
        if (stringFlags != null) {
            for (int i = 0; i < stringFlags.length(); ++i) {
                switch (stringFlags.charAt(i)) {
                    case 'i':
                        flags |= Pattern.CASE_INSENSITIVE;
                        break;
                    case 'c':
                        flags &= ~Pattern.CASE_INSENSITIVE;
                        break;
                    case 'n':
                        flags |= Pattern.DOTALL;
                        break;
                    case 'm':
                        flags |= Pattern.MULTILINE;
                        break;
                    case 'g':
                        break;
                        // $FALL-THROUGH$
                    default:
                        throw new IllegalArgumentException(
                                String.format(
                                        "The function arguments regexpMode %s is not supported",
                                        flags));
                }
            }
        }
        return flags;
    }

    public static Pair<SeaTunnelDataType<?>, Object> trim(
            List<SeaTunnelDataType<?>> types,
            List<Object> objects,
            boolean leading,
            boolean trailing) {
        if (objects.size() < 1 || objects.size() > 2) {
            throw new IllegalArgumentException(
                    "The function arguments size require between 1 and 2");
        }
        Object val = objects.get(0);
        if (val != null) {
            String origin = val.toString();
            String trimStr = " ";
            if (objects.size() == 2) {
                trimStr = FunctionUtils.convertToString(objects.get(1), trimStr);
                trimStr = StringUtils.substring(trimStr, 0, 1);
            }
            if (leading) {
                origin = StringUtils.stripStart(origin, trimStr);
            }
            if (trailing) {
                origin = StringUtils.stripEnd(origin, trimStr);
            }
            val = origin;
        }

        return Pair.of(BasicType.STRING_TYPE, val);
    }

    public static Pair<SeaTunnelDataType<?>, Object> pad(
            List<SeaTunnelDataType<?>> types, List<Object> objects, boolean isLeft) {
        if (objects.size() < 2 || objects.size() > 3) {
            throw new IllegalArgumentException(
                    "The function arguments size require between 2 and 3");
        }

        Object val = objects.get(0);
        if (val != null) {
            String origin = val.toString();
            int size =
                    Optional.ofNullable(objects.get(1))
                            .map(item -> NumberUtils.toInt(item.toString()))
                            .orElse(0);
            String padStr = " ";
            if (objects.size() == 3) {
                padStr = FunctionUtils.convertToString(objects.get(2), padStr);
            }

            if (isLeft) {
                val = StringUtils.leftPad(origin, size, padStr);
            } else {
                val = StringUtils.rightPad(origin, size, padStr);
            }
        }
        return Pair.of(BasicType.STRING_TYPE, val);
    }
    // endregion

    // region number function
    public static Pair<SeaTunnelDataType<?>, Object> round(
            Object p1, Object p2, SeaTunnelDataType<?> type, RoundingMode mode) {
        Object val = null;
        if (p1 != null && NumberUtils.isCreatable(p1.toString())) {
            BigDecimal bigDecimal = new BigDecimal(p1.toString());
            int scale = 0;
            int n =
                    Optional.ofNullable(p2)
                            .map(item -> NumberUtils.toInt(item.toString()))
                            .orElse(0);
            scale = Math.max(n, scale);
            if (type.getSqlType() == SqlType.INT) {
                val = bigDecimal.setScale(scale, mode).intValue();
            }
            if (type.getSqlType() == SqlType.BIGINT) {
                val = bigDecimal.setScale(scale, mode).longValue();
            }
            if (type.getSqlType() == SqlType.FLOAT) {
                val = bigDecimal.setScale(scale, mode).floatValue();
            }
            if (type.getSqlType() == SqlType.DOUBLE) {
                val = bigDecimal.setScale(scale, mode).doubleValue();
            }
            if (type instanceof DecimalType) {
                val = bigDecimal.setScale(scale, mode);
            }
        }
        return Pair.of(type, val);
    }
    // endregion

    // region date time function
    private static final Map<Integer, DateTimeFormatter> dateTimeFormatterMap =
            new HashMap<Integer, DateTimeFormatter>() {
                {
                    put("yyyy-MM-dd HH".length(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH"));
                    put(
                            "yyyy-MM-dd HH:mm".length(),
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
                    put(
                            "yyyyMMdd HH:mm:ss".length(),
                            DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss"));
                    put(
                            "yyyy-MM-dd HH:mm:ss".length(),
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    put(
                            "yyyy-MM-dd HH:mm:ss.S".length(),
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S"));
                    put(
                            "yyyy-MM-dd HH:mm:ss.SS".length(),
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SS"));
                    put(
                            "yyyy-MM-dd HH:mm:ss.SSS".length(),
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
                    put(
                            "yyyy-MM-dd HH:mm:ss.SSSS".length(),
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSS"));
                    put(
                            "yyyy-MM-dd HH:mm:ss.SSSSSS".length(),
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"));
                }
            };

    public static LocalDateTime parseDate(String fieldValue) {
        // handle strings of timestamp type
        if (fieldValue == null || fieldValue.isEmpty()) {
            return null;
        }

        if (NumberUtils.isDigits(fieldValue)) {
            int fieldValueLength = fieldValue.length();
            if (fieldValueLength == 10 || fieldValueLength == 13) {
                long ts = Long.parseLong(fieldValue);
                ts = fieldValueLength == 10 ? ts * 1000 : ts;
                return LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault());
            }
        }

        String formatDate = fieldValue.replace("T", " ");
        if (fieldValue.length() == "yyyyMMdd".length()
                || fieldValue.length() == "yyyy-MM-dd".length()) {
            formatDate = fieldValue + " 00:00:00";
        }
        DateTimeFormatter dateTimeFormatter = dateTimeFormatterMap.get(formatDate.length());
        if (dateTimeFormatter != null) {
            try {
                return LocalDateTime.parse(formatDate, dateTimeFormatter);
            } catch (Exception e) {
                // no op
            }
        }
        return null;
    }

    public static Object convertDateTimeByType(
            SeaTunnelDataType<?> type, LocalDateTime localDateTime) {
        if (LocalTimeType.LOCAL_DATE_TYPE.equals(type)) {
            return localDateTime.toLocalDate();
        }
        if (LocalTimeType.LOCAL_TIME_TYPE.equals(type)) {
            return localDateTime.toLocalTime();
        }
        if (SqlType.STRING.equals(type.getSqlType())) {
            return localDateTime.toString();
        }

        return localDateTime;
    }

    public static ChronoUnit getDatetimeField(String datetimeField) {
        ChronoUnit chronoUnit;
        switch (datetimeField.toUpperCase()) {
            case "YEAR":
                chronoUnit = ChronoUnit.YEARS;
                break;
            case "MONTH":
                chronoUnit = ChronoUnit.MONTHS;
                break;
            case "WEEK":
                chronoUnit = ChronoUnit.WEEKS;
                break;
            case "HOUR":
                chronoUnit = ChronoUnit.HOURS;
                break;
            case "MINUTE":
                chronoUnit = ChronoUnit.MINUTES;
                break;
            case "SECOND":
                chronoUnit = ChronoUnit.SECONDS;
                break;
            case "MILLISECOND":
                chronoUnit = ChronoUnit.MILLIS;
                break;
            case "DAY":
            default:
                chronoUnit = ChronoUnit.DAYS;
                break;
        }
        return chronoUnit;
    }

    public static Pair<SeaTunnelDataType<?>, Object> getDaysByName(
            List<SeaTunnelDataType<?>> types, List<Object> objects, String name) {
        SeaTunnelDataType<?> type = BasicType.INT_TYPE;
        LocalDateTime dateTime = LocalDateTime.now();
        if (objects.size() == 1) {
            Object val = objects.get(0);
            if (val != null) {
                dateTime = FunctionUtils.parseDate(val.toString());
            }
        }
        if (dateTime != null) {
            int val = -1;
            TemporalField temporalField = getTemporalField(name);
            if (temporalField != null) {
                val = dateTime.get(temporalField);
            }
            return Pair.of(type, val);
        }
        return Pair.of(type, null);
    }

    public static Pair<SeaTunnelDataType<?>, Object> getDayName(
            List<SeaTunnelDataType<?>> types, List<Object> objects, String name) {
        SeaTunnelDataType<?> type = BasicType.STRING_TYPE;
        LocalDateTime dateTime = LocalDateTime.now();
        if (objects.size() == 1) {
            Object val = objects.get(0);
            if (val != null) {
                dateTime = FunctionUtils.parseDate(val.toString());
            }
        }
        if (dateTime != null) {
            if ("DAYNAME".equals(name)) {
                String val = dateTime.getDayOfWeek().toString();
                return Pair.of(type, val);
            }
            if ("MONTHNAME".equals(name)) {
                String val = dateTime.getMonth().getDisplayName(TextStyle.FULL, Locale.ENGLISH);
                return Pair.of(type, val);
            }
        }
        return Pair.of(type, null);
    }

    public static TemporalField getTemporalField(String name) {
        TemporalField temporalField;

        switch (name.toUpperCase()) {
            case "YEAR":
                temporalField = ChronoField.YEAR;
                break;
            case "MONTH":
                temporalField = ChronoField.MONTH_OF_YEAR;
                break;
            case "DAY":
                temporalField = ChronoField.DAY_OF_MONTH;
                break;
            case "HOUR":
                temporalField = ChronoField.HOUR_OF_DAY;
                break;
            case "MINUTE":
                temporalField = ChronoField.MINUTE_OF_HOUR;
                break;
            case "SECOND":
                temporalField = ChronoField.SECOND_OF_MINUTE;
                break;
            case "MILLISECOND":
                temporalField = ChronoField.MILLI_OF_SECOND;
                break;
            case "DAYOFWEEK":
                temporalField = ChronoField.DAY_OF_WEEK;
                break;
            case "DAYOFYEAR":
                temporalField = ChronoField.DAY_OF_YEAR;
                break;
            case "WEEKOFYEAR":
                temporalField = ChronoField.ALIGNED_WEEK_OF_YEAR;
                break;
            default:
                temporalField = null;
        }

        return temporalField;
    }
    // endregion

    // region system function
    public static Pair<SeaTunnelDataType<?>, Object> getNotNullFirst(
            List<SeaTunnelDataType<?>> types, List<Object> objects) {
        SeaTunnelDataType<?> type = types.get(0);
        Object val = null;
        for (int i = 0; i < objects.size(); i++) {
            val = objects.get(i);
            if (val != null) {
                type = types.get(i);
                break;
            }
        }
        return Pair.of(type, val);
    }
    // endregion

}
