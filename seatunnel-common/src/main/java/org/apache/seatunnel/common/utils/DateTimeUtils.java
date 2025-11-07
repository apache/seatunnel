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

package org.apache.seatunnel.common.utils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;

public class DateTimeUtils {

    private static final Map<Formatter, DateTimeFormatter> FORMATTER_MAP =
            new HashMap<Formatter, DateTimeFormatter>();

    static {
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS_SSSSSS,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_SSSSSS.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS_SPOT,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_SPOT.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS_SLASH,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_SLASH.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_M_D_HH_MM_SS_SLASH,
                DateTimeFormatter.ofPattern(Formatter.YYYY_M_D_HH_MM_SS_SLASH.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_M_D_HH_MM_SS_ISO8601,
                DateTimeFormatter.ofPattern(Formatter.YYYY_M_D_HH_MM_SS_ISO8601.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_M_D_HH_MM_SLASH,
                DateTimeFormatter.ofPattern(Formatter.YYYY_M_D_HH_MM_SLASH.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_M_D_HH_MM_ISO8601,
                DateTimeFormatter.ofPattern(Formatter.YYYY_M_D_HH_MM_ISO8601.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS_NO_SPLIT,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_NO_SPLIT.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS_ISO8601,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_ISO8601.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS_SSS_ISO8601,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_SSS_ISO8601.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS_SSSSSS_ISO8601,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_SSSSSS_ISO8601.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS_SSSSSSSSS_ISO8601,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_SSSSSSSSS_ISO8601.value));
    }

    // if the datatime string length is 17, find the DateTimeFormatter from this map
    public static final Map<Pattern, DateTimeFormatter> YYYY_M_D_HH_MM_SS_17_FORMATTER_MAP =
            new LinkedHashMap<>();

    // if the datatime string length is 15, find the DateTimeFormatter from this map
    public static final Map<Pattern, DateTimeFormatter> YYYY_M_D_HH_MM_15_FORMATTER_MAP =
            new LinkedHashMap<>();

    // all Pattern in this set
    public static Set<Map.Entry<Pattern, DateTimeFormatter>>
            YYYY_M_D_HH_MM_SS_17_FORMATTER_MAP_ENTRY_SET = new LinkedHashSet<>();

    // all Pattern in this set
    public static Set<Map.Entry<Pattern, DateTimeFormatter>>
            YYYY_M_D_HH_MM_15_FORMATTER_MAP_ENTRY_SET = new LinkedHashSet<>();

    // if the datatime string length is 19, find the DateTimeFormatter from this map
    public static final Map<Pattern, DateTimeFormatter> YYYY_MM_DD_HH_MM_SS_19_FORMATTER_MAP =
            new LinkedHashMap<>();

    public static Set<Map.Entry<Pattern, DateTimeFormatter>>
            YYYY_MM_DD_HH_MM_SS_19_FORMATTER_MAP_ENTRY_SET = new LinkedHashSet<>();

    // if the datatime string length bigger than 19, find the DateTimeFormatter from this map
    public static final Map<Pattern, DateTimeFormatter> YYYY_MM_DD_HH_MM_SS_M19_FORMATTER_MAP =
            new LinkedHashMap<>();
    public static Set<Map.Entry<Pattern, DateTimeFormatter>>
            YYYY_MM_DD_HH_MM_SS_M19_FORMATTER_MAP_ENTRY_SET = new LinkedHashSet<>();

    // if the datatime string length is 14, use this formatter
    public static final DateTimeFormatter YYYY_MM_DD_HH_MM_SS_14_FORMATTER =
            DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_NO_SPLIT.value);

    static {
        YYYY_MM_DD_HH_MM_SS_19_FORMATTER_MAP.put(
                Pattern.compile("\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}"),
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS.value));

        YYYY_MM_DD_HH_MM_SS_M19_FORMATTER_MAP.put(
                Pattern.compile("\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}.*"),
                new DateTimeFormatterBuilder()
                        .parseCaseInsensitive()
                        .append(DateTimeFormatter.ISO_LOCAL_DATE)
                        .appendLiteral(' ')
                        .append(DateTimeFormatter.ISO_LOCAL_TIME)
                        .toFormatter());

        YYYY_MM_DD_HH_MM_SS_19_FORMATTER_MAP.put(
                Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}"),
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_ISO8601.value));

        YYYY_MM_DD_HH_MM_SS_M19_FORMATTER_MAP.put(
                Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}.*"),
                DateTimeFormatter.ISO_LOCAL_DATE_TIME);

        YYYY_MM_DD_HH_MM_SS_19_FORMATTER_MAP.put(
                Pattern.compile("\\d{4}/\\d{2}/\\d{2}\\s\\d{2}:\\d{2}:\\d{2}"),
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_SLASH.value));

        YYYY_M_D_HH_MM_15_FORMATTER_MAP.put(
                Pattern.compile("\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{2}:\\d{2}"),
                DateTimeFormatter.ofPattern(Formatter.YYYY_M_D_HH_MM_SLASH.value));

        YYYY_M_D_HH_MM_15_FORMATTER_MAP.put(
                Pattern.compile("\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{2}:\\d{2}"),
                DateTimeFormatter.ofPattern(Formatter.YYYY_M_D_HH_MM_ISO8601.value));

        YYYY_M_D_HH_MM_SS_17_FORMATTER_MAP.put(
                Pattern.compile("\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{2}:\\d{2}:\\d{2}"),
                DateTimeFormatter.ofPattern(Formatter.YYYY_M_D_HH_MM_SS_SLASH.value));

        YYYY_M_D_HH_MM_SS_17_FORMATTER_MAP.put(
                Pattern.compile("\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{2}:\\d{2}:\\d{2}"),
                DateTimeFormatter.ofPattern(Formatter.YYYY_M_D_HH_MM_SS_ISO8601.value));

        YYYY_MM_DD_HH_MM_SS_M19_FORMATTER_MAP.put(
                Pattern.compile("\\d{4}/\\d{2}/\\d{2}\\s\\d{2}:\\d{2}.*"),
                new DateTimeFormatterBuilder()
                        .parseCaseInsensitive()
                        .append(
                                new DateTimeFormatterBuilder()
                                        .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
                                        .appendLiteral('/')
                                        .appendValue(MONTH_OF_YEAR, 2)
                                        .appendLiteral('/')
                                        .appendValue(DAY_OF_MONTH, 2)
                                        .toFormatter())
                        .appendLiteral(' ')
                        .append(DateTimeFormatter.ISO_LOCAL_TIME)
                        .toFormatter());

        YYYY_MM_DD_HH_MM_SS_19_FORMATTER_MAP.put(
                Pattern.compile("\\d{4}\\.\\d{2}\\.\\d{2}\\s\\d{2}:\\d{2}:\\d{2}"),
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_SPOT.value));

        YYYY_MM_DD_HH_MM_SS_M19_FORMATTER_MAP.put(
                Pattern.compile("\\d{4}\\.\\d{2}\\.\\d{2}\\s\\d{2}:\\d{2}.*"),
                new DateTimeFormatterBuilder()
                        .parseCaseInsensitive()
                        .append(
                                new DateTimeFormatterBuilder()
                                        .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
                                        .appendLiteral('.')
                                        .appendValue(MONTH_OF_YEAR, 2)
                                        .appendLiteral('.')
                                        .appendValue(DAY_OF_MONTH, 2)
                                        .toFormatter())
                        .appendLiteral(' ')
                        .append(DateTimeFormatter.ISO_LOCAL_TIME)
                        .toFormatter());

        YYYY_MM_DD_HH_MM_SS_M19_FORMATTER_MAP.put(
                Pattern.compile("\\d{4}年\\d{2}月\\d{2}日\\s\\d{2}时\\d{2}分\\d{2}秒"),
                DateTimeFormatter.ofPattern("yyyy年MM月dd日 HH时mm分ss秒"));

        YYYY_MM_DD_HH_MM_SS_19_FORMATTER_MAP_ENTRY_SET.addAll(
                YYYY_MM_DD_HH_MM_SS_19_FORMATTER_MAP.entrySet());
        YYYY_MM_DD_HH_MM_SS_M19_FORMATTER_MAP_ENTRY_SET.addAll(
                YYYY_MM_DD_HH_MM_SS_M19_FORMATTER_MAP.entrySet());

        YYYY_M_D_HH_MM_SS_17_FORMATTER_MAP_ENTRY_SET.addAll(
                YYYY_M_D_HH_MM_SS_17_FORMATTER_MAP.entrySet());

        YYYY_M_D_HH_MM_15_FORMATTER_MAP_ENTRY_SET.addAll(
                YYYY_M_D_HH_MM_15_FORMATTER_MAP.entrySet());
    }

    /**
     * gave a datetime string and return the {@link DateTimeFormatter} which can be used to parse
     * it.
     *
     * @param dateTime eg: 2020-02-03 12:12:10.101
     * @return the DateTimeFormatter matched, will return null when not matched any pattern
     */
    public static DateTimeFormatter matchDateTimeFormatter(String dateTime) {
        if (dateTime.length() == 19) {
            for (Map.Entry<Pattern, DateTimeFormatter> entry :
                    YYYY_MM_DD_HH_MM_SS_19_FORMATTER_MAP_ENTRY_SET) {
                if (entry.getKey().matcher(dateTime).matches()) {
                    return entry.getValue();
                }
            }
            for (Map.Entry<Pattern, DateTimeFormatter> entry :
                    YYYY_M_D_HH_MM_SS_17_FORMATTER_MAP_ENTRY_SET) {
                if (entry.getKey().matcher(dateTime).matches()) {
                    return entry.getValue();
                }
            }
        } else if (dateTime.length() > 19) {
            for (Map.Entry<Pattern, DateTimeFormatter> entry :
                    YYYY_MM_DD_HH_MM_SS_M19_FORMATTER_MAP_ENTRY_SET) {
                if (entry.getKey().matcher(dateTime).matches()) {
                    return entry.getValue();
                }
            }
        } else if (dateTime.length() == 17 || dateTime.length() == 18) {
            for (Map.Entry<Pattern, DateTimeFormatter> entry :
                    YYYY_M_D_HH_MM_SS_17_FORMATTER_MAP_ENTRY_SET) {
                if (entry.getKey().matcher(dateTime).matches()) {
                    return entry.getValue();
                }
            }
        } else if (dateTime.length() == 15 || dateTime.length() == 16) {
            for (Map.Entry<Pattern, DateTimeFormatter> entry :
                    YYYY_M_D_HH_MM_15_FORMATTER_MAP_ENTRY_SET) {
                if (entry.getKey().matcher(dateTime).matches()) {
                    return entry.getValue();
                }
            }
        } else if (dateTime.length() == 14) {
            for (Map.Entry<Pattern, DateTimeFormatter> entry :
                    YYYY_M_D_HH_MM_15_FORMATTER_MAP_ENTRY_SET) {
                if (entry.getKey().matcher(dateTime).matches()) {
                    return entry.getValue();
                }
            }
            return YYYY_MM_DD_HH_MM_SS_14_FORMATTER;
        }
        return null;
    }

    public static LocalDateTime parse(String dateTime, DateTimeFormatter dateTimeFormatter) {
        TemporalAccessor parsedTimestamp = dateTimeFormatter.parse(dateTime);
        LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
        LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());
        return LocalDateTime.of(localDate, localTime);
    }

    /**
     * gave a datetime string and return {@link LocalDateTime}
     *
     * <p>Due to the need to determine the rules of the formatter through regular expressions, there
     * will be a certain performance loss. When tested on 8c16g macos, the most significant
     * performance decrease compared to directly passing the formatter is
     * 'Pattern.compile("\\d{4}\\.\\d{2}\\.\\d{2}\\s\\d{2}:\\d{2}.*")' has increased from 4.5
     * seconds to 10 seconds in a scenario where 1000w calculations are performed.
     *
     * <p>Analysis shows that there are two main reasons: one is that the regular expression
     * position in the map is 4, before this, three regular expression matches are required.
     *
     * <p>Another reason is to support the length of non fixed millisecond bits (minimum 0, maximum
     * 9), we used {@link DateTimeFormatter#ISO_LOCAL_TIME}, which also increases the time for time
     * conversion.
     *
     * @param dateTime eg: 2020-02-03 12:12:10.101
     * @return {@link LocalDateTime}
     */
    public static LocalDateTime parse(String dateTime) {
        DateTimeFormatter dateTimeFormatter = matchDateTimeFormatter(dateTime);
        return LocalDateTime.parse(dateTime, dateTimeFormatter);
    }

    public static LocalDateTime parse(String dateTime, Formatter formatter) {
        return LocalDateTime.parse(dateTime, FORMATTER_MAP.get(formatter));
    }

    public static LocalDateTime parse(long timestamp) {
        return parse(timestamp, ZoneId.systemDefault());
    }

    public static LocalDateTime parse(long timestamp, ZoneId zoneId) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        return LocalDateTime.ofInstant(instant, zoneId);
    }

    public static String toString(LocalDateTime dateTime, Formatter formatter) {
        return dateTime.format(FORMATTER_MAP.get(formatter));
    }

    public static String toString(long timestamp, Formatter formatter) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        return toString(LocalDateTime.ofInstant(instant, ZoneId.systemDefault()), formatter);
    }

    public enum Formatter {
        YYYY_MM_DD_HH_MM_SS("yyyy-MM-dd HH:mm:ss"),
        YYYY_MM_DD_HH_MM_SS_SSSSSS("yyyy-MM-dd HH:mm:ss.SSSSSS"),
        YYYY_MM_DD_HH_MM_SS_SPOT("yyyy.MM.dd HH:mm:ss"),
        YYYY_MM_DD_HH_MM_SS_SLASH("yyyy/MM/dd HH:mm:ss"),
        YYYY_M_D_HH_MM_SLASH("yyyy/M/d HH:mm"),
        YYYY_M_D_HH_MM_ISO8601("yyyy-M-d HH:mm"),
        YYYY_M_D_HH_MM_SS_SLASH("yyyy/M/d HH:mm:ss"),
        YYYY_M_D_HH_MM_SS_ISO8601("yyyy-M-d HH:mm:ss"),
        YYYY_MM_DD_HH_MM_SS_NO_SPLIT("yyyyMMddHHmmss"),
        YYYY_MM_DD_HH_MM_SS_ISO8601("yyyy-MM-dd'T'HH:mm:ss"),
        YYYY_MM_DD_HH_MM_SS_SSS_ISO8601("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        YYYY_MM_DD_HH_MM_SS_SSSSSS_ISO8601("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
        YYYY_MM_DD_HH_MM_SS_SSSSSSSSS_ISO8601("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS");

        private final String value;

        Formatter(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static Formatter parse(String format) {
            Formatter[] formatters = Formatter.values();
            for (Formatter formatter : formatters) {
                if (formatter.getValue().equals(format)) {
                    return formatter;
                }
            }
            String errorMsg = String.format("Illegal format [%s]", format);
            throw new IllegalArgumentException(errorMsg);
        }
    }
}
