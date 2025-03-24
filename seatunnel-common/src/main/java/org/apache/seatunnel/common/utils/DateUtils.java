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

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_TIME;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

public class DateUtils {
    private static final Map<Formatter, DateTimeFormatter> FORMATTER_MAP = new HashMap<>();

    static {
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD, DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_SPOT,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_SPOT.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_SLASH,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_SLASH.value));
    }

    public static final Pattern[] PATTERN_ARRAY =
            new Pattern[] {
                Pattern.compile("\\d{4}-\\d{2}-\\d{2}"),
                Pattern.compile("\\d{4}年\\d{2}月\\d{2}日"),
                Pattern.compile("\\d{4}/\\d{2}/\\d{2}"),
                Pattern.compile("\\d{4}\\.\\d{2}\\.\\d{2}"),
                Pattern.compile("\\d{8}"),
                Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,9})?Z"),
                Pattern.compile("\\d{2}:\\d{2}:\\d{2}\\+\\d{2}:\\d{2}"),
                Pattern.compile("\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,9})?"),
                Pattern.compile("\\d{4}/\\d{1,2}/\\d{1,2}")
            };

    public static final Map<Pattern, DateTimeFormatter> DATE_FORMATTER_MAP = new HashMap();

    static {
        DATE_FORMATTER_MAP.put(
                PATTERN_ARRAY[0],
                new DateTimeFormatterBuilder()
                        .parseCaseInsensitive()
                        .append(DateTimeFormatter.ISO_LOCAL_DATE)
                        .toFormatter());

        DATE_FORMATTER_MAP.put(
                PATTERN_ARRAY[1],
                new DateTimeFormatterBuilder()
                        .parseCaseInsensitive()
                        .append(
                                new DateTimeFormatterBuilder()
                                        .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
                                        .appendLiteral("年")
                                        .appendValue(MONTH_OF_YEAR, 2)
                                        .appendLiteral("月")
                                        .appendValue(DAY_OF_MONTH, 2)
                                        .appendLiteral("日")
                                        .toFormatter())
                        .toFormatter());

        DATE_FORMATTER_MAP.put(
                PATTERN_ARRAY[2],
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
                        .toFormatter());

        DATE_FORMATTER_MAP.put(
                PATTERN_ARRAY[3],
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
                        .toFormatter());

        DATE_FORMATTER_MAP.put(
                PATTERN_ARRAY[4],
                new DateTimeFormatterBuilder()
                        .parseCaseInsensitive()
                        .append(
                                new DateTimeFormatterBuilder()
                                        .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
                                        .appendValue(MONTH_OF_YEAR, 2)
                                        .appendValue(DAY_OF_MONTH, 2)
                                        .toFormatter())
                        .toFormatter());
        DATE_FORMATTER_MAP.put(
                PATTERN_ARRAY[5],
                new DateTimeFormatterBuilder()
                        .parseCaseInsensitive()
                        .append(ISO_LOCAL_DATE)
                        .appendLiteral('T')
                        .append(
                                new DateTimeFormatterBuilder()
                                        .appendValue(HOUR_OF_DAY, 2)
                                        .appendLiteral(':')
                                        .appendValue(MINUTE_OF_HOUR, 2)
                                        .optionalStart()
                                        .appendLiteral(':')
                                        .appendValue(SECOND_OF_MINUTE, 2)
                                        .optionalStart()
                                        .appendFraction(NANO_OF_SECOND, 0, 9, true)
                                        .appendLiteral('Z')
                                        .toFormatter())
                        .toFormatter());
        DATE_FORMATTER_MAP.put(PATTERN_ARRAY[6], ISO_OFFSET_TIME);
        DATE_FORMATTER_MAP.put(PATTERN_ARRAY[7], ISO_LOCAL_TIME);
        DATE_FORMATTER_MAP.put(
                PATTERN_ARRAY[8],
                new DateTimeFormatterBuilder()
                        .parseCaseInsensitive()
                        .append(DateTimeFormatter.ofPattern("yyyy/M/d"))
                        .toFormatter());
    }

    /**
     * gave a date string and return the {@link DateTimeFormatter} which can be used to parse it.
     *
     * @param dateTime eg: 2020-02-03
     * @return the DateTimeFormatter matched, will return null when not matched any pattern in
     *     {@link #PATTERN_ARRAY}
     */
    public static DateTimeFormatter matchDateFormatter(String dateTime) {
        for (int j = 0; j < PATTERN_ARRAY.length; j++) {
            if (PATTERN_ARRAY[j].matcher(dateTime).matches()) {
                return DATE_FORMATTER_MAP.get(PATTERN_ARRAY[j]);
            }
        }
        return null;
    }

    public static LocalDate parse(String date) {
        DateTimeFormatter dateTimeFormatter = matchDateFormatter(date);
        return parse(date, dateTimeFormatter);
    }

    public static LocalDate parse(String date, DateTimeFormatter dateTimeFormatter) {
        return LocalDate.parse(date, dateTimeFormatter);
    }

    public static LocalDate parse(String date, Formatter formatter) {
        return LocalDate.parse(date, FORMATTER_MAP.get(formatter));
    }

    public static String toString(LocalDate date, Formatter formatter) {
        return date.format(FORMATTER_MAP.get(formatter));
    }

    public enum Formatter {
        YYYY_MM_DD("yyyy-MM-dd"),
        YYYY_M_D("yyyy/M/d"),
        YYYY_MM_DD_SPOT("yyyy.MM.dd"),
        YYYY_MM_DD_SLASH("yyyy/MM/dd");
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
