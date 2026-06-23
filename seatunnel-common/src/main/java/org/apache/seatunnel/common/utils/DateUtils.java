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

import lombok.Getter;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.List;
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

    // Define date format pattern, containing regex and corresponding formatter
    private static class DatePattern {
        final Pattern pattern;
        final DateTimeFormatter formatter;

        DatePattern(String regex, String formatter) {
            this.pattern = Pattern.compile(regex);
            this.formatter =
                    new DateTimeFormatterBuilder()
                            .parseCaseInsensitive()
                            .appendOptional(DateTimeFormatter.ofPattern(formatter))
                            .toFormatter();
        }

        DatePattern(String regex, DateTimeFormatter format) {
            this.pattern = Pattern.compile(regex);
            this.formatter = format;
        }
    }

    // List of date format patterns, sorted by priority
    private static final List<DatePattern> PATTERN_LIST = new ArrayList<>();

    static {
        // Initialize date format patterns
        PATTERN_LIST.add(new DatePattern("\\d{4}-\\d{2}-\\d{2}", Formatter.YYYY_MM_DD.value));
        PATTERN_LIST.add(new DatePattern("\\d{8}", "yyyyMMdd"));
        PATTERN_LIST.add(new DatePattern("\\d{4}/\\d{2}/\\d{2}", Formatter.YYYY_MM_DD_SLASH.value));
        PATTERN_LIST.add(
                new DatePattern("\\d{4}\\.\\d{2}\\.\\d{2}", Formatter.YYYY_MM_DD_SPOT.value));
        PATTERN_LIST.add(new DatePattern("\\d{4}-\\d{1,2}-\\d{1,2}", Formatter.YYYY_M_D.value));
        PATTERN_LIST.add(
                new DatePattern("\\d{4}/\\d{1,2}/\\d{1,2}", Formatter.YYYY_M_D_SLASH.value));
        PATTERN_LIST.add(
                new DatePattern("\\d{4}\\.\\d{1,2}\\.\\d{1,2}", Formatter.YYYY_M_D_SPOT.value));
        PATTERN_LIST.add(
                new DatePattern(
                        "^\\d{1,2}[-/]\\d{1,2}[-/]\\d{4}",
                        new DateTimeFormatterBuilder()
                                .parseCaseInsensitive()
                                .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NEVER)
                                .appendOptional(
                                        new DateTimeFormatterBuilder()
                                                .appendLiteral('/')
                                                .toFormatter())
                                .appendOptional(
                                        new DateTimeFormatterBuilder()
                                                .appendLiteral('-')
                                                .toFormatter())
                                .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NEVER)
                                .appendOptional(
                                        new DateTimeFormatterBuilder()
                                                .appendLiteral('/')
                                                .toFormatter())
                                .appendOptional(
                                        new DateTimeFormatterBuilder()
                                                .appendLiteral('-')
                                                .toFormatter())
                                .appendValue(ChronoField.YEAR, 4)
                                .toFormatter()));
        PATTERN_LIST.add(
                new DatePattern(
                        "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,9})?Z?",
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
                                .toFormatter()));
        PATTERN_LIST.add(new DatePattern("\\d{2}:\\d{2}:\\d{2}\\+\\d{2}:\\d{2}", ISO_OFFSET_TIME));
        PATTERN_LIST.add(new DatePattern("\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,9})?", ISO_LOCAL_TIME));
        PATTERN_LIST.add(
                new DatePattern(
                        "\\d{4}年\\d{2}月\\d{2}日",
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
                                .toFormatter()));
    }

    /**
     * Match the corresponding DateTimeFormatter based on the date string
     *
     * @param date Date string, e.g.: 2020-02-03
     * @return Matched DateTimeFormatter, or null if no pattern matches
     */
    public static DateTimeFormatter matchDateFormatter(String date) {
        for (DatePattern pattern : PATTERN_LIST) {
            if (pattern.pattern.matcher(date).matches()) {
                return pattern.formatter;
            }
        }
        return null;
    }

    /**
     * Automatically infer date string format and parse
     *
     * @param date Date string, e.g.: 2020-02-03
     * @return Parsed LocalDate object
     */
    public static LocalDate parse(String date) {
        DateTimeFormatter dateTimeFormatter = matchDateFormatter(date);
        if (dateTimeFormatter == null) {
            throw new IllegalArgumentException("Unsupported date format: " + date);
        }
        return parse(date, dateTimeFormatter);
    }

    /**
     * Parse date string using the specified DateTimeFormatter
     *
     * @param date Date string
     * @param dateTimeFormatter Date formatter
     * @return Parsed LocalDate object
     */
    public static LocalDate parse(String date, DateTimeFormatter dateTimeFormatter) {
        TemporalAccessor temporalAccessor = dateTimeFormatter.parse(date);
        return temporalAccessor.query(TemporalQueries.localDate());
    }

    /**
     * Parse date string using the specified Formatter enum
     *
     * @param date Date string
     * @param formatter Date format enum
     * @return Parsed LocalDate object
     */
    public static LocalDate parse(String date, Formatter formatter) {
        return parse(date, formatter.getDateTimeFormatter());
    }

    /**
     * Parse date string using the specified format string
     *
     * @param date Date string
     * @param format Date format string, e.g.: yyyy-MM-dd
     * @return Parsed LocalDate object
     */
    public static LocalDate parse(String date, String format) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(format);
        return parse(date, dateTimeFormatter);
    }

    /**
     * Format LocalDate to string with specified format
     *
     * @param date Date object
     * @param formatter Date format enum
     * @return Formatted string
     */
    public static String toString(LocalDate date, Formatter formatter) {
        return date.format(formatter.getDateTimeFormatter());
    }

    /**
     * Format LocalDate to string with specified format string
     *
     * @param date Date object
     * @param format Date format string, e.g.: yyyy-MM-dd
     * @return Formatted string
     */
    public static String toString(LocalDate date, String format) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(format);
        return date.format(dateTimeFormatter);
    }

    /**
     * Format Temporal object to string with specified format
     *
     * @param temporal Date object
     * @param formatter Date format enum
     * @return Formatted string
     */
    public static String toString(Temporal temporal, Formatter formatter) {
        return formatter.getDateTimeFormatter().format(temporal);
    }

    @Getter
    public enum Formatter implements org.apache.seatunnel.common.config.Formatter<Formatter> {
        YYYY_MM_DD("yyyy-MM-dd"),
        YYYY_MM_DD_SPOT("yyyy.MM.dd"),
        YYYY_MM_DD_SLASH("yyyy/MM/dd"),
        YYYY_M_D("yyyy-M-d"),
        YYYY_M_D_SPOT("yyyy.M.d"),
        YYYY_M_D_SLASH("yyyy/M/d");
        private final String value;
        private final DateTimeFormatter dateTimeFormatter;

        Formatter(String value) {
            this.value = value;
            this.dateTimeFormatter = DateTimeFormatter.ofPattern(value);
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

        @Override
        public Formatter getFormatter() {
            return this;
        }

        @Override
        public String getPattern() {
            return getValue();
        }

        @Override
        public String toString() {
            return value;
        }
    }
}
