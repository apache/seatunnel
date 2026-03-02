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

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

public class TimeUtils {

    // List of time format patterns, sorted by priority
    private static final List<TimePattern> PATTERN_LIST = new ArrayList<>();

    static {
        // Initialize time format patterns - most common formats first
        PATTERN_LIST.add(
                new TimePattern(
                        "\\d{2}:\\d{2}:\\d{2}", Formatter.HH_MM_SS.value)); // Most common: HH:mm:ss
        PATTERN_LIST.add(
                new TimePattern(
                        "\\d{1,2}:\\d{2}:\\d{2}", Formatter.H_MM_SS.value)); // Common: H:mm:ss
        PATTERN_LIST.add(
                new TimePattern(
                        "\\d{2}:\\d{2}:\\d{2}\\.\\d{3}",
                        Formatter.HH_MM_SS_SSS.value)); // With milliseconds
        PATTERN_LIST.add(
                new TimePattern("\\d{1,2}:\\d{2}:\\d{2}\\.\\d{3}", Formatter.H_MM_SS_SSS.value));
        PATTERN_LIST.add(new TimePattern("\\d{2}:\\d{2}", Formatter.HH_MM.value));
        PATTERN_LIST.add(new TimePattern("\\d{1,2}:\\d{2}", Formatter.H_MM.value));
        PATTERN_LIST.add(
                new TimePattern(
                        "\\d{1,2}:\\d{2}:\\d{2}\\.\\d+",
                        new DateTimeFormatterBuilder()
                                .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NEVER)
                                .appendLiteral(':')
                                .appendValue(MINUTE_OF_HOUR, 2)
                                .appendLiteral(':')
                                .appendValue(SECOND_OF_MINUTE, 2)
                                .optionalStart()
                                .appendFraction(NANO_OF_SECOND, 0, 9, true)
                                .optionalEnd()
                                .toFormatter()));
        PATTERN_LIST.add(
                new TimePattern(
                        "\\d{1,2}:\\d{2}:\\d{2}(?:\\.\\d{0,9})?(?:[+-]\\d{2}:\\d{2}|Z)$",
                        new DateTimeFormatterBuilder()
                                .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NEVER)
                                .appendLiteral(':')
                                .appendValue(MINUTE_OF_HOUR, 2)
                                .appendLiteral(':')
                                .appendValue(SECOND_OF_MINUTE, 2)
                                .optionalStart()
                                .appendFraction(NANO_OF_SECOND, 0, 9, true)
                                .optionalEnd()
                                .optionalStart()
                                .appendOffset("+HH:mm", "Z")
                                .optionalEnd()
                                .toFormatter()));
    }

    // Define time format pattern, containing regex and corresponding Formatter
    @Getter
    private static class TimePattern {
        final Pattern pattern;
        final DateTimeFormatter formatter;

        TimePattern(String regex, String formatter) {
            this.pattern = Pattern.compile(regex);
            this.formatter =
                    new DateTimeFormatterBuilder()
                            .parseCaseInsensitive()
                            .appendOptional(DateTimeFormatter.ofPattern(formatter))
                            .toFormatter();
        }

        TimePattern(String regex, DateTimeFormatter timeFormatter) {
            this.pattern = Pattern.compile(regex);
            this.formatter = timeFormatter;
        }
    }

    /**
     * Match the corresponding Formatter based on the time string
     *
     * @param dateTime Time string, e.g.: 23:00:00
     * @return Matched Formatter, or null if no pattern matches
     */
    public static DateTimeFormatter matchTimeFormatter(String dateTime) {
        for (TimePattern pattern : PATTERN_LIST) {
            if (pattern.getPattern().matcher(dateTime).matches()) {
                return pattern.getFormatter();
            }
        }
        return null;
    }

    /**
     * Automatically infer time string format and parse
     *
     * @param time Time string, e.g.: 23:00:00
     * @return Parsed LocalTime object
     */
    public static LocalTime parse(String time) {
        DateTimeFormatter formatter = matchTimeFormatter(time);
        if (formatter == null) {
            throw new IllegalArgumentException("Unsupported time format: " + time);
        }
        return parse(time, formatter);
    }

    /**
     * Parse time string using the specified Formatter enum
     *
     * @param time Time string
     * @param formatter Time format enum
     * @return Parsed LocalTime object
     */
    public static LocalTime parse(String time, Formatter formatter) {
        return parse(time, formatter.getDateTimeFormatter());
    }

    /**
     * Parse time string using the specified format string
     *
     * @param time Time string
     * @param format Time format string, e.g.: HH:mm:ss
     * @return Parsed LocalTime object
     */
    public static LocalTime parse(String time, String format) {
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern(format);
        return LocalTime.parse(time, timeFormatter);
    }

    /**
     * Parse time string using the specified DateTimeFormatter
     *
     * @param time Time string
     * @param timeFormatter Time formatter
     * @return Parsed LocalTime object
     */
    public static LocalTime parse(String time, DateTimeFormatter timeFormatter) {
        TemporalAccessor temporalAccessor = timeFormatter.parse(time);
        return temporalAccessor.query(TemporalQueries.localTime());
    }

    /**
     * Format LocalTime to string with specified format
     *
     * @param time Time object
     * @param formatter Time format enum
     * @return Formatted string
     */
    public static String toString(LocalTime time, Formatter formatter) {
        return time.format(formatter.getDateTimeFormatter());
    }

    /**
     * Format LocalTime to string with specified format string
     *
     * @param time Time object
     * @param format Time format string, e.g.: HH:mm:ss
     * @return Formatted string
     */
    public static String toString(LocalTime time, String format) {
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern(format);
        return time.format(timeFormatter);
    }

    @Getter
    public enum Formatter implements org.apache.seatunnel.common.config.Formatter<Formatter> {
        HH_MM_SS("HH:mm:ss"),
        HH_MM_SS_SSS("HH:mm:ss.SSS"),
        H_MM_SS("H:mm:ss"),
        H_MM_SS_SSS("H:mm:ss.SSS"),
        HH_MM("HH:mm"),
        H_MM("H:mm");
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
        public String toString() {
            return value;
        }
    }
}
