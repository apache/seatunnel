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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

public class DateTimeUtils {

    // List of date-time format patterns, sorted by priority
    private static final Map<Integer, List<DateTimePattern>> DATETIME_PATTERN_MAP = new HashMap<>();
    private static final int OVER_LENGTH_THRESHOLD = 23;
    private static final int OVER_LENGTH_KEY = -1;
    private static final int HAS_TIME_ZONE = -2;

    static {
        initPatternMap();
    }
    /**
     * Initialize the datetime pattern mapping: group by string length for performance optimization
     *
     * <p>Rule1: String length ≤ 23 → group by actual length (14/16/17/19/21/23)
     *
     * <p>Rule 2: String length > 23 → unified into the ultra-long group (key = OVER_LENGTH_KEY =
     * -1)
     *
     * <p>Rule 3: In each group,sort common formats first to prioritize matching
     *
     * <p>Rule 4: Fixed-length format is associated with Formatter enum to reuse pre-defined
     * DateTimeFormatter
     */
    private static void initPatternMap() {
        // Clear the map to avoid repeated initialization when the class is reloaded
        DATETIME_PATTERN_MAP.clear();

        // ===================== Length 14: Fixed Length =====================
        // Format Type: No-separator compact format / Single digit month/day (no second)
        // Example: 20240520123456, 2024-5-1 12:34
        List<DateTimePattern> length14Patterns = new ArrayList<>();
        // High priority: 14-digit no-separator common format
        length14Patterns.add(
                new DateTimePattern("\\d{14}", Formatter.YYYY_MM_DD_HH_MM_SS_NO_SPLIT.value));
        // Secondary: Single digit month/day, ISO8601 separator (no second)
        length14Patterns.add(
                new DateTimePattern(
                        "\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{2}:\\d{2}",
                        Formatter.YYYY_M_D_HH_MM_ISO8601.value));
        // Secondary: Single digit month/day, slash separator (no second)
        length14Patterns.add(
                new DateTimePattern(
                        "\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{2}:\\d{2}",
                        Formatter.YYYY_M_D_HH_MM_SLASH.value));
        DATETIME_PATTERN_MAP.put(14, length14Patterns);

        // ===================== Length 15: Fixed Length =====================
        // Format Type: Mix digit month/day (no second)
        // Example: 2024-12-1 12:34, 2024-1-12 12:34, 2024/12/1 12:34, 2024/1/12 12:34
        List<DateTimePattern> length15Patterns = new ArrayList<>();
        length15Patterns.add(
                new DateTimePattern(
                        "\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{2}:\\d{2}",
                        Formatter.YYYY_M_D_HH_MM_ISO8601.value));
        length15Patterns.add(
                new DateTimePattern(
                        "\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{2}:\\d{2}",
                        Formatter.YYYY_M_D_HH_MM_SLASH.value));
        DATETIME_PATTERN_MAP.put(15, length15Patterns);

        // ===================== Length 16: Fixed Length =====================
        // Format Type: Double digit month/day (no second) + Single digit month/day + single digit
        // hour (with second)
        // Example: 2024-12-31 12:34, 2024/12/31 12:34, 2024-5-1 9:34:56
        List<DateTimePattern> length16Patterns = new ArrayList<>();
        length16Patterns.add(
                new DateTimePattern(
                        "\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{2}:\\d{2}",
                        Formatter.YYYY_M_D_HH_MM_ISO8601.value));
        length16Patterns.add(
                new DateTimePattern(
                        "\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{2}:\\d{2}",
                        Formatter.YYYY_M_D_HH_MM_SLASH.value));
        length16Patterns.add(
                new DateTimePattern(
                        "\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}",
                        Formatter.YYYY_M_D_H_MM_SS_ISO8601.value));
        length16Patterns.add(
                new DateTimePattern(
                        "\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}",
                        Formatter.YYYY_M_D_H_MM_SS_SLASH.value));
        DATETIME_PATTERN_MAP.put(16, length16Patterns);

        // ===================== Length 17: Fixed Length =====================
        // Format Type: Single digit month/day + two digit hour (with second)
        // Example: 2024-5-1 12:34:56, 2024/5/1 12:34:56
        List<DateTimePattern> length17Patterns = new ArrayList<>();
        length17Patterns.add(
                new DateTimePattern(
                        "\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}",
                        Formatter.YYYY_M_D_H_MM_SS_ISO8601.value));
        length17Patterns.add(
                new DateTimePattern(
                        "\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}",
                        Formatter.YYYY_M_D_H_MM_SS_SLASH.value));
        DATETIME_PATTERN_MAP.put(17, length17Patterns);

        // ===================== Length 18: Fixed Length =====================
        // Format Type: Single digit month/day + two digit hour (with second)
        // Example: 2024-5-1 12:34:56, 2024/5/1 12:34:56
        List<DateTimePattern> length18Patterns = new ArrayList<>();
        length18Patterns.add(
                new DateTimePattern(
                        "\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}",
                        Formatter.YYYY_M_D_H_MM_SS_ISO8601.value));
        length18Patterns.add(
                new DateTimePattern(
                        "\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}",
                        Formatter.YYYY_M_D_H_MM_SS_SLASH.value));
        DATETIME_PATTERN_MAP.put(18, length18Patterns);

        // ===================== Length 19: Fixed Length (Core Common) =====================
        // Format Type: Two digit month/day/hour/min/sec (standard format, highest usage frequency)
        // Example: 2024-05-20 12:34:56, 2024-05-20T12:34:56, 2024/05/20 12:34:56
        List<DateTimePattern> length19Patterns = new ArrayList<>();
        // Highest priority: Hyphen separator (most common business format)
        length19Patterns.add(
                new DateTimePattern(
                        "\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}",
                        Formatter.YYYY_MM_DD_HH_MM_SS.value));
        // High priority: ISO8601 standard format (T separator)
        length19Patterns.add(
                new DateTimePattern(
                        "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}",
                        Formatter.YYYY_MM_DD_HH_MM_SS_ISO8601.value));
        // Secondary: Slash separator
        length19Patterns.add(
                new DateTimePattern(
                        "\\d{4}/\\d{2}/\\d{2}\\s\\d{2}:\\d{2}:\\d{2}",
                        Formatter.YYYY_MM_DD_HH_MM_SS_SLASH.value));
        // Secondary: Dot separator
        length19Patterns.add(
                new DateTimePattern(
                        "\\d{4}\\.\\d{2}\\.\\d{2}\\s\\d{2}:\\d{2}:\\d{2}",
                        Formatter.YYYY_MM_DD_HH_MM_SS_SPOT.value));
        DATETIME_PATTERN_MAP.put(19, length19Patterns);

        // ===================== Length 21: Fixed Length (Chinese Exclusive) =====================
        // Format Type: Chinese datetime format (fixed 21 bits, no variable part)
        List<DateTimePattern> length21Patterns = new ArrayList<>();
        length21Patterns.add(
                new DateTimePattern(
                        "\\d{4}年\\d{2}月\\d{2}日\\s\\d{2}时\\d{2}分\\d{2}秒", "yyyy年MM月dd日 HH时mm分ss秒"));
        DATETIME_PATTERN_MAP.put(21, length21Patterns);

        // ===================== Length 23: Fixed Length (3-digit millisecond) =====================
        // Format Type: Standard format with 3-digit millisecond (max fixed length for normal
        // format)
        // Example: 2024-05-20T12:34:56.123, 2024-05-20 12:34:56.123
        List<DateTimePattern> length23Patterns = new ArrayList<>();
        // High priority: ISO8601 3-digit millisecond (standard)
        length23Patterns.add(
                new DateTimePattern(
                        "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}",
                        Formatter.YYYY_MM_DD_HH_MM_SS_SSS_ISO8601.value));
        // High priority: Hyphen separator 3-digit millisecond (common business format)
        length23Patterns.add(
                new DateTimePattern(
                        "\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d{3}",
                        Formatter.YYYY_MM_DD_HH_MM_SS_SSS.value));
        // Secondary: Slash separator 3-digit millisecond
        length23Patterns.add(
                new DateTimePattern(
                        "\\d{4}/\\d{2}/\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d{3}",
                        "yyyy/MM/dd HH:mm:ss.SSS"));
        // Secondary: Dot separator 3-digit millisecond
        length23Patterns.add(
                new DateTimePattern(
                        "\\d{4}\\.\\d{2}\\.\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d{3}",
                        "yyyy.MM.dd HH:mm:ss.SSS"));
        DATETIME_PATTERN_MAP.put(23, length23Patterns);

        // ===================== Ultra Long Group: Length >23 (key = OVER_LENGTH_KEY=-1)
        // =====================
        // Format Type: Variable millisecond (6/9 digit) format, no fixed length
        // Example: 2024-05-20T12:34:56.123456, 2024-05-20 12:34:56.123456789
        List<DateTimePattern> overLengthPatterns = new ArrayList<>();
        // High priority: ISO8601 variable millisecond (6/9 digit, standard)
        overLengthPatterns.add(
                new DateTimePattern(
                        "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d+",
                        DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        // High priority: Hyphen separator variable millisecond (common business)
        overLengthPatterns.add(
                new DateTimePattern(
                        "\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d+",
                        new DateTimeFormatterBuilder()
                                .parseCaseInsensitive()
                                .append(DateTimeFormatter.ISO_LOCAL_DATE)
                                .appendLiteral(' ')
                                .append(DateTimeFormatter.ISO_LOCAL_TIME)
                                .toFormatter()));
        // Secondary: Slash separator variable millisecond
        overLengthPatterns.add(
                new DateTimePattern(
                        "\\d{4}/\\d{2}/\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d+",
                        new DateTimeFormatterBuilder()
                                .parseCaseInsensitive()
                                .appendValue(ChronoField.YEAR, 4)
                                .appendLiteral('/')
                                .appendValue(ChronoField.MONTH_OF_YEAR, 2)
                                .appendLiteral('/')
                                .appendValue(ChronoField.DAY_OF_MONTH, 2)
                                .appendLiteral(' ')
                                .append(DateTimeFormatter.ISO_LOCAL_TIME)
                                .toFormatter()));
        // Secondary: Dot separator variable millisecond
        overLengthPatterns.add(
                new DateTimePattern(
                        "\\d{4}\\.\\d{2}\\.\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d+",
                        new DateTimeFormatterBuilder()
                                .parseCaseInsensitive()
                                .appendValue(ChronoField.YEAR, 4)
                                .appendLiteral('.')
                                .appendValue(ChronoField.MONTH_OF_YEAR, 2)
                                .appendLiteral('.')
                                .appendValue(ChronoField.DAY_OF_MONTH, 2)
                                .appendLiteral(' ')
                                .append(DateTimeFormatter.ISO_LOCAL_TIME)
                                .toFormatter()));
        DATETIME_PATTERN_MAP.put(OVER_LENGTH_KEY, overLengthPatterns);

        List<DateTimePattern> hasTimeZonePatterns = new ArrayList<>();
        hasTimeZonePatterns.add(
                new DateTimePattern(
                        "^\\d{4}-\\d{2}-\\d{2}[T\\s]\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{0,9})?(?:[+-]\\d{2}:\\d{2}|Z)$",
                        new DateTimeFormatterBuilder()
                                .parseCaseInsensitive()
                                .append(DateTimeFormatter.ISO_LOCAL_DATE)
                                .optionalStart()
                                .appendLiteral('T')
                                .optionalEnd()
                                .optionalStart()
                                .appendLiteral(' ')
                                .optionalEnd()
                                .appendValue(HOUR_OF_DAY, 2)
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
        hasTimeZonePatterns.add(
                new DateTimePattern(
                        "^\\d{4}/\\d{2}/\\d{2}[T\\s]\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{0,9})?(?:[+-]\\d{2}:\\d{2}|Z)$",
                        new DateTimeFormatterBuilder()
                                .parseCaseInsensitive()
                                .appendPattern("yyyy/MM/dd")
                                .optionalStart()
                                .appendLiteral('T')
                                .optionalEnd()
                                .optionalStart()
                                .appendLiteral(' ')
                                .optionalEnd()
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
        DATETIME_PATTERN_MAP.put(HAS_TIME_ZONE, hasTimeZonePatterns);
    }

    // Define date-time format pattern, containing regex and corresponding formatter
    @Getter
    private static class DateTimePattern {
        private final Pattern pattern;
        private final DateTimeFormatter dateTimeFormatter;

        DateTimePattern(String regex, DateTimeFormatter dateTimeFormatter) {
            this.pattern = Pattern.compile(regex);
            this.dateTimeFormatter = dateTimeFormatter;
        }

        DateTimePattern(String regex, String formatter) {
            this.pattern = Pattern.compile(regex);
            this.dateTimeFormatter =
                    new DateTimeFormatterBuilder()
                            .parseCaseInsensitive()
                            .appendOptional(DateTimeFormatter.ofPattern(formatter))
                            .toFormatter();
        }
    }

    /**
     * Match the corresponding DateTimeFormatter based on the date-time string
     *
     * @param dateTimeStr Date-time string, e.g.: 2020-02-03 12:12:10.101
     * @return Matched DateTimeFormatter, or null if no pattern matches
     */
    public static DateTimeFormatter matchDateTimeFormatter(String dateTimeStr) {
        if (dateTimeStr == null || dateTimeStr.isEmpty()) {
            throw new IllegalArgumentException("Datetime string cannot be null or empty");
        }
        int strLength = dateTimeStr.length();
        int targetKey =
                isTimeStrHasTimeZone(dateTimeStr)
                        ? HAS_TIME_ZONE
                        : strLength > OVER_LENGTH_THRESHOLD ? OVER_LENGTH_KEY : strLength;
        List<DateTimePattern> dateTimePatterns = DATETIME_PATTERN_MAP.get(targetKey);
        if (dateTimePatterns == null || dateTimePatterns.isEmpty()) {
            return null;
        }
        for (DateTimePattern pattern : dateTimePatterns) {
            if (pattern.getPattern().matcher(dateTimeStr).matches()) {
                return pattern.getDateTimeFormatter();
            }
        }
        return null;
    }

    /**
     * Parse date-time string using the specified DateTimeFormatter
     *
     * @param dateTime Date-time string
     * @param dateTimeFormatter Date-time formatter
     * @return Parsed LocalDateTime object
     */
    public static LocalDateTime parse(String dateTime, DateTimeFormatter dateTimeFormatter) {
        TemporalAccessor parsedTimestamp = dateTimeFormatter.parse(dateTime);
        LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
        LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());
        return LocalDateTime.of(localDate, localTime);
    }

    /**
     * Parses a datetime string by automatically inferring its format.
     *
     * <p>This method supports a wide range of common datetime patterns through a length-based
     * pattern-matching mechanism (lengths 14, 15, 16, 17, 18, 19, 21, 23, and >23). Patterns are
     * grouped by string length to optimize matching performance, with higher-priority formats
     * checked first within each length group.
     *
     * <p><strong>Supported formats include:</strong>
     *
     * <ul>
     *   <li>Standard: 2023-12-25 15:30:45, 2023-12-25T15:30:45
     *   <li>Slash: 2023/12/25 15:30:45, 2023/1/25 8:30:45
     *   <li>Chinese: 2023年12月25日 15时30分45秒
     *   <li>Compact: 20231225153045
     *   <li>With milliseconds: 2023-12-25 15:30:45.123 (supports 1-9 digits)
     * </ul>
     *
     * <p><strong>Performance characteristics:</strong>
     *
     * <ul>
     *   <li>Auto-format parsing: 4.2-4.8 ms per 10⁷ iterations (0.42-0.48 μs/iteration)
     *   <li>Predefined formatter parsing: ~2.97 ms per 10⁷ iterations (0.297 μs/iteration)
     *   <li>Performance overhead: ~1.3-1.9 ms per 10⁷ iterations vs predefined formatters
     * </ul>
     *
     * The overhead primarily results from regex pattern matching and support for variable-length
     * millisecond precision. For maximum performance when the format is known, use {@link
     * #parse(String, DateTimeFormatter)} or {@link #parse(String, Formatter)}.
     *
     * @param dateTime the datetime string to parse (must not be null or empty)
     * @return the parsed {@link LocalDateTime}
     * @throws IllegalArgumentException if the format is unsupported or the input is invalid
     * @see #matchDateTimeFormatter(String) for the underlying pattern-matching logic
     * @see #parse(String, DateTimeFormatter) for parsing with a known formatter
     * @see #parse(String, Formatter) for parsing with a predefined formatter
     */
    public static LocalDateTime parse(String dateTime) {
        DateTimeFormatter dateTimeFormatter = matchDateTimeFormatter(dateTime);
        if (dateTimeFormatter == null) {
            throw new IllegalArgumentException("Unsupported datetime format: " + dateTime);
        }
        return parse(dateTime, dateTimeFormatter);
    }

    /**
     * Parse date-time string using the specified Formatter enum
     *
     * @param dateTime Date-time string
     * @param formatter Date-time format enum
     * @return Parsed LocalDateTime object
     */
    public static LocalDateTime parse(String dateTime, Formatter formatter) {
        return LocalDateTime.parse(dateTime, formatter.getDateTimeFormatter());
    }

    /**
     * Parse date-time string using the specified format string
     *
     * @param dateTime Date-time string
     * @param format Date-time format string, e.g.: yyyy-MM-dd HH:mm:ss
     * @return Parsed LocalDateTime object
     */
    public static LocalDateTime parse(String dateTime, String format) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(format);
        return parse(dateTime, dateTimeFormatter);
    }

    public static LocalDateTime parse(long timestamp) {
        return parse(timestamp, ZoneId.systemDefault());
    }

    public static LocalDateTime parse(long timestamp, ZoneId zoneId) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        return LocalDateTime.ofInstant(instant, zoneId);
    }

    /**
     * Format LocalDateTime to string with specified format
     *
     * @param dateTime Date-time object
     * @param formatter Date-time format enum
     * @return Formatted string
     */
    public static String toString(LocalDateTime dateTime, Formatter formatter) {
        return dateTime.format(formatter.getDateTimeFormatter());
    }

    /**
     * Format LocalDateTime to string with specified format string
     *
     * @param dateTime Date-time object
     * @param format Date-time format string, e.g.: yyyy-MM-dd HH:mm:ss
     * @return Formatted string
     */
    public static String toString(LocalDateTime dateTime, String format) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(format);
        return dateTime.format(dateTimeFormatter);
    }

    /**
     * Format OffsetDateTime to string with specified format
     *
     * @param offsetDateTime Offset date-time object
     * @param formatter Date-time format enum
     * @return Formatted string
     */
    public static String toString(OffsetDateTime offsetDateTime, Formatter formatter) {
        return toString(offsetDateTime.toLocalDateTime(), formatter);
    }

    /**
     * Format Temporal object to string with specified format
     *
     * @param temporal Date-time object
     * @param formatter Date-time format enum
     * @return Formatted string
     */
    public static String toString(Temporal temporal, Formatter formatter) {
        if (temporal instanceof OffsetDateTime) {
            return toString(((OffsetDateTime) temporal).toLocalDateTime(), formatter);
        } else if (temporal instanceof java.time.ZonedDateTime) {
            return toString(((java.time.ZonedDateTime) temporal).toLocalDateTime(), formatter);
        } else {
            return formatter.getDateTimeFormatter().format(temporal);
        }
    }

    /**
     * Format timestamp to string with specified format
     *
     * @param timestamp Timestamp in milliseconds
     * @param formatter Date-time format enum
     * @return Formatted string
     */
    public static String toString(long timestamp, Formatter formatter) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        return toString(LocalDateTime.ofInstant(instant, ZoneId.systemDefault()), formatter);
    }

    /**
     * Format timestamp to string with specified format string
     *
     * @param timestamp Timestamp in milliseconds
     * @param format Date-time format string, e.g.: yyyy-MM-dd HH:mm:ss
     * @return Formatted string
     */
    public static String toString(long timestamp, String format) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        return toString(LocalDateTime.ofInstant(instant, ZoneId.systemDefault()), format);
    }

    public static boolean isTimeStrHasTimeZone(String timeStr) {
        if (timeStr == null || timeStr.isEmpty()) {
            return false;
        }
        return timeStr.endsWith("Z")
                || timeStr.contains("+") && timeStr.contains(":")
                || timeStr.contains("-") && timeStr.indexOf("-") > 5;
    }

    @Getter
    public enum Formatter implements org.apache.seatunnel.common.config.Formatter<Formatter> {
        YYYY_MM_DD_HH_MM_SS("yyyy-MM-dd HH:mm:ss"),
        YYYY_MM_DD_HH_MM_SS_SSSSSS("yyyy-MM-dd HH:mm:ss.SSSSSS"),
        YYYY_MM_DD_HH_MM_SS_SSS("yyyy-MM-dd HH:mm:ss.SSS"),
        YYYY_MM_DD_HH_MM_SS_SPOT("yyyy.MM.dd HH:mm:ss"),
        YYYY_MM_DD_HH_MM_SS_SLASH("yyyy/MM/dd HH:mm:ss"),
        YYYY_M_D_HH_MM_SLASH("yyyy/M/d HH:mm"),
        YYYY_M_D_HH_MM_ISO8601("yyyy-M-d HH:mm"),
        YYYY_M_D_H_MM_SS_SLASH("yyyy/M/d H:mm:ss"),
        YYYY_M_D_H_MM_SS_ISO8601("yyyy-M-d H:mm:ss"),
        YYYY_MM_DD_HH_MM_SS_NO_SPLIT("yyyyMMddHHmmss"),
        YYYY_MM_DD_HH_MM_SS_ISO8601("yyyy-MM-dd'T'HH:mm:ss"),
        YYYY_MM_DD_HH_MM_SS_SSS_ISO8601("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        YYYY_MM_DD_HH_MM_SS_SSSSSS_ISO8601("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
        YYYY_MM_DD_HH_MM_SS_SSSSSSSSS_ISO8601("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS");

        private final String value;
        private final int length;
        private final DateTimeFormatter dateTimeFormatter;

        Formatter(String value) {
            this.value = value;
            this.length = value.length();
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
