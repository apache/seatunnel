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

import org.apache.seatunnel.common.config.Formatter;
import org.apache.seatunnel.common.config.FormatterConfig;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;

public interface DateTimeParseHelper {

    /**
     * Generic method to parse date/time values with formatter caching and automatic re-matching on
     * failure.
     */
    @FunctionalInterface
    interface FormatterSupplier {
        DateTimeFormatter get(String fieldVal);
    }

    @FunctionalInterface
    interface Parser<T> {
        T parse(String fieldVal, DateTimeFormatter formatter);
    }

    @FunctionalInterface
    interface ErrorSupplier {
        SeaTunnelRuntimeException get(String fieldVal, String fieldName);
    }

    default <T> T parseDateTimeValue(
            String fieldVal,
            String fieldName,
            FormatterConfig<?> formatterConfig,
            FormatterSupplier autoFormatterSupplier,
            Parser<T> parser,
            ErrorSupplier errorSupplier,
            Map<String, DateTimeFormatter> fieldFormatterCache) {
        if (fieldVal == null || fieldVal.isEmpty()) {
            return null;
        }
        boolean isUserConfigured =
                Objects.nonNull(formatterConfig) && formatterConfig.isUserConfigured();
        DateTimeFormatter formatter =
                fieldFormatterCache.computeIfAbsent(
                        fieldName,
                        key -> {
                            if (isUserConfigured) {
                                Formatter configFormatter = formatterConfig.getFormatter();
                                return DateTimeFormatter.ofPattern(configFormatter.getPattern());
                            } else {
                                DateTimeFormatter matched = autoFormatterSupplier.get(fieldVal);
                                if (matched == null) {
                                    throw errorSupplier.get(fieldVal, fieldName);
                                }
                                return matched;
                            }
                        });
        try {
            return parser.parse(fieldVal, formatter);
        } catch (Exception e) {
            if (isUserConfigured) {
                // If user configured formatter fails, we can't do anything
                throw errorSupplier.get(fieldVal, fieldName);
            }
            // Re-match: replace cached formatter
            DateTimeFormatter newFormatter = autoFormatterSupplier.get(fieldVal);
            if (newFormatter == null) {
                throw errorSupplier.get(fieldVal, fieldName);
            }
            // Atomic replacement (note: there may be concurrency issues here, but rare in actual
            // scenarios)
            fieldFormatterCache.replace(fieldName, formatter, newFormatter);
            return parser.parse(fieldVal, newFormatter);
        }
    }

    default LocalDate parseDate(
            String fieldVal,
            String fieldName,
            FormatterConfig<?> dateFormatterConfig,
            Map<String, DateTimeFormatter> cache) {
        return parseDateTimeValue(
                fieldVal,
                fieldName,
                dateFormatterConfig,
                DateUtils::matchDateFormatter,
                DateUtils::parse,
                CommonError::formatDateError,
                cache);
    }

    default LocalTime parseTime(
            String fieldVal,
            String fieldName,
            FormatterConfig<?> timeFormatterConfig,
            Map<String, DateTimeFormatter> cache) {
        return parseDateTimeValue(
                fieldVal,
                fieldName,
                timeFormatterConfig,
                TimeUtils::matchTimeFormatter,
                TimeUtils::parse,
                CommonError::formatTimeError,
                cache);
    }

    default LocalDateTime parseTimestamp(
            String fieldVal,
            String fieldName,
            FormatterConfig<?> dateTimeFormatterConfig,
            Map<String, DateTimeFormatter> cache) {
        return parseDateTimeValue(
                fieldVal,
                fieldName,
                dateTimeFormatterConfig,
                DateTimeUtils::matchDateTimeFormatter,
                DateTimeUtils::parse,
                CommonError::formatDateTimeError,
                cache);
    }
}
