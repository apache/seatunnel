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
        DateTimeFormatter formatter = fieldFormatterCache.get(fieldName);
        boolean isUserConfigured =
                Objects.nonNull(formatterConfig) && formatterConfig.isUserConfigured();
        if (formatter == null) {
            if (isUserConfigured) {
                // User configured formatter, extract the pattern value
                String pattern = getPatternStr(formatterConfig);
                formatter = DateTimeFormatter.ofPattern(pattern);
            } else {
                // Auto match formatter
                formatter = autoFormatterSupplier.get(fieldVal);
            }
        }
        if (formatter == null) {
            throw errorSupplier.get(fieldVal, fieldName);
        } else {
            fieldFormatterCache.put(fieldName, formatter);
        }
        try {
            return parser.parse(fieldVal, formatter);
        } catch (Exception e) {
            if (isUserConfigured) {
                // If user configured formatter fails, we can't do anything
                throw errorSupplier.get(fieldVal, fieldName);
            }
            // Re-match formatter and update cache
            formatter = autoFormatterSupplier.get(fieldVal);
            if (formatter == null) {
                throw errorSupplier.get(fieldVal, fieldName);
            }
            fieldFormatterCache.put(fieldName, formatter);
            return parser.parse(fieldVal, formatter);
        }
    }

    default String getPatternStr(FormatterConfig<?> formatterConfig) {
        Object formatterObj = formatterConfig.getFormatter();
        String pattern = "";
        if (formatterObj instanceof DateUtils.Formatter) {
            pattern = ((DateUtils.Formatter) formatterObj).getValue();
        } else if (formatterObj instanceof TimeUtils.Formatter) {
            pattern = ((TimeUtils.Formatter) formatterObj).getValue();
        } else if (formatterObj instanceof DateTimeUtils.Formatter) {
            pattern = ((DateTimeUtils.Formatter) formatterObj).getValue();
        }
        return pattern;
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
