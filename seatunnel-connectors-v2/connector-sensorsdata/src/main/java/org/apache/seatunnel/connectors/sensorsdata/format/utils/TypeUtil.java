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

package org.apache.seatunnel.connectors.sensorsdata.format.utils;

import org.apache.seatunnel.shade.com.google.common.base.Objects;

import org.apache.seatunnel.connectors.sensorsdata.format.SensorsDataTypes;
import org.apache.seatunnel.connectors.sensorsdata.format.exception.SensorsDataErrorCode;
import org.apache.seatunnel.connectors.sensorsdata.format.exception.SensorsDataException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@UtilityClass
public class TypeUtil {

    public static final DateTimeFormatter FULL_DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    public static final DateTimeFormatter DEFAULT_DATE_FORMATTER =
            DateTimeFormatter.ofPattern(DEFAULT_DATE_FORMAT).withZone(ZoneId.systemDefault());
    public static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final DateTimeFormatter DEFAULT_DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern(DEFAULT_DATETIME_FORMAT).withZone(ZoneId.systemDefault());

    public static final DateTimeFormatter SHORT_DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").withZone(ZoneId.systemDefault());

    public static final DateTimeFormatter SHORT_DAY_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.systemDefault());
    public static final DateTimeFormatter SHORT_DAY_HOUR_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").withZone(ZoneId.systemDefault());

    /**
     * ORDER IS IMPORTANT Must strictly control the order of this array, otherwise it may cause
     * DateUtil/DateTimeUtil's tryParse function to fail
     */
    public static final DateTimeFormatter[] INTERNAL_DATETIME_FORMATS =
            new DateTimeFormatter[] {
                FULL_DATETIME_FORMATTER,
                DEFAULT_DATETIME_FORMATTER,
                SHORT_DAY_HOUR_FORMATTER,
                DEFAULT_DATE_FORMATTER,
                SHORT_DATETIME_FORMATTER,
                SHORT_DAY_FORMATTER
            };

    private static final String TRANSFORM_WARN_INFO =
            "convert target data type error. source:{}, targetType:{}";

    private static final Map<String, DateTimeFormatter> DATE_TIME_FORMATTER_MAP = new HashMap<>();

    /**
     * Write sensorsdata-inf-sdk data type logic
     *
     * <p>Since inf-sdk writing unsupported data types will cause an error, it is necessary to
     * validate the data type at the beginning, and then insert it; so here supports the following
     * data type conversion: bool: support boolean/number type/boolean string data/timestamp:
     * support date/timestamp/ yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss", "yyyyMMdd", "yyyyMMdd HHmmss four
     * date strings BigInt: support int/long/able to convert to long type string DECIMAL: support
     * number/able to convert to decimal type string int: support int/able to convert to int type
     * string number: support number/able to convert to number type string string: no additional
     * processing list: not additional processing
     */
    public static Object toTargetType(Object source, String targetType) {
        if (null == source || StringUtils.isBlank(targetType)) {
            return source;
        }
        SensorsDataTypes type = SensorsDataTypes.of(targetType);
        return toTargetType(source, type.getType(), type.getExtra());
    }

    public static Object toTargetType(Object source, SensorsDataTypes.DataTypes targetType) {
        return toTargetType(source, targetType, null);
    }

    public static Object toTargetType(
            Object source, SensorsDataTypes.DataTypes targetType, String extra) {
        if (source == null) {
            return null;
        }
        switch (targetType) {
            case BOOLEAN:
                return toBoolean(source, targetType);
            case DECIMAL:
                return toBigDecimal(source, targetType);
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case NUMBER:
                return toNumber(source, targetType);
            case LIST:
                return toList(source, '\n');
            case LIST_COMMA:
                return toList(source, ',');
            case LIST_SEMICOLON:
                return toList(source, ';');
            case TIMESTAMP:
                return toTimestamp(source, targetType, extra);
            case DATE:
                return toDate(source, targetType);
            case STRING:
            default:
                return toString(source);
        }
    }

    private static List<String> toList(Object str, char sep) {
        if (str instanceof String) {
            return Arrays.asList(StringUtils.split((String) str, sep));
        } else {
            throw new SensorsDataException(
                    SensorsDataErrorCode.DATA_TYPE_CAST_FIELD,
                    "Value type must be STRING when target column type is LIST.");
        }
    }

    private static Object toTimestamp(
            Object source, SensorsDataTypes.DataTypes targetType, String format) {
        if (source instanceof Date) {
            return ((Date) source).getTime();
        }
        if (source instanceof Number) {
            return source;
        }
        if (source instanceof LocalDate) {
            return ((LocalDate) source)
                    .atStartOfDay(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli();
        }
        if (source instanceof LocalDateTime) {
            return ((LocalDateTime) source)
                    .atZone(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli();
        }
        if (source instanceof String) {
            Long timestamp;
            if (format == null) {
                timestamp = tryParse((String) source);
            } else {
                DateTimeFormatter formatter = parseDateTimeFormatter(format);
                timestamp = tryParse((String) source, formatter);
            }
            if (timestamp != null) {
                return timestamp;
            }
        }
        log.warn(TRANSFORM_WARN_INFO, source, targetType);
        return source;
    }

    private static Object toBoolean(Object source, SensorsDataTypes.DataTypes targetType) {
        if (source instanceof Boolean) {
            return source;
        }
        if (source instanceof Number) {
            return !Objects.equal(0, source)
                    && !Objects.equal(0F, source)
                    && !Objects.equal(0D, source)
                    && !Objects.equal(0L, source);
        }
        if (source instanceof String) {
            return StringUtils.equalsIgnoreCase("true", source.toString());
        }
        log.warn(TRANSFORM_WARN_INFO, source, targetType);
        return source;
    }

    private static Object toBigDecimal(Object source, SensorsDataTypes.DataTypes targetType) {
        if (source instanceof String) {
            try {
                return NumberUtils.createBigDecimal(source.toString());
            } catch (Exception e) {
                log.warn(TRANSFORM_WARN_INFO, source, targetType);
            }
        } else if (source instanceof Boolean) {
            return BigDecimal.valueOf(Boolean.TRUE.equals(source) ? 1 : 0);
        }
        return source;
    }

    private static Object toNumber(Object source, SensorsDataTypes.DataTypes targetType) {
        if (source instanceof Number) {
            return source;
        }
        if (source instanceof String) {
            try {
                return NumberUtils.createNumber(source.toString());
            } catch (Exception e) {
                log.warn(TRANSFORM_WARN_INFO, source, targetType);
            }
        }
        if (source instanceof Boolean) {
            return Boolean.TRUE.equals(source) ? 1 : 0;
        }
        return source;
    }

    private static String toString(Object source) {
        if (source instanceof byte[]) {
            return new String((byte[]) source);
        }
        return source.toString();
    }

    private static Long tryParse(String str) {
        for (DateTimeFormatter formatter : INTERNAL_DATETIME_FORMATS) {
            Long timestamp = tryParse(str, formatter);
            if (timestamp != null) {
                return timestamp;
            }
        }
        return null;
    }

    private static Long tryParse(String str, DateTimeFormatter formatter) {
        // Since parse fails, it will return null, and the outside world should have some
        // expectations for this method to return null
        // But in the process of loop parsing, only ParseException is processed, so the null value
        // passed in is separated for processing to prevent NPE
        if (StringUtils.isBlank(str)) {
            return null;
        }
        ZonedDateTime time;
        try {
            time = ZonedDateTime.from(formatter.parse(str));
            return time.toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            //  This error should be ignored
            log.debug("Failed to parse date time. [str='{}', formatter='{}']", str, formatter, e);
            return null;
        }
    }

    private static DateTimeFormatter parseDateTimeFormatter(String str) {
        return DATE_TIME_FORMATTER_MAP.computeIfAbsent(str, k -> DateTimeFormatter.ofPattern(str));
    }

    private static Object toDate(Object source, SensorsDataTypes.DataTypes targetType) {
        if (source instanceof Date) {
            return source;
        }
        if (source instanceof Number) {
            return new Date((long) source);
        }
        if (source instanceof LocalDate) {
            return Date.from(((LocalDate) source).atStartOfDay(ZoneId.systemDefault()).toInstant());
        }
        if (source instanceof LocalDateTime) {
            return Date.from(((LocalDateTime) source).atZone(ZoneId.systemDefault()).toInstant());
        }
        if (source instanceof String) {
            Long timestamp = tryParse((String) source);
            if (timestamp != null) {
                return new Date(timestamp);
            }
        }
        log.warn(TRANSFORM_WARN_INFO, source, targetType);
        return source;
    }
}
