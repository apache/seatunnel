package org.apache.seatunnel.format.sensorsdata.utils;

import org.apache.seatunnel.shade.com.google.common.base.Objects;

import org.apache.seatunnel.format.sensorsdata.SensorsDataTypes;
import org.apache.seatunnel.format.sensorsdata.exception.SensorsDataErrorCode;
import org.apache.seatunnel.format.sensorsdata.exception.SensorsDataException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

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

    // 必须严格控制该数组的排序，否则可能导致DateUtil/DateTimeUtil的tryParse函数出错
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
     * 写 inf-sdk 数据类型逻辑
     *
     * <p>由于 inf-sdk 写入不支持的数据类型会报错，则需要在开始的时候就校验好数据类型，再塞入；所以这里支持以下数据类型转换： bool：支持 布尔/数字类型/布尔字符串
     * data/timestamp：支持 日期/时间戳/ yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss", "yyyyMMdd", "yyyyMMdd HHmmss
     * 四种日期字符串 BigInt：支持 int/long/能转成 long 类型的字符串 DECIMAL：支持 number/能转成 decimal 类型的字符串 int：支持
     * int/能转成 int 类型的字符串 number：支持 number/能转成 number 类型的字符串 string：不额外处理 list：本期不额外处理
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
                    SensorsDataErrorCode.DATA_TYPE_CAST_FILED,
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
            return BigDecimal.valueOf((Boolean) source ? 1 : 0);
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
            return (Boolean) source ? 1 : 0;
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
        // 既然当 parse 失败后会返回 null，则外界对于这个方法会 return null 应该有一定的预期
        // 但是在循环 parse 过程中只对 ParseException 进行了处理，所以此处把传入空值的情况拎出来单独处理防止 NPE
        if (StringUtils.isBlank(str)) {
            return null;
        }
        ZonedDateTime time;
        try {
            time = ZonedDateTime.from(formatter.parse(str));
            return time.toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            // 这个错误应该被忽略
            log.debug("Failed to parse date time. [str='{}', formatter='{}']", str, formatter, e);
            return null;
        }
    }

    private static DateTimeFormatter parseDateTimeFormatter(String str) {
        if (!DATE_TIME_FORMATTER_MAP.containsKey(str)) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(str);
            DATE_TIME_FORMATTER_MAP.put(str, formatter);
        }
        return DATE_TIME_FORMATTER_MAP.get(str);
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
