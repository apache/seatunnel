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
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalField;
import java.util.List;
import java.util.Optional;

public enum DateTimeFunctionEnum {
    /**
     * CURRENT_DATE [()]
     *
     * <p>Returns the current date.
     *
     * <p>These functions return the same value within a transaction (default) or within a command
     * depending on database mode.
     *
     * <p>Example:
     */
    CURRENT_DATE {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 0) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 0");
            }
            SeaTunnelDataType<?> type = LocalTimeType.LOCAL_DATE_TYPE;
            Object val = LocalDate.now();
            return Pair.of(type, val);
        }
    },
    /**
     * CURRENT_TIME [()]
     *
     * <p>Returns the current time with system time zone. The actual maximum available precision
     * depends on operating system and JVM and can be 3 (milliseconds) or higher. Higher precision
     * is not available before Java 9.
     *
     * <p>Example:
     *
     * <p>CURRENT_TIME
     */
    CURRENT_TIME {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 0) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 0");
            }
            SeaTunnelDataType<?> type = LocalTimeType.LOCAL_TIME_TYPE;
            Object val = LocalTime.now();
            return Pair.of(type, val);
        }
    },
    /**
     * CURRENT_TIMESTAMP[()] | NOW()
     *
     * <p>Returns the current timestamp with system time zone. The actual maximum available
     * precision depends on operating system and JVM and can be 3 (milliseconds) or higher. Higher
     * precision is not available before Java 9.
     *
     * <p>Example:
     *
     * <p>CURRENT_TIMESTAMP
     */
    CURRENT_TIMESTAMP {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 0) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 0");
            }
            SeaTunnelDataType<?> type = LocalTimeType.LOCAL_DATE_TIME_TYPE;
            Object val = LocalDateTime.now();
            return Pair.of(type, val);
        }
    },
    NOW {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            return CURRENT_TIMESTAMP.execute(types, objects);
        }
    },
    /**
     * DATEADD| TIMESTAMPADD(dateAndTime, addIntLong[, datetimeFieldString])
     *
     * <p>Adds units to a date-time value. The datetimeFieldString indicates the unit. Use negative
     * values to subtract units. addIntLong may be a long value when manipulating milliseconds,
     * microseconds, or nanoseconds otherwise its range is restricted to int. This method returns a
     * value with the same type as specified value if unit is compatible with this value. If
     * specified field is an HOUR, MINUTE, SECOND, MILLISECOND, etc. and value is a DATE value
     * DATEADD returns combined TIMESTAMP. Fields DAY, MONTH, YEAR, WEEK, etc. are not allowed for
     * TIME values.
     *
     * <p>Example:
     *
     * <p>DATEADD(CREATED, 1, 'MONTH')
     */
    DATEADD {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() < 2 || objects.size() > 3) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require between 2 and 3");
            }
            SeaTunnelDataType<?> type = types.get(0);
            Object val = objects.get(0);

            if (val != null) {
                int count =
                        Optional.ofNullable(objects.get(1))
                                .map(item -> NumberUtils.toInt(item.toString()))
                                .orElse(0);

                ChronoUnit unit = ChronoUnit.DAYS;
                if (objects.size() == 3) {
                    unit =
                            Optional.ofNullable(objects.get(2))
                                    .map(item -> FunctionUtils.getDatetimeField(item.toString()))
                                    .orElse(unit);
                }

                LocalDateTime dateTime = FunctionUtils.parseDate(val.toString());
                if (dateTime != null && dateTime.isSupported(unit)) {
                    dateTime = dateTime.plus(count, unit);
                    val = FunctionUtils.convertDateTimeByType(type, dateTime);
                    return Pair.of(type, val);
                }
            }
            return Pair.of(type, null);
        }
    },
    TIMESTAMPADD {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            return DATEADD.execute(types, objects);
        }
    },
    /**
     * DATEDIFF(aDateAndTime, bDateAndTime[, datetimeFieldString])
     *
     * <p>Returns the number of crossed unit boundaries between two date-time values. This method
     * returns a long. The datetimeField indicates the unit.
     *
     * <p>Example:
     *
     * <p>DATEDIFF(T1.CREATED, T2.CREATED, 'MONTH')
     */
    DATEDIFF {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() < 2 || objects.size() > 3) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require between 2 and 3");
            }
            SeaTunnelDataType<?> type = BasicType.LONG_TYPE;
            Object o1 = objects.get(0);
            Object o2 = objects.get(1);
            if (o1 != null && o2 != null) {
                ChronoUnit unit = ChronoUnit.DAYS;
                if (objects.size() == 3) {
                    unit =
                            Optional.ofNullable(objects.get(2))
                                    .map(item -> FunctionUtils.getDatetimeField(item.toString()))
                                    .orElse(unit);
                }
                LocalDateTime d1 = FunctionUtils.parseDate(o1.toString());
                LocalDateTime d2 = FunctionUtils.parseDate(o2.toString());
                if (d1 != null && d2 != null && d1.isSupported(unit) && d2.isSupported(unit)) {
                    long between = d1.until(d2, unit);
                    return Pair.of(type, between);
                }
            }
            return Pair.of(type, null);
        }
    },
    /**
     * DATE_TRUNC (dateAndTime[, datetimeFieldString])
     *
     * <p>Truncates the specified date-time value to the specified field.
     *
     * <p>Example:
     *
     * <p>DATE_TRUNC(CREATED, 'DAY');
     */
    DATE_TRUNC {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() < 1 || objects.size() > 2) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require between 1 and 2");
            }
            SeaTunnelDataType<?> type = types.get(0);
            Object val = objects.get(0);
            if (val != null) {
                LocalDateTime dateTime = FunctionUtils.parseDate(val.toString());

                if (dateTime != null) {
                    int year = dateTime.getYear();
                    int month = dateTime.getMonthValue();
                    int day = dateTime.getDayOfMonth();
                    int hour = dateTime.getHour();
                    int minute = dateTime.getMinute();
                    int second = dateTime.getSecond();

                    String datetimeField = "DAY";
                    if (objects.size() == 2) {
                        datetimeField =
                                FunctionUtils.convertToString(objects.get(1), datetimeField);
                    }
                    LocalDateTime result;
                    switch (datetimeField.toUpperCase()) {
                        case "YEAR":
                            result = LocalDateTime.of(year, 1, 1, 0, 0, 0);
                            break;
                        case "MONTH":
                            result = LocalDateTime.of(year, month, 1, 0, 0, 0);
                            break;
                        case "DAY":
                            result = LocalDateTime.of(year, month, day, 0, 0, 0);
                            break;
                        case "HOUR":
                            result = LocalDateTime.of(year, month, day, hour, 0, 0);
                            break;
                        case "MINUTE":
                            result = LocalDateTime.of(year, month, day, hour, minute, 0);
                            break;
                        case "SECOND":
                        default:
                            result = LocalDateTime.of(year, month, day, hour, minute, second);
                    }

                    val = FunctionUtils.convertDateTimeByType(type, result);
                    return Pair.of(type, val);
                }
            }
            return Pair.of(type, null);
        }
    },
    /**
     * DAYNAME([dateAndTime])
     *
     * <p>Returns the name of the day (in English).
     *
     * <p>Example:
     *
     * <p>DAYNAME(CREATED)
     */
    DAYNAME {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() > 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require lt 2");
            }
            return FunctionUtils.getDayName(types, objects, name());
        }
    },
    /**
     * DAY_OF_MONTH([dateAndTime])
     *
     * <p>Returns the day of the month (1-31).
     *
     * <p>Example:
     *
     * <p>DAY_OF_MONTH(CREATED)
     */
    DAY_OF_MONTH {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() > 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require lt 2");
            }

            return FunctionUtils.getDaysByName(types, objects, "DAY");
        }
    },
    /**
     * DAY_OF_WEEK([dateAndTime])
     *
     * <p>Returns the day of the week (1-7) (Monday-Sunday), locale-specific.
     *
     * <p>Example:
     *
     * <p>DAY_OF_WEEK(CREATED)
     */
    DAY_OF_WEEK {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() > 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require lt 2");
            }
            return FunctionUtils.getDaysByName(types, objects, "DAYOFWEEK");
        }
    },
    /**
     * DAY_OF_YEAR([dateAndTime])
     *
     * <p>Returns the day of the year (1-366).
     *
     * <p>Example:
     *
     * <p>DAY_OF_YEAR(CREATED)
     */
    DAY_OF_YEAR {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() > 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require lt 2");
            }
            return FunctionUtils.getDaysByName(types, objects, "DAYOFYEAR");
        }
    },
    /**
     * EXTRACT ( datetimeField FROM dateAndTime)
     *
     * <p>Returns a value of the specific time unit from a date/time value. This method returns a
     * numeric value with EPOCH field and an int for all other fields.
     *
     * <p>Example:
     *
     * <p>EXTRACT(SECOND FROM CURRENT_TIMESTAMP)
     */
    EXTRACT {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 2) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 2");
            }
            SeaTunnelDataType<?> type = BasicType.INT_TYPE;
            Object val = null;

            String dateTimeStr = FunctionUtils.convertToString(objects.get(0), "");
            LocalDateTime dateTime = FunctionUtils.parseDate(dateTimeStr);
            if (dateTime != null) {
                Object p2 = objects.get(1);
                if (p2 != null) {
                    String datetimeField = p2.toString().toUpperCase();
                    TemporalField temporalField = FunctionUtils.getTemporalField(datetimeField);
                    if (temporalField != null) {
                        val = dateTime.get(temporalField);
                    }
                }
            }
            return Pair.of(type, val);
        }
    },
    /**
     * FORMATDATETIME (dateAndTime, formatString)
     *
     * <p>Formats a date, time or timestamp as a string. The most important format characters are: y
     * year, M month, d day, H hour, m minute, s second. For details of the format, see
     * java.time.format.DateTimeFormatter.
     *
     * <p>This method returns a string.
     *
     * <p>Example:
     *
     * <p>CALL FORMATDATETIME(CREATED, 'yyyy-MM-dd HH:mm:ss')
     */
    FORMATDATETIME {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 2) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 2");
            }
            SeaTunnelDataType<?> type = BasicType.STRING_TYPE;
            Object val = null;

            LocalDateTime dateTime =
                    Optional.ofNullable(objects.get(0))
                            .map(item -> FunctionUtils.parseDate(item.toString()))
                            .orElse(null);

            if (dateTime != null) {
                String formatStr = FunctionUtils.convertToString(objects.get(1), null);
                try {
                    DateTimeFormatter df = DateTimeFormatter.ofPattern(formatStr);
                    val = df.format(dateTime);
                    return Pair.of(type, val);
                } catch (Exception e) {
                    // no op
                }
            }
            return Pair.of(type, val);
        }
    },
    /**
     * HOUR([dateAndTime])
     *
     * <p>Returns the hour (0-23) from a date/time value.
     *
     * <p>Example:
     *
     * <p>HOUR(CREATED)
     */
    HOUR {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() > 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require lt 2");
            }
            return FunctionUtils.getDaysByName(types, objects, "HOUR");
        }
    },
    /**
     * MINUTE([dateAndTime])
     *
     * <p>Returns the minute (0-59) from a date/time value.
     *
     * <p>This function is deprecated, use EXTRACT instead of it.
     *
     * <p>Example:
     *
     * <p>MINUTE(CREATED)
     */
    MINUTE {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() > 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require lt 2");
            }
            return FunctionUtils.getDaysByName(types, objects, "MINUTE");
        }
    },
    /**
     * MONTH(dateAndTime)
     *
     * <p>Returns the month (1-12) from a date/time value.
     *
     * <p>This function is deprecated, use EXTRACT instead of it.
     *
     * <p>Example:
     *
     * <p>MONTH(CREATED)
     */
    MONTH {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() > 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require lt 2");
            }
            return FunctionUtils.getDaysByName(types, objects, "MONTH");
        }
    },
    /**
     * MONTHNAME(dateAndTime)
     *
     * <p>Returns the name of the month (in English).
     *
     * <p>Example:
     *
     * <p>MONTHNAME(CREATED)
     */
    MONTHNAME {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() > 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require lt 2");
            }
            return FunctionUtils.getDayName(types, objects, name());
        }
    },
    /**
     * PARSEDATETIME | TO_DATE(string, formatString) Parses a string and returns a TIMESTAMP WITH
     * TIME ZONE value. The most important format characters are: y year, M month, d day, H hour, m
     * minute, s second. For details of the format, see java.time.format.DateTimeFormatter.
     *
     * <p>Example:
     *
     * <p>CALL PARSEDATETIME('2021-04-08 13:34:45','yyyy-MM-dd HH:mm:ss')
     */
    PARSEDATETIME {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() != 2) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require eq 2");
            }
            SeaTunnelDataType<?> type = LocalTimeType.LOCAL_DATE_TIME_TYPE;
            Object val = null;
            String dateTimeStr = FunctionUtils.convertToString(objects.get(0), "");
            String formatStr = FunctionUtils.convertToString(objects.get(1), "");

            try {
                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(formatStr);
                if (formatStr.contains("yy") && formatStr.contains("mm")) {
                    val = LocalDateTime.parse(dateTimeStr, dateTimeFormatter);
                    return Pair.of(type, val);
                }
                if (formatStr.contains("yy")) {
                    type = LocalTimeType.LOCAL_DATE_TYPE;
                    val = LocalDate.parse(dateTimeStr, dateTimeFormatter);
                    return Pair.of(type, val);
                }
                if (formatStr.contains("mm")) {
                    type = LocalTimeType.LOCAL_TIME_TYPE;
                    val = LocalTime.parse(dateTimeStr, dateTimeFormatter);
                    return Pair.of(type, val);
                }
            } catch (Exception e) {
                // no op
            }
            return Pair.of(type, val);
        }
    },
    TO_DATE {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            return PARSEDATETIME.execute(types, objects);
        }
    },
    /**
     * QUARTER([dateAndTime])
     *
     * <p>Returns the quarter (1-4) from a date/time value.
     *
     * <p>Example:
     *
     * <p>QUARTER(CREATED)
     */
    QUARTER {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() > 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require lt 2");
            }
            Pair<SeaTunnelDataType<?>, Object> pair =
                    FunctionUtils.getDaysByName(types, objects, "MONTH");
            SeaTunnelDataType<?> type = pair.getKey();
            Object val = pair.getValue();
            if (val != null) {
                int month = Integer.parseInt(val.toString());
                if (month > 0) {
                    month = (month + 2) / 3;
                    return Pair.of(type, month);
                }
            }
            return pair;
        }
    },
    /**
     * SECOND([dateAndTime])
     *
     * <p>Returns the second (0-59) from a date/time value.
     *
     * <p>This function is deprecated, use EXTRACT instead of it.
     *
     * <p>Example:
     *
     * <p>SECOND(CREATED)
     */
    SECOND {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() > 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require lt 2");
            }
            return FunctionUtils.getDaysByName(types, objects, "SECOND");
        }
    },
    /**
     * WEEK([dateAndTime])
     *
     * <p>Returns the week (1-53) from a date/time value.
     *
     * <p>This function uses the current system locale.
     *
     * <p>Example:
     *
     * <p>WEEK(CREATED)
     */
    WEEK {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() > 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require lt 2");
            }
            Pair<SeaTunnelDataType<?>, Object> pair =
                    FunctionUtils.getDaysByName(types, objects, "WEEKOFYEAR");
            SeaTunnelDataType<?> type = pair.getKey();
            Object val = pair.getValue();
            if (val != null) {
                int week = Integer.parseInt(val.toString());
                if (week > 0) {
                    week = week + 1;
                    return Pair.of(type, week);
                }
            }
            return pair;
        }
    },
    /**
     * YEAR([dateAndTime])
     *
     * <p>Returns the year from a date/time value.
     *
     * <p>Example:
     *
     * <p>YEAR(CREATED)
     */
    YEAR {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() > 1) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require lt 2");
            }
            return FunctionUtils.getDaysByName(types, objects, "YEAR");
        }
    },
    FROM_UNIXTIME {
        @Override
        public Pair<SeaTunnelDataType<?>, Object> execute(
                List<SeaTunnelDataType<?>> types, List<Object> objects) {
            if (objects.size() < 2 || objects.size() > 3) {
                throw new IllegalArgumentException(
                        "The " + name() + " function arguments size require between 2 and 3");
            }
            BasicType<String> type = BasicType.STRING_TYPE;
            Object val = objects.get(0);
            if (val != null) {
                Long unixTime = null;
                try {
                    unixTime = Long.parseLong(val.toString());
                } catch (Exception e) {
                    // no op
                }
                String format = FunctionUtils.convertToString(objects.get(1), null);
                ZoneId zoneId = ZoneId.systemDefault();
                if (objects.size() == 3) {
                    String timeZone = FunctionUtils.convertToString(objects.get(2), null);
                    try {
                        zoneId = ZoneId.of(timeZone);
                    } catch (Exception e) {
                        // no op
                    }
                }
                try {
                    DateTimeFormatter df = DateTimeFormatter.ofPattern(format);
                    LocalDateTime datetime =
                            Instant.ofEpochSecond(unixTime).atZone(zoneId).toLocalDateTime();
                    val = df.format(datetime);
                    return Pair.of(type, val);
                } catch (Exception e) {
                    // no op
                }
            }
            return Pair.of(type, null);
        }
    };

    public abstract Pair<SeaTunnelDataType<?>, Object> execute(
            List<SeaTunnelDataType<?>> types, List<Object> objects);

    public static Pair<SeaTunnelDataType<?>, Object> execute(
            String funcName, List<SeaTunnelDataType<?>> types, List<Object> objects) {
        for (DateTimeFunctionEnum value : values()) {
            if (value.name().equals(funcName.toUpperCase())) {
                return value.execute(types, objects);
            }
        }
        return null;
    }
}
