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

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.sql.zeta.ZetaSQLFunction;

import java.text.DateFormatSymbols;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.WeekFields;
import java.util.List;
import java.util.Locale;

public class DateTimeFunction {
    /** English names of months and week days. */
    private static volatile String[][] MONTHS_AND_WEEKS;

    public static LocalDate currentDate() {
        return LocalDate.now();
    }

    public static LocalTime currentTime() {
        return LocalTime.now();
    }

    public static LocalDateTime currentTimestamp() {
        return LocalDateTime.now();
    }

    public static Object dateadd(List<Object> args) {
        Temporal datetime = (Temporal) args.get(0);
        if (datetime == null) {
            return null;
        }
        long count = ((Number) args.get(1)).longValue();
        String datetimeField = "DAY";
        if (args.size() >= 3) {
            String df = (String) args.get(2);
            if (df != null) {
                datetimeField = df.toUpperCase();
            }
        }
        switch (datetimeField) {
            case "YEAR":
                if (datetime instanceof LocalDate) {
                    return ((LocalDate) datetime).plusYears(count);
                }
                if (datetime instanceof LocalDateTime) {
                    return ((LocalDateTime) datetime).plusYears(count);
                }
                break;
            case "MONTH":
                if (datetime instanceof LocalDate) {
                    return ((LocalDate) datetime).plusMonths(count);
                }
                if (datetime instanceof LocalDateTime) {
                    return ((LocalDateTime) datetime).plusMonths(count);
                }
                break;
            case "WEEK":
                if (datetime instanceof LocalDate) {
                    return ((LocalDate) datetime).plusWeeks(count);
                }
                if (datetime instanceof LocalDateTime) {
                    return ((LocalDateTime) datetime).plusWeeks(count);
                }
                break;
            case "DAY":
                if (datetime instanceof LocalDate) {
                    return ((LocalDate) datetime).plusDays(count);
                }
                if (datetime instanceof LocalDateTime) {
                    return ((LocalDateTime) datetime).plusDays(count);
                }
                break;
            case "HOUR":
                if (datetime instanceof LocalTime) {
                    return ((LocalTime) datetime).plusHours(count);
                }
                if (datetime instanceof LocalDateTime) {
                    return ((LocalDateTime) datetime).plusHours(count);
                }
                break;
            case "MINUTE":
                if (datetime instanceof LocalTime) {
                    return ((LocalTime) datetime).plusMinutes(count);
                }
                if (datetime instanceof LocalDateTime) {
                    return ((LocalDateTime) datetime).plusMinutes(count);
                }
                break;
            case "SECOND":
                if (datetime instanceof LocalTime) {
                    return ((LocalTime) datetime).plusSeconds(count);
                }
                if (datetime instanceof LocalDateTime) {
                    return ((LocalDateTime) datetime).plusSeconds(count);
                }
                break;
            case "MILLISECOND":
                if (datetime instanceof LocalTime) {
                    return ((LocalTime) datetime).plusNanos(count * 1000_000L);
                }
                if (datetime instanceof LocalDateTime) {
                    return ((LocalDateTime) datetime).plusNanos(count * 1000_000L);
                }
                break;
            default:
                throw new TransformException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format(
                                "Unsupported dateTimeField: %s for function: %s",
                                datetimeField, ZetaSQLFunction.DATEDIFF));
        }
        return datetime;
    }

    public static Long datediff(List<Object> args) {
        Temporal datetime1 = (Temporal) args.get(0);
        if (datetime1 == null) {
            return null;
        }
        Temporal datetime2 = (Temporal) args.get(1);
        if (datetime2 == null) {
            return null;
        }
        String datetimeField = "DAY";
        if (args.size() >= 3) {
            String df = (String) args.get(2);
            if (df != null) {
                datetimeField = df.toUpperCase();
            }
        }

        LocalDate date1 = null;
        LocalDate date2 = null;
        if ("YEAR".equals(datetimeField)
                || "MONTH".equals(datetimeField)
                || "DAY".equals(datetimeField)) {
            if (datetime1 instanceof LocalDateTime) {
                date1 = ((LocalDateTime) datetime1).toLocalDate();
            }
            if (datetime1 instanceof LocalDate) {
                date1 = (LocalDate) datetime1;
            }
            if (datetime2 instanceof LocalDateTime) {
                date2 = ((LocalDateTime) datetime2).toLocalDate();
            }
            if (datetime2 instanceof LocalDate) {
                date2 = (LocalDate) datetime2;
            }
        }

        switch (datetimeField) {
            case "YEAR":
                if (date1 != null && date2 != null) {
                    return (long) Period.between(date1, date2).getYears();
                }
                break;
            case "MONTH":
                if (date1 != null && date2 != null) {
                    return (long) Period.between(date1, date2).getMonths();
                }
                break;
            case "WEEK":
                return Duration.between(datetime1, datetime2).toDays() / 7L;
            case "DAY":
                if (date1 != null && date2 != null) {
                    LocalTime lt = LocalTime.of(0, 0, 0);
                    LocalDateTime d1 = LocalDateTime.of(date1, lt);
                    LocalDateTime d2 = LocalDateTime.of(date2, lt);
                    return Duration.between(d1, d2).toDays();
                }
                break;
            case "DAYTIME":
                return Duration.between(datetime1, datetime2).toDays();
            case "HOUR":
                return Duration.between(datetime1, datetime2).toHours();
            case "MINUTE":
                return Duration.between(datetime1, datetime2).toMinutes();
            case "SECOND":
                return Duration.between(datetime1, datetime2).toMillis() / 1000L;
            case "MILLISECOND":
                return Duration.between(datetime1, datetime2).toMillis();
            default:
                throw new TransformException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format(
                                "Unsupported dateTimeField: %s for function: %s",
                                datetimeField, ZetaSQLFunction.DATEDIFF));
        }
        return null;
    }

    public static LocalDateTime dateTrunc(List<Object> args) {
        LocalDateTime datetime = (LocalDateTime) args.get(0);
        if (datetime == null) {
            return null;
        }
        String datetimeField = "DAY";
        if (args.size() >= 2) {
            String df = (String) args.get(1);
            if (df != null) {
                datetimeField = df.toUpperCase();
            }
        }
        int year = datetime.getYear();
        int month = datetime.getMonthValue();
        int day = datetime.getDayOfMonth();
        int hour = datetime.getHour();
        int minute = datetime.getMinute();
        int second = datetime.getSecond();

        switch (datetimeField) {
            case "YEAR":
                month = 1;
                day = 1;
                hour = 0;
                minute = 0;
                second = 0;
                break;
            case "MONTH":
                day = 1;
                hour = 0;
                minute = 0;
                second = 0;
                break;
            case "DAY":
                hour = 0;
                minute = 0;
                second = 0;
                break;
            case "HOUR":
                minute = 0;
                second = 0;
                break;
            case "MINUTE":
                second = 0;
                break;
            case "SECOND":
                break;
            default:
                throw new TransformException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format(
                                "Unsupported dateTimeField: %s for function: %s",
                                datetimeField, ZetaSQLFunction.DATEDIFF));
        }

        return LocalDateTime.of(year, month, day, hour, minute, second);
    }

    public static String dayname(List<Object> args) {
        Temporal datetime = (Temporal) args.get(0);
        if (datetime == null) {
            return null;
        }
        LocalDate localDate = convertToLocalDate(datetime);
        int dow = localDate.getDayOfWeek().getValue();
        dow++;
        if (dow == 8) {
            dow = 1;
        }
        return getMonthsAndWeeks(1)[dow];
    }

    private static String[] getMonthsAndWeeks(int field) {
        String[][] result = MONTHS_AND_WEEKS;
        if (result == null) {
            result = new String[2][];
            DateFormatSymbols dfs = DateFormatSymbols.getInstance(Locale.ENGLISH);
            result[0] = dfs.getMonths();
            result[1] = dfs.getWeekdays();
            MONTHS_AND_WEEKS = result;
        }
        return result[field];
    }

    private static LocalDate convertToLocalDate(Temporal datetime) {
        LocalDate localDate = null;
        if (datetime instanceof LocalDateTime) {
            localDate = ((LocalDateTime) datetime).toLocalDate();
        } else if (datetime instanceof LocalDate) {
            localDate = (LocalDate) datetime;
        }
        return localDate;
    }

    public static Integer dayOfMonth(List<Object> args) {
        Temporal datetime = (Temporal) args.get(0);
        if (datetime == null) {
            return null;
        }
        LocalDate localDate = convertToLocalDate(datetime);
        return localDate.getDayOfMonth();
    }

    public static Integer dayOfWeek(List<Object> args) {
        Temporal datetime = (Temporal) args.get(0);
        if (datetime == null) {
            return null;
        }
        LocalDate localDate = convertToLocalDate(datetime);
        return localDate.getDayOfWeek().getValue();
    }

    public static Integer dayOfYear(List<Object> args) {
        Temporal datetime = (Temporal) args.get(0);
        if (datetime == null) {
            return null;
        }
        LocalDate localDate = convertToLocalDate(datetime);
        return localDate.getDayOfYear();
    }

    public static Integer extract(List<Object> args) {
        Temporal datetime = (Temporal) args.get(0);
        if (datetime == null) {
            return null;
        }
        String datetimeField = (String) args.get(1);
        switch (datetimeField.toUpperCase()) {
            case "YEAR":
                if (datetime instanceof LocalDate) {
                    return ((LocalDate) datetime).getYear();
                }
                if (datetime instanceof LocalDateTime) {
                    return ((LocalDateTime) datetime).getYear();
                }
                break;
            case "MONTH":
                if (datetime instanceof LocalDate) {
                    return ((LocalDate) datetime).getMonthValue();
                }
                if (datetime instanceof LocalDateTime) {
                    return ((LocalDateTime) datetime).getMonthValue();
                }
                break;
            case "DAY":
                if (datetime instanceof LocalDate) {
                    return ((LocalDate) datetime).getDayOfMonth();
                }
                if (datetime instanceof LocalDateTime) {
                    return ((LocalDateTime) datetime).getDayOfMonth();
                }
                break;
            case "HOUR":
                if (datetime instanceof LocalTime) {
                    return ((LocalTime) datetime).getHour();
                }
                if (datetime instanceof LocalDateTime) {
                    return ((LocalDateTime) datetime).getHour();
                }
                break;
            case "MINUTE":
                if (datetime instanceof LocalTime) {
                    return ((LocalTime) datetime).getMinute();
                }
                if (datetime instanceof LocalDateTime) {
                    return ((LocalDateTime) datetime).getMinute();
                }
                break;
            case "SECOND":
                if (datetime instanceof LocalTime) {
                    return ((LocalTime) datetime).getSecond();
                }
                if (datetime instanceof LocalDateTime) {
                    return ((LocalDateTime) datetime).getSecond();
                }
                break;
            case "MILLISECOND":
                if (datetime instanceof LocalTime) {
                    return ((LocalTime) datetime).getNano() / 1000_000;
                }
                if (datetime instanceof LocalDateTime) {
                    return ((LocalDateTime) datetime).getNano() / 1000_000;
                }
                break;
            case "DAYOFWEEK":
                return dayOfWeek(args);
            case "DAYOFYEAR":
                return dayOfYear(args);
            default:
                throw new TransformException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format(
                                "Unsupported dateTimeField: %s for function: %s",
                                datetimeField, ZetaSQLFunction.EXTRACT));
        }
        return null;
    }

    public static String formatdatetime(List<Object> args) {
        TemporalAccessor datetime = (TemporalAccessor) args.get(0);
        if (datetime == null) {
            return null;
        }
        String format = (String) args.get(1);
        DateTimeFormatter df = DateTimeFormatter.ofPattern(format);
        return df.format(datetime);
    }

    public static Integer hour(List<Object> args) {
        Temporal datetime = (Temporal) args.get(0);
        if (datetime == null) {
            return null;
        }
        LocalTime localTime = convertToLocalTime(datetime);
        return localTime.getHour();
    }

    private static LocalTime convertToLocalTime(Temporal datetime) {
        LocalTime localTime = null;
        if (datetime instanceof LocalDateTime) {
            localTime = ((LocalDateTime) datetime).toLocalTime();
        } else if (datetime instanceof LocalTime) {
            localTime = (LocalTime) datetime;
        }
        return localTime;
    }

    public static Integer minute(List<Object> args) {
        Temporal datetime = (Temporal) args.get(0);
        if (datetime == null) {
            return null;
        }
        LocalTime localTime = convertToLocalTime(datetime);
        return localTime.getMinute();
    }

    public static Integer month(List<Object> args) {
        Temporal datetime = (Temporal) args.get(0);
        if (datetime == null) {
            return null;
        }
        LocalDate localDate = convertToLocalDate(datetime);
        return localDate.getMonthValue();
    }

    public static String monthname(List<Object> args) {
        Temporal datetime = (Temporal) args.get(0);
        if (datetime == null) {
            return null;
        }
        LocalDate localDate = convertToLocalDate(datetime);
        int dow = localDate.getMonthValue();
        return getMonthsAndWeeks(0)[dow - 1];
    }

    public static Temporal parsedatetime(List<Object> args) {
        String str = (String) args.get(0);
        if (str == null) {
            return null;
        }
        String format = (String) args.get(1);
        if (format.contains("yy") && format.contains("mm")) {
            DateTimeFormatter df = DateTimeFormatter.ofPattern(format);
            return LocalDateTime.parse(str, df);
        }
        if (format.contains("yy")) {
            DateTimeFormatter df = DateTimeFormatter.ofPattern(format);
            return LocalDate.parse(str, df);
        }
        if (format.contains("mm")) {
            DateTimeFormatter df = DateTimeFormatter.ofPattern(format);
            return LocalTime.parse(str, df);
        }
        throw new TransformException(
                CommonErrorCode.UNSUPPORTED_OPERATION,
                String.format(
                        "Unknown pattern letter %s for function: %s",
                        format, ZetaSQLFunction.PARSEDATETIME));
    }

    public static Integer quarter(List<Object> args) {
        Temporal datetime = (Temporal) args.get(0);
        if (datetime == null) {
            return null;
        }
        LocalDate localDate = convertToLocalDate(datetime);
        int month = localDate.getMonthValue();
        if (month <= 3) {
            return 1;
        }
        if (month <= 6) {
            return 2;
        }
        if (month <= 9) {
            return 3;
        }
        return 4;
    }

    public static Integer second(List<Object> args) {
        Temporal datetime = (Temporal) args.get(0);
        if (datetime == null) {
            return null;
        }
        LocalTime localTime = convertToLocalTime(datetime);
        return localTime.getSecond();
    }

    public static Integer week(List<Object> args) {
        Temporal datetime = (Temporal) args.get(0);
        if (datetime == null) {
            return null;
        }
        LocalDate localDate = convertToLocalDate(datetime);
        WeekFields weekFields = WeekFields.ISO;
        return localDate.get(weekFields.weekOfYear()) + 1;
    }

    public static Integer year(List<Object> args) {
        Temporal datetime = (Temporal) args.get(0);
        if (datetime == null) {
            return null;
        }
        LocalDate localDate = convertToLocalDate(datetime);
        return localDate.getYear();
    }
}
