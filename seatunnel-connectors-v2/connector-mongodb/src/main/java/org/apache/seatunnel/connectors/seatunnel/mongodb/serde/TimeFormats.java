package org.apache.seatunnel.connectors.seatunnel.mongodb.serde;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

public class TimeFormats {

    /** Formatter for RFC 3339-compliant string representation of a time value. */
    public static final DateTimeFormatter RFC3339_TIME_FORMAT =
            new DateTimeFormatterBuilder()
                    .appendPattern("HH:mm:ss")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .appendPattern("'Z'")
                    .toFormatter();

    /**
     * Formatter for RFC 3339-compliant string representation of a timestamp value (with UTC
     * timezone).
     */
    public static final DateTimeFormatter RFC3339_TIMESTAMP_FORMAT =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral('T')
                    .append(RFC3339_TIME_FORMAT)
                    .toFormatter();

    /** Formatter for ISO8601 string representation of a timestamp value (without UTC timezone). */
    public static final DateTimeFormatter ISO8601_TIMESTAMP_FORMAT =
            DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    /** Formatter for ISO8601 string representation of a timestamp value (with UTC timezone). */
    public static final DateTimeFormatter ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral('T')
                    .append(DateTimeFormatter.ISO_LOCAL_TIME)
                    .appendPattern("'Z'")
                    .toFormatter();

    /** Formatter for SQL string representation of a time value. */
    public static final DateTimeFormatter SQL_TIME_FORMAT =
            new DateTimeFormatterBuilder()
                    .appendPattern("HH:mm:ss")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .toFormatter();

    /** Formatter for SQL string representation of a timestamp value (without UTC timezone). */
    public static final DateTimeFormatter SQL_TIMESTAMP_FORMAT =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .append(SQL_TIME_FORMAT)
                    .toFormatter();

    /** Formatter for SQL string representation of a timestamp value (with UTC timezone). */
    public static final DateTimeFormatter SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .append(SQL_TIME_FORMAT)
                    .appendPattern("'Z'")
                    .toFormatter();

    private TimeFormats() {}
}
