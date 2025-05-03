package org.apache.seatunnel.connectors.seatunnel.maxcompute.util;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

public class FormatterContext {
    private final String localDateTimeFormat;
    private final String offsetDateTimeFormat;

    public FormatterContext(String localDateTimeFormat, String offsetDateTimeFormat) {
        this.localDateTimeFormat = localDateTimeFormat;
        this.offsetDateTimeFormat = offsetDateTimeFormat;
    }

    public boolean isDateTimeType(Object field) {
        if (field instanceof LocalDateTime) {
            return true;
        }
        if (field instanceof OffsetDateTime) {
            return true;
        }
        return false;
    }

    public String formatDateTime(Object field) {
        if (field instanceof LocalDateTime) {
            return this.format(((LocalDateTime) field));
        }
        if (field instanceof OffsetDateTime) {
            return this.format(((OffsetDateTime) field));
        }
        return String.valueOf(field);
    }

    private String format(LocalDateTime localDateTime) {
        return localDateTime.format(DateTimeFormatter.ofPattern(localDateTimeFormat));
    }

    private String format(OffsetDateTime offsetDateTime) {
        return offsetDateTime.format(DateTimeFormatter.ofPattern(offsetDateTimeFormat));
    }
}
