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

package org.apache.seatunnel.format.csv;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.format.csv.constant.CsvFormatConstant;
import org.apache.seatunnel.format.csv.constant.CsvStringQuoteMode;
import org.apache.seatunnel.format.csv.exception.SeaTunnelCsvFormatException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;

import lombok.NonNull;

import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class CsvSerializationSchema implements SerializationSchema {

    private final SeaTunnelRowType seaTunnelRowType;
    private final String[] separators;
    private final DateUtils.Formatter dateFormatter;
    private final DateTimeUtils.Formatter dateTimeFormatter;
    private final TimeUtils.Formatter timeFormatter;
    private final Charset charset;
    private final String nullValue;
    private final CsvStringQuoteMode quoteMode;
    /**
     * Immutable quoting format built once for {@link CsvStringQuoteMode#ALL} and {@link
     * CsvStringQuoteMode#MINIMAL}; {@code null} for {@link CsvStringQuoteMode#NONE}, which is built
     * lazily per record to preserve the existing fail-on-serialize semantics of NONE.
     */
    private final CSVFormat quoteFormat;
    /** When true, TIMESTAMP_TZ is serialized as wall-clock (no offset) for DB sinks like Doris. */
    private final boolean wallClockTimestampTz;

    /**
     * Zone used when {@link #wallClockTimestampTz} is true to drop the offset and emit a wall-clock
     * {@code LocalDateTime}. Defaults to {@link ZoneId#systemDefault()} for backward compatibility;
     * callers that know the target session zone (for example the Doris sink session timezone)
     * should pass it explicitly so the JVM default is not silently relied on.
     */
    private final ZoneId wallClockTimestampTzZoneId;

    private CsvSerializationSchema(
            @NonNull SeaTunnelRowType seaTunnelRowType,
            String[] separators,
            DateUtils.Formatter dateFormatter,
            DateTimeUtils.Formatter dateTimeFormatter,
            TimeUtils.Formatter timeFormatter,
            Charset charset,
            String nullValue,
            CsvStringQuoteMode quoteMode,
            boolean wallClockTimestampTz,
            ZoneId wallClockTimestampTzZoneId) {
        this.seaTunnelRowType = seaTunnelRowType;
        // Defensive copy so that mutating the caller's (or builder's) array afterwards cannot
        // desync the field delimiter used for joining from the one used for quoting below.
        this.separators = separators.clone();
        this.dateFormatter = dateFormatter;
        this.dateTimeFormatter = dateTimeFormatter;
        this.timeFormatter = timeFormatter;
        this.charset = charset;
        this.nullValue = nullValue;
        this.quoteMode = quoteMode;
        this.quoteFormat = buildQuoteFormat();
        this.wallClockTimestampTz = wallClockTimestampTz;
        this.wallClockTimestampTzZoneId =
                wallClockTimestampTzZoneId == null
                        ? ZoneId.systemDefault()
                        : wallClockTimestampTzZoneId;
    }

    /**
     * Validates the field delimiter and builds the immutable quoting format once at construction
     * time, so that a mis-configured delimiter fails fast when the sink is initialized instead of
     * throwing a raw exception on the first serialized string field of every row.
     *
     * <p>For {@link CsvStringQuoteMode#NONE} this returns {@code null} and the format is built
     * lazily in {@link #addQuotesUsingCSVFormat(String)}: eagerly building it would move NONE's
     * fail on a missing escape character from serialization to initialization and break string-free
     * NONE sinks, which never reach the quoting path.
     *
     * @return the cached quoting format, or {@code null} for {@code NONE}
     */
    private CSVFormat buildQuoteFormat() {
        String delimiter = separators[0];
        if (delimiter == null || delimiter.isEmpty()) {
            throw new SeaTunnelCsvFormatException(
                    CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                    String.format(
                            "The csv option [field_delimiter] must be a non-empty string, but was [%s]",
                            delimiter));
        }
        if (quoteMode == CsvStringQuoteMode.NONE) {
            return null;
        }
        QuoteMode commonsQuoteMode;
        switch (quoteMode) {
            case ALL:
                commonsQuoteMode = QuoteMode.ALL;
                break;
            case MINIMAL:
                commonsQuoteMode = QuoteMode.MINIMAL;
                break;
            default:
                throw new SeaTunnelCsvFormatException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        String.format(
                                "SeaTunnel format csv not supported for parsing this type [%s]",
                                quoteMode));
        }
        try {
            return CSVFormat.DEFAULT
                    .builder()
                    .setRecordSeparator("")
                    .setDelimiter(delimiter.charAt(0))
                    .setQuoteMode(commonsQuoteMode)
                    .build();
        } catch (IllegalArgumentException e) {
            throw new SeaTunnelCsvFormatException(
                    CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                    String.format(
                            "The csv option [field_delimiter] value [%s] is invalid for quote mode [%s]: %s",
                            delimiter, quoteMode, e.getMessage()),
                    e);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private SeaTunnelRowType seaTunnelRowType;
        private String[] separators = CsvFormatConstant.SEPARATOR.clone();
        private DateUtils.Formatter dateFormatter = DateUtils.Formatter.YYYY_MM_DD;
        private DateTimeUtils.Formatter dateTimeFormatter =
                DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS;
        private TimeUtils.Formatter timeFormatter = TimeUtils.Formatter.HH_MM_SS;
        private Charset charset = StandardCharsets.UTF_8;
        private String nullValue = "";
        private CsvStringQuoteMode quoteMode = CsvStringQuoteMode.MINIMAL;
        private boolean wallClockTimestampTz = false;
        private ZoneId wallClockTimestampTzZoneId = null;

        private Builder() {}

        public Builder seaTunnelRowType(SeaTunnelRowType seaTunnelRowType) {
            this.seaTunnelRowType = seaTunnelRowType;
            return this;
        }

        public Builder delimiter(String delimiter) {
            this.separators[0] = delimiter;
            return this;
        }

        public Builder separators(String[] separators) {
            this.separators = separators.clone();
            return this;
        }

        public Builder dateFormatter(DateUtils.Formatter dateFormatter) {
            this.dateFormatter = dateFormatter;
            return this;
        }

        public Builder dateTimeFormatter(DateTimeUtils.Formatter dateTimeFormatter) {
            this.dateTimeFormatter = dateTimeFormatter;
            return this;
        }

        public Builder timeFormatter(TimeUtils.Formatter timeFormatter) {
            this.timeFormatter = timeFormatter;
            return this;
        }

        public Builder charset(Charset charset) {
            this.charset = charset;
            return this;
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        public Builder quoteMode(CsvStringQuoteMode quoteMode) {
            this.quoteMode = quoteMode;
            return this;
        }

        /**
         * When set to true, TIMESTAMP_TZ fields are serialized as wall-clock local datetime
         * (without offset) for timezone-unaware DB sinks such as Doris.
         */
        public Builder wallClockTimestampTz(boolean wallClockTimestampTz) {
            this.wallClockTimestampTz = wallClockTimestampTz;
            return this;
        }

        /**
         * Sets the target zone used when {@link #wallClockTimestampTz(boolean)} is true. Pass the
         * actual session zone (for example the Doris sink session timezone) so the JVM default is
         * not silently relied on. If unset, {@link ZoneId#systemDefault()} is used.
         */
        public Builder wallClockTimestampTzZoneId(ZoneId wallClockTimestampTzZoneId) {
            this.wallClockTimestampTzZoneId = wallClockTimestampTzZoneId;
            return this;
        }

        public CsvSerializationSchema build() {
            return new CsvSerializationSchema(
                    seaTunnelRowType,
                    separators,
                    dateFormatter,
                    dateTimeFormatter,
                    timeFormatter,
                    charset,
                    nullValue,
                    quoteMode,
                    wallClockTimestampTz,
                    wallClockTimestampTzZoneId);
        }
    }

    @Override
    public byte[] serialize(SeaTunnelRow element) {
        if (element.getFields().length != seaTunnelRowType.getTotalFields()) {
            throw new IndexOutOfBoundsException(
                    "The data does not match the configured schema information, please check");
        }
        Object[] fields = element.getFields();
        String[] strings = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            strings[i] = convert(fields[i], seaTunnelRowType.getFieldType(i), 0);
        }
        return String.join(separators[0], strings).getBytes(charset);
    }

    private String convert(Object field, SeaTunnelDataType<?> fieldType, int level) {
        if (field == null) {
            return nullValue;
        }
        switch (fieldType.getSqlType()) {
            case DOUBLE:
            case FLOAT:
            case INT:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case BIGINT:
                return field.toString();
            case DECIMAL:
                BigDecimal bd = (BigDecimal) field;
                return bd.stripTrailingZeros().toPlainString();
            case STRING:
                byte[] bytes = field.toString().getBytes(StandardCharsets.UTF_8);
                String str = new String(bytes, StandardCharsets.UTF_8);
                // Focus only on the base string
                return level == 0 ? addQuotesUsingCSVFormat(str) : str;
            case DATE:
                return DateUtils.toString((LocalDate) field, dateFormatter);
            case TIME:
                return TimeUtils.toString((LocalTime) field, timeFormatter);
            case TIMESTAMP:
                return DateTimeUtils.toString((LocalDateTime) field, dateTimeFormatter);
            case TIMESTAMP_TZ:
                OffsetDateTime odt = (OffsetDateTime) field;
                if (wallClockTimestampTz) {
                    // Preserve the instant and convert to the target zone (the Doris
                    // session zone when supplied, otherwise the JVM default for backward
                    // compatibility). A plain toLocalDateTime() would emit the wall-clock
                    // of the value's own offset (e.g. UTC), which shifts TIMESTAMP_TZ
                    // columns by the timezone delta once a sink such as Doris parses the
                    // wall-clock in its session timezone (issue #10795).
                    return DateTimeUtils.toString(
                            odt.atZoneSameInstant(wallClockTimestampTzZoneId).toLocalDateTime(),
                            dateTimeFormatter);
                }
                return odt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            case NULL:
                return "";
            case BYTES:
                return new String((byte[]) field, StandardCharsets.UTF_8);
            case ARRAY:
                SeaTunnelDataType<?> elementType = ((ArrayType<?, ?>) fieldType).getElementType();
                return Arrays.stream((Object[]) field)
                        .map(f -> convert(f, elementType, level + 1))
                        .collect(Collectors.joining(separators[level + 1]));
            case MAP:
                SeaTunnelDataType<?> keyType = ((MapType<?, ?>) fieldType).getKeyType();
                SeaTunnelDataType<?> valueType = ((MapType<?, ?>) fieldType).getValueType();
                return ((Map<Object, Object>) field)
                        .entrySet().stream()
                                .map(
                                        entry ->
                                                String.join(
                                                        separators[level + 2],
                                                        convert(entry.getKey(), keyType, level + 1),
                                                        convert(
                                                                entry.getValue(),
                                                                valueType,
                                                                level + 1)))
                                .collect(Collectors.joining(separators[level + 1]));
            case ROW:
                Object[] fields = ((SeaTunnelRow) field).getFields();
                String[] strings = new String[fields.length];
                for (int i = 0; i < fields.length; i++) {
                    strings[i] =
                            convert(
                                    fields[i],
                                    ((SeaTunnelRowType) fieldType).getFieldType(i),
                                    level + 1);
                }
                return String.join(separators[level + 1], strings);
            default:
                throw new SeaTunnelCsvFormatException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        String.format(
                                "SeaTunnel format text not supported for parsing this type [%s]",
                                fieldType.getSqlType()));
        }
    }

    /**
     * Quotes the given top-level string field value according to the configured quote mode.
     *
     * <p>Top-level fields are joined with the configured separator and are split back by {@code
     * CsvReadStrategy}, which parses the file with a {@code CSVParser} whose delimiter is the full
     * {@code field_delimiter} string. Nested ROW/ARRAY/MAP levels are split by {@code
     * DefaultCsvLineProcessor}, which uses only the first character of the separator. The printer
     * therefore uses the first character of the configured separator as its delimiter: for the
     * common single-character case this matches both readers exactly, and for a multi-character
     * separator it quotes a superset of the values that contain the full separator, so it can only
     * over-quote (never under-quote) and values still read back as a single field.
     */
    private String addQuotesUsingCSVFormat(String fieldValue) {
        CSVFormat format = quoteFormat;
        if (format == null) {
            // NONE: built lazily so its fail-on-serialize semantics for a missing escape
            // character are preserved and string-free NONE sinks keep working.
            format =
                    CSVFormat.DEFAULT
                            .builder()
                            .setRecordSeparator("")
                            .setDelimiter(separators[0].charAt(0))
                            .setQuoteMode(QuoteMode.NONE)
                            .build();
        }
        StringWriter stringWriter = new StringWriter();
        try (CSVPrinter printer = new CSVPrinter(stringWriter, format)) {
            printer.printRecord(fieldValue);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return stringWriter.toString();
    }
}
