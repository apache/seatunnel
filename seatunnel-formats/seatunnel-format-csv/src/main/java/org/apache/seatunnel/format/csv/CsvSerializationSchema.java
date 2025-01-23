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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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

    private CsvSerializationSchema(
            @NonNull SeaTunnelRowType seaTunnelRowType,
            String[] separators,
            DateUtils.Formatter dateFormatter,
            DateTimeUtils.Formatter dateTimeFormatter,
            TimeUtils.Formatter timeFormatter,
            Charset charset,
            String nullValue,
            CsvStringQuoteMode quoteMode) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.separators = separators;
        this.dateFormatter = dateFormatter;
        this.dateTimeFormatter = dateTimeFormatter;
        this.timeFormatter = timeFormatter;
        this.charset = charset;
        this.nullValue = nullValue;
        this.quoteMode = quoteMode;
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
            this.separators = separators;
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

        public CsvSerializationSchema build() {
            return new CsvSerializationSchema(
                    seaTunnelRowType,
                    separators,
                    dateFormatter,
                    dateTimeFormatter,
                    timeFormatter,
                    charset,
                    nullValue,
                    quoteMode);
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
            case DECIMAL:
                return field.toString();
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

    private String addQuotesUsingCSVFormat(String fieldValue) {
        CSVFormat.Builder builder = CSVFormat.DEFAULT.builder().setRecordSeparator("");
        switch (quoteMode) {
            case ALL:
                builder.setQuoteMode(QuoteMode.ALL);
                break;
            case MINIMAL:
                builder.setQuoteMode(QuoteMode.MINIMAL);
                break;
            case NONE:
                builder.setQuoteMode(QuoteMode.NONE);
                break;
            default:
                throw new SeaTunnelCsvFormatException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        String.format(
                                "SeaTunnel format csv not supported for parsing this type [%s]",
                                quoteMode));
        }
        CSVFormat format = builder.build();
        StringWriter stringWriter = new StringWriter();
        try (CSVPrinter printer = new CSVPrinter(stringWriter, format)) {
            printer.printRecord(fieldValue);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return stringWriter.toString();
    }
}
