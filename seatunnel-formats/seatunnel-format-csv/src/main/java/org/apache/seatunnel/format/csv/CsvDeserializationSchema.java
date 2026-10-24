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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.EncodingUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.format.csv.constant.CsvFormatConstant;
import org.apache.seatunnel.format.csv.exception.SeaTunnelCsvFormatException;
import org.apache.seatunnel.format.csv.processor.CsvLineProcessor;
import org.apache.seatunnel.format.csv.processor.DefaultCsvLineProcessor;

import lombok.NonNull;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class CsvDeserializationSchema implements Serializable {
    private final SeaTunnelRowType seaTunnelRowType;
    private final String[] separators;
    private final String encoding;
    private final String nullFormat;
    private final CsvLineProcessor processor;
    private final CatalogTable catalogTable;
    private final Object[] defaultValues;

    @SuppressWarnings("MagicNumber")
    public static final DateTimeFormatter TIME_FORMAT =
            new DateTimeFormatterBuilder()
                    .appendPattern("HH:mm:ss")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .toFormatter();

    // Formatters are derived from file data and are not serializable.
    public transient Map<String, DateTimeFormatter> fieldFormatterMap = new HashMap<>();

    private CsvDeserializationSchema(
            @NonNull SeaTunnelRowType seaTunnelRowType,
            String[] separators,
            String encoding,
            String nullFormat,
            CsvLineProcessor processor,
            CatalogTable catalogTable) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.separators = separators;
        this.encoding = encoding;
        this.nullFormat = nullFormat;
        this.processor = processor;
        this.catalogTable = catalogTable;
        Map<String, Column> columnsByName = new HashMap<>();
        if (catalogTable != null) {
            for (Column column : catalogTable.getTableSchema().getColumns()) {
                columnsByName.put(column.getName(), column);
            }
        }
        this.defaultValues = new Object[seaTunnelRowType.getTotalFields()];
        for (int i = 0; i < defaultValues.length; i++) {
            String fieldName = seaTunnelRowType.getFieldName(i);
            defaultValues[i] =
                    resolveDefaultValue(
                            columnsByName.get(fieldName),
                            seaTunnelRowType.getFieldType(i),
                            fieldName);
        }
    }

    private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException {
        input.defaultReadObject();
        fieldFormatterMap = new HashMap<>();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private SeaTunnelRowType seaTunnelRowType;
        private CatalogTable catalogTable;
        private String[] separators = CsvFormatConstant.SEPARATOR.clone();
        private DateUtils.Formatter dateFormatter = DateUtils.Formatter.YYYY_MM_DD;
        private DateTimeUtils.Formatter dateTimeFormatter =
                DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS;
        private TimeUtils.Formatter timeFormatter = TimeUtils.Formatter.HH_MM_SS;
        private String encoding = StandardCharsets.UTF_8.name();
        private String nullFormat;
        private CsvLineProcessor csvLineProcessor = new DefaultCsvLineProcessor();

        private Builder() {}

        public Builder setCatalogTable(CatalogTable catalogTable) {
            this.catalogTable = catalogTable;
            return this;
        }

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

        public Builder encoding(String encoding) {
            this.encoding = encoding;
            return this;
        }

        public Builder nullFormat(String nullFormat) {
            this.nullFormat = nullFormat;
            return this;
        }

        public Builder csvLineProcessor(CsvLineProcessor csvLineProcessor) {
            this.csvLineProcessor = csvLineProcessor;
            return this;
        }

        public CsvDeserializationSchema build() {
            return new CsvDeserializationSchema(
                    seaTunnelRowType,
                    separators,
                    encoding,
                    nullFormat,
                    csvLineProcessor,
                    catalogTable);
        }
    }

    protected SeaTunnelRow deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null;
        }
        String content = new String(message, EncodingUtils.tryParseCharset(encoding));
        Map<Integer, String> splitsMap = splitLineBySeaTunnelRowType(content, seaTunnelRowType, 0);
        SeaTunnelRow seaTunnelRow = getSeaTunnelRow(splitsMap);
        return seaTunnelRow;
    }

    public SeaTunnelRow getSeaTunnelRow(Map<Integer, String> splitsMap) {
        Object[] objects = new Object[seaTunnelRowType.getTotalFields()];
        for (int i = 0; i < objects.length; i++) {
            String fieldValue = splitsMap.get(i);
            if (fieldValue == null || StringUtils.equals(fieldValue, nullFormat)) {
                objects[i] = getDefaultValue(i);
                continue;
            }
            if (StringUtils.isBlank(fieldValue)) {
                continue;
            }
            objects[i] =
                    convert(
                            fieldValue,
                            seaTunnelRowType.getFieldType(i),
                            0,
                            seaTunnelRowType.getFieldNames()[i]);
        }
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(objects);
        Optional<TablePath> tablePath =
                Optional.ofNullable(catalogTable).map(CatalogTable::getTablePath);
        if (tablePath.isPresent()) {
            seaTunnelRow.setTableId(tablePath.toString());
        }
        return seaTunnelRow;
    }

    private Object getDefaultValue(int fieldIndex) {
        return copyDefaultValue(defaultValues[fieldIndex]);
    }

    /** Resolves defaults once, independently of the format learned from file data. */
    private Object resolveDefaultValue(
            Column column, SeaTunnelDataType<?> fieldType, String fieldName) {
        if (column == null || column.getDefaultValue() == null) {
            return null;
        }
        try {
            return convertDefaultValue(column.getDefaultValue(), fieldType, fieldName);
        } catch (RuntimeException ignored) {
            // Parser exceptions can include the configured value in their messages.
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid defaultValue for CSV column [%s] of type [%s]",
                            fieldName, fieldType));
        }
    }

    private Object copyDefaultValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return ((byte[]) value).clone();
        }
        if (value.getClass().isArray()) {
            int length = Array.getLength(value);
            Object copy = Array.newInstance(value.getClass().getComponentType(), length);
            for (int i = 0; i < length; i++) {
                Array.set(copy, i, copyDefaultValue(Array.get(value, i)));
            }
            return copy;
        }
        if (value instanceof Map) {
            Map<Object, Object> copy = new LinkedHashMap<>();
            ((Map<?, ?>) value)
                    .forEach(
                            (key, mapValue) ->
                                    copy.put(copyDefaultValue(key), copyDefaultValue(mapValue)));
            return copy;
        }
        if (value instanceof SeaTunnelRow) {
            Object[] fields = ((SeaTunnelRow) value).getFields();
            return new SeaTunnelRow((Object[]) copyDefaultValue(fields));
        }
        return value;
    }

    private Object convertDefaultValue(
            Object value, SeaTunnelDataType<?> fieldType, String fieldName) {
        if (value == null) {
            return null;
        }
        switch (fieldType.getSqlType()) {
            case STRING:
                return value.toString();
            case BOOLEAN:
                if (!"true".equalsIgnoreCase(value.toString())
                        && !"false".equalsIgnoreCase(value.toString())) {
                    throw new IllegalArgumentException("Boolean default must be true or false");
                }
                return Boolean.parseBoolean(value.toString());
            case DATE:
                if (value instanceof LocalDate) {
                    return value;
                }
                break;
            case TIME:
                if (value instanceof LocalTime) {
                    return value;
                }
                break;
            case TIMESTAMP:
                if (value instanceof LocalDateTime) {
                    return value;
                }
                break;
            case TIMESTAMP_TZ:
                if (value instanceof OffsetDateTime) {
                    return value;
                }
                break;
            case BYTES:
                if (value instanceof byte[]) {
                    return ((byte[]) value).clone();
                }
                if (value instanceof String) {
                    return ((String) value).getBytes(StandardCharsets.UTF_8);
                }
                break;
            case ARRAY:
                if (value instanceof Collection || value.getClass().isArray()) {
                    ArrayType<?, ?> arrayType = (ArrayType<?, ?>) fieldType;
                    SeaTunnelDataType<?> elementType = arrayType.getElementType();
                    int length =
                            value instanceof Collection
                                    ? ((Collection<?>) value).size()
                                    : Array.getLength(value);
                    Object result = Array.newInstance(elementType.getTypeClass(), length);
                    int index = 0;
                    if (value instanceof Collection) {
                        for (Object element : (Collection<?>) value) {
                            Array.set(
                                    result,
                                    index++,
                                    convertDefaultValue(element, elementType, fieldName));
                        }
                    } else {
                        for (int i = 0; i < length; i++) {
                            Array.set(
                                    result,
                                    i,
                                    convertDefaultValue(
                                            Array.get(value, i), elementType, fieldName));
                        }
                    }
                    return result;
                }
                break;
            case MAP:
                if (value instanceof Map) {
                    MapType<?, ?> mapType = (MapType<?, ?>) fieldType;
                    Map<Object, Object> result = new LinkedHashMap<>();
                    ((Map<?, ?>) value)
                            .forEach(
                                    (key, mapValue) ->
                                            result.put(
                                                    convertDefaultValue(
                                                            key, mapType.getKeyType(), fieldName),
                                                    convertDefaultValue(
                                                            mapValue,
                                                            mapType.getValueType(),
                                                            fieldName)));
                    return result;
                }
                break;
            case ROW:
                if (value instanceof Map || value instanceof SeaTunnelRow) {
                    SeaTunnelRowType rowType = (SeaTunnelRowType) fieldType;
                    Object[] fields = new Object[rowType.getTotalFields()];
                    for (int i = 0; i < fields.length; i++) {
                        Object fieldValue =
                                value instanceof Map
                                        ? ((Map<?, ?>) value).get(rowType.getFieldName(i))
                                        : ((SeaTunnelRow) value).getField(i);
                        fields[i] =
                                convertDefaultValue(
                                        fieldValue,
                                        rowType.getFieldType(i),
                                        fieldName + "." + rowType.getFieldName(i));
                    }
                    return new SeaTunnelRow(fields);
                }
                break;
            case NULL:
                return null;
            default:
                break;
        }
        Object converted = convert(value.toString(), fieldType, 0, fieldName, true);
        if (converted == null) {
            throw new IllegalArgumentException(
                    "Default value cannot be converted to CSV field type");
        }
        return converted;
    }

    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return seaTunnelRowType;
    }

    protected Map<Integer, String> splitLineBySeaTunnelRowType(
            String line, SeaTunnelRowType seaTunnelRowType, int level) {
        String[] splits = processor.splitLine(line, separators[level]);
        LinkedHashMap<Integer, String> splitsMap = new LinkedHashMap<>();
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        for (int i = 0; i < splits.length; i++) {
            splitsMap.put(i, splits[i]);
        }
        if (fieldTypes.length > splits.length) {
            // contains partition columns
            for (int i = splits.length; i < fieldTypes.length; i++) {
                splitsMap.put(i, null);
            }
        }
        return splitsMap;
    }

    private Object convert(
            String field, SeaTunnelDataType<?> fieldType, int level, String fieldName) {
        return convert(field, fieldType, level, fieldName, false);
    }

    private Object convert(
            String field,
            SeaTunnelDataType<?> fieldType,
            int level,
            String fieldName,
            boolean isDefaultValue) {
        if (StringUtils.isBlank(field)) {
            return null;
        }
        switch (fieldType.getSqlType()) {
            case ARRAY:
                SeaTunnelDataType<?> elementType = ((ArrayType<?, ?>) fieldType).getElementType();
                String[] elements = field.split(separators[level + 1]);
                ArrayList<Object> objectArrayList = new ArrayList<>();
                for (String element : elements) {
                    objectArrayList.add(
                            convert(element, elementType, level + 1, fieldName, isDefaultValue));
                }
                switch (elementType.getSqlType()) {
                    case STRING:
                        return objectArrayList.toArray(new String[0]);
                    case BOOLEAN:
                        return objectArrayList.toArray(new Boolean[0]);
                    case TINYINT:
                        return objectArrayList.toArray(new Byte[0]);
                    case SMALLINT:
                        return objectArrayList.toArray(new Short[0]);
                    case INT:
                        return objectArrayList.toArray(new Integer[0]);
                    case BIGINT:
                        return objectArrayList.toArray(new Long[0]);
                    case FLOAT:
                        return objectArrayList.toArray(new Float[0]);
                    case DOUBLE:
                        return objectArrayList.toArray(new Double[0]);
                    case DECIMAL:
                        return objectArrayList.toArray(new BigDecimal[0]);
                    case DATE:
                        return objectArrayList.toArray(new LocalDate[0]);
                    case TIME:
                        return objectArrayList.toArray(new LocalTime[0]);
                    case TIMESTAMP:
                        return objectArrayList.toArray(new LocalDateTime[0]);
                    case TIMESTAMP_TZ:
                        return objectArrayList.toArray(new OffsetDateTime[0]);
                    default:
                        throw new SeaTunnelCsvFormatException(
                                CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                String.format(
                                        "SeaTunnel array not support this data type [%s]",
                                        elementType.getSqlType()));
                }
            case MAP:
                SeaTunnelDataType<?> keyType = ((MapType<?, ?>) fieldType).getKeyType();
                SeaTunnelDataType<?> valueType = ((MapType<?, ?>) fieldType).getValueType();
                LinkedHashMap<Object, Object> objectMap = new LinkedHashMap<>();
                String[] kvs = field.split(separators[level + 1]);
                for (String kv : kvs) {
                    String[] splits = kv.split(separators[level + 2]);
                    if (splits.length < 2) {
                        objectMap.put(
                                convert(splits[0], keyType, level + 1, fieldName, isDefaultValue),
                                null);
                    } else {
                        objectMap.put(
                                convert(splits[0], keyType, level + 1, fieldName, isDefaultValue),
                                convert(
                                        splits[1],
                                        valueType,
                                        level + 1,
                                        fieldName,
                                        isDefaultValue));
                    }
                }
                return objectMap;
            case STRING:
                return field;
            case BOOLEAN:
                return Boolean.parseBoolean(field);
            case TINYINT:
                return Byte.parseByte(field);
            case SMALLINT:
                return Short.parseShort(field);
            case INT:
                return Integer.parseInt(field);
            case BIGINT:
                return Long.parseLong(field);
            case FLOAT:
                return Float.parseFloat(field);
            case DOUBLE:
                return Double.parseDouble(field);
            case DECIMAL:
                return new BigDecimal(field);
            case NULL:
                return null;
            case BYTES:
                return field.getBytes(StandardCharsets.UTF_8);
            case DATE:
                return isDefaultValue ? DateUtils.parse(field) : parseDate(field, fieldName);
            case TIME:
                return parseTime(field);
            case TIMESTAMP:
                return isDefaultValue
                        ? DateTimeUtils.parse(field)
                        : parseTimestamp(field, fieldName);
            case TIMESTAMP_TZ:
                return parseTimestampTz(field, fieldName);
            case ROW:
                Map<Integer, String> splitsMap =
                        splitLineBySeaTunnelRowType(field, (SeaTunnelRowType) fieldType, level + 1);
                Object[] objects = new Object[splitsMap.size()];
                String[] eleFieldNames = ((SeaTunnelRowType) fieldType).getFieldNames();
                for (int i = 0; i < objects.length; i++) {
                    objects[i] =
                            convert(
                                    splitsMap.get(i),
                                    ((SeaTunnelRowType) fieldType).getFieldType(i),
                                    level + 1,
                                    fieldName + "." + eleFieldNames[i],
                                    isDefaultValue);
                }
                return new SeaTunnelRow(objects);
            default:
                throw CommonError.unsupportedDataType(
                        "SeaTunnel", fieldType.getSqlType().toString(), fieldName);
        }
    }

    private LocalDate parseDate(String field, String fieldName) {
        DateTimeFormatter dateFormatter = fieldFormatterMap.get(fieldName);
        if (dateFormatter == null) {
            dateFormatter = DateUtils.matchDateFormatter(field);
            fieldFormatterMap.put(fieldName, dateFormatter);
        }
        if (dateFormatter == null) {
            throw CommonError.formatDateError(field, fieldName);
        }

        return dateFormatter.parse(field).query(TemporalQueries.localDate());
    }

    private LocalTime parseTime(String field) {
        try {
            TemporalAccessor parsedTime = TIME_FORMAT.parse(field);
            return parsedTime.query(TemporalQueries.localTime());
        } catch (DateTimeParseException e) {
            throw new SeaTunnelCsvFormatException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE, "Invalid time format: " + field, e);
        }
    }

    private LocalDateTime parseTimestamp(String field, String fieldName) {
        DateTimeFormatter dateTimeFormatter =
                fieldFormatterMap.computeIfAbsent(
                        fieldName, f -> DateTimeUtils.matchDateTimeFormatter(field));
        if (dateTimeFormatter == null) {
            throw new SeaTunnelCsvFormatException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    String.format(
                            "SeaTunnel can not parse this date format [%s] of field [%s]",
                            field, fieldName));
        }
        TemporalAccessor parsedTimestamp = dateTimeFormatter.parse(field);
        return LocalDateTime.of(
                parsedTimestamp.query(TemporalQueries.localDate()),
                parsedTimestamp.query(TemporalQueries.localTime()));
    }

    private OffsetDateTime parseTimestampTz(String field, String fieldName) {
        try {
            return OffsetDateTime.parse(field, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        } catch (DateTimeParseException ignored) {
            // Fallback: data written by old SeaTunnel (wall-clock, no offset).
            DateTimeFormatter fallbackFmt = DateTimeUtils.matchDateTimeFormatter(field);
            if (fallbackFmt == null) {
                throw CommonError.formatDateTimeError(field, fieldName);
            }
            TemporalAccessor ta = fallbackFmt.parse(field);
            return LocalDateTime.of(
                            ta.query(TemporalQueries.localDate()),
                            ta.query(TemporalQueries.localTime()))
                    .atOffset(ZoneOffset.UTC);
        }
    }
}
