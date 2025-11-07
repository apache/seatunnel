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

package org.apache.seatunnel.format.text;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
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
import org.apache.seatunnel.format.text.constant.TextFormatConstant;
import org.apache.seatunnel.format.text.exception.SeaTunnelTextFormatException;
import org.apache.seatunnel.format.text.splitor.DefaultTextLineSplitor;
import org.apache.seatunnel.format.text.splitor.TextLineSplitor;

import org.apache.commons.lang3.StringUtils;

import lombok.NonNull;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class TextDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {
    private final SeaTunnelRowType seaTunnelRowType;
    private final String[] separators;
    private final String encoding;
    private final String nullFormat;
    private final TextLineSplitor splitor;
    private final CatalogTable catalogTable;

    @SuppressWarnings("MagicNumber")
    public static final DateTimeFormatter TIME_FORMAT =
            new DateTimeFormatterBuilder()
                    .appendPattern("HH:mm:ss")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .toFormatter();

    public Map<String, DateTimeFormatter> fieldFormatterMap = new HashMap<>();

    private TextDeserializationSchema(
            @NonNull SeaTunnelRowType seaTunnelRowType,
            String[] separators,
            String encoding,
            String nullFormat,
            TextLineSplitor splitor,
            CatalogTable catalogTable) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.separators = separators;
        this.encoding = encoding;
        this.nullFormat = nullFormat;
        this.splitor = splitor;
        this.catalogTable = catalogTable;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private SeaTunnelRowType seaTunnelRowType;
        private CatalogTable catalogTable;
        private String[] separators = TextFormatConstant.SEPARATOR.clone();
        private DateUtils.Formatter dateFormatter = DateUtils.Formatter.YYYY_MM_DD;
        private DateTimeUtils.Formatter dateTimeFormatter =
                DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS;
        private TimeUtils.Formatter timeFormatter = TimeUtils.Formatter.HH_MM_SS;
        private String encoding = StandardCharsets.UTF_8.name();
        private String nullFormat;
        private TextLineSplitor textLineSplitor = new DefaultTextLineSplitor();

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

        public Builder textLineSplitor(TextLineSplitor splitor) {
            this.textLineSplitor = splitor;
            return this;
        }

        public TextDeserializationSchema build() {
            return new TextDeserializationSchema(
                    seaTunnelRowType,
                    separators,
                    encoding,
                    nullFormat,
                    textLineSplitor,
                    catalogTable);
        }
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null;
        }
        String content = new String(message, EncodingUtils.tryParseCharset(encoding));
        Map<Integer, String> splitsMap = splitLineBySeaTunnelRowType(content, seaTunnelRowType, 0);
        Object[] objects = new Object[seaTunnelRowType.getTotalFields()];
        for (int i = 0; i < objects.length; i++) {
            String fieldValue = splitsMap.get(i);
            if (StringUtils.equals(fieldValue, nullFormat)) {
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

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return seaTunnelRowType;
    }

    private Map<Integer, String> splitLineBySeaTunnelRowType(
            String line, SeaTunnelRowType seaTunnelRowType, int level) {
        String[] splits = splitor.spliteLine(line, separators[level]);
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
        if (StringUtils.isEmpty(field)) {
            return null;
        }
        switch (fieldType.getSqlType()) {
            case ARRAY:
                SeaTunnelDataType<?> elementType = ((ArrayType<?, ?>) fieldType).getElementType();
                String[] elements = field.split(separators[level + 1]);
                ArrayList<Object> objectArrayList = new ArrayList<>();
                for (String element : elements) {
                    objectArrayList.add(convert(element, elementType, level + 1, fieldName));
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
                    default:
                        throw new SeaTunnelTextFormatException(
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
                        objectMap.put(convert(splits[0], keyType, level + 1, fieldName), null);
                    } else {
                        objectMap.put(
                                convert(splits[0], keyType, level + 1, fieldName),
                                convert(splits[1], valueType, level + 1, fieldName));
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
                DateTimeFormatter dateFormatter = fieldFormatterMap.get(fieldName);
                if (dateFormatter == null) {
                    dateFormatter = DateUtils.matchDateFormatter(field);
                    fieldFormatterMap.put(fieldName, dateFormatter);
                }
                if (dateFormatter == null) {
                    throw CommonError.formatDateError(field, fieldName);
                }

                return dateFormatter.parse(field).query(TemporalQueries.localDate());
            case TIME:
                TemporalAccessor parsedTime = TIME_FORMAT.parse(field);
                return parsedTime.query(TemporalQueries.localTime());
            case TIMESTAMP:
                DateTimeFormatter dateTimeFormatter = fieldFormatterMap.get(fieldName);
                if (dateTimeFormatter == null) {
                    dateTimeFormatter = DateTimeUtils.matchDateTimeFormatter(field);
                    fieldFormatterMap.put(fieldName, dateTimeFormatter);
                }
                if (dateTimeFormatter == null) {
                    throw CommonError.formatDateTimeError(field, fieldName);
                }

                TemporalAccessor parsedTimestamp = dateTimeFormatter.parse(field);
                LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
                LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());
                return LocalDateTime.of(localDate, localTime);
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
                                    fieldName + "." + eleFieldNames[i]);
                }
                return new SeaTunnelRow(objects);
            default:
                throw CommonError.unsupportedDataType(
                        "SeaTunnel", fieldType.getSqlType().toString(), fieldName);
        }
    }
}
