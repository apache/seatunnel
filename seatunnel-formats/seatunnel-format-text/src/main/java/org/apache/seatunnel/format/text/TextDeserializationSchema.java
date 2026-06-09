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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.FormatterConfig;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.DateTimeParseHelper;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.EncodingUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.format.text.constant.TextFormatConstant;
import org.apache.seatunnel.format.text.exception.SeaTunnelTextFormatException;
import org.apache.seatunnel.format.text.splitor.DefaultTextLineSplitor;
import org.apache.seatunnel.format.text.splitor.TextLineSplitor;

import lombok.NonNull;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class TextDeserializationSchema
        implements DeserializationSchema<SeaTunnelRow>, DateTimeParseHelper {
    private final SeaTunnelRowType seaTunnelRowType;
    private final String[] separators;
    private final String encoding;
    private final String nullFormat;
    private final TextLineSplitor splitor;
    private final CatalogTable catalogTable;
    private final FormatterConfig<DateUtils.Formatter> dateFormatterConfig;
    private final FormatterConfig<DateTimeUtils.Formatter> dateTimeFormatterConfig;
    private final FormatterConfig<TimeUtils.Formatter> timeFormatterConfig;

    private final Map<String, DateTimeFormatter> fieldFormatterCache = new ConcurrentHashMap<>();

    private TextDeserializationSchema(
            @NonNull SeaTunnelRowType seaTunnelRowType,
            String[] separators,
            String encoding,
            String nullFormat,
            TextLineSplitor splitor,
            CatalogTable catalogTable,
            FormatterConfig<DateUtils.Formatter> dateFormatterConfig,
            FormatterConfig<DateTimeUtils.Formatter> dateTimeFormatterConfig,
            FormatterConfig<TimeUtils.Formatter> timeFormatterConfig) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.separators = separators;
        this.encoding = encoding;
        this.nullFormat = nullFormat;
        this.splitor = splitor;
        this.catalogTable = catalogTable;
        this.dateFormatterConfig = dateFormatterConfig;
        this.dateTimeFormatterConfig = dateTimeFormatterConfig;
        this.timeFormatterConfig = timeFormatterConfig;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private SeaTunnelRowType seaTunnelRowType;
        private CatalogTable catalogTable;
        private String[] separators = TextFormatConstant.SEPARATOR.clone();
        private FormatterConfig<DateUtils.Formatter> dateFormatterConfig =
                FormatterConfig.ofDefault(DateUtils.Formatter.YYYY_MM_DD);
        private FormatterConfig<DateTimeUtils.Formatter> dateTimeFormatterConfig =
                FormatterConfig.ofDefault(DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS);
        private FormatterConfig<TimeUtils.Formatter> timeFormatterConfig =
                FormatterConfig.ofDefault(TimeUtils.Formatter.HH_MM_SS);
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
            this.dateFormatterConfig = FormatterConfig.ofUserConfigured(dateFormatter);
            return this;
        }

        public Builder dateTimeFormatter(DateTimeUtils.Formatter dateTimeFormatter) {
            this.dateTimeFormatterConfig = FormatterConfig.ofUserConfigured(dateTimeFormatter);
            return this;
        }

        public Builder timeFormatter(TimeUtils.Formatter timeFormatter) {
            this.timeFormatterConfig = FormatterConfig.ofUserConfigured(timeFormatter);
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
                    catalogTable,
                    dateFormatterConfig,
                    dateTimeFormatterConfig,
                    timeFormatterConfig);
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
            String fieldValue, SeaTunnelDataType<?> fieldType, int level, String fieldName) {
        if (StringUtils.isEmpty(fieldValue)) {
            return null;
        }
        switch (fieldType.getSqlType()) {
            case ARRAY:
                SeaTunnelDataType<?> elementType = ((ArrayType<?, ?>) fieldType).getElementType();
                String[] elements = fieldValue.split(separators[level + 1]);
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
                String[] kvs = fieldValue.split(separators[level + 1]);
                for (String kv : kvs) {
                    String[] splits = kv.split(separators[level + 2]);
                    if (splits.length < 2) {
                        objectMap.put(
                                convert(splits[0], keyType, level + 1, fieldName + ".key"), null);
                    } else {
                        objectMap.put(
                                convert(splits[0], keyType, level + 1, fieldName + ".key"),
                                convert(splits[1], valueType, level + 1, fieldName + ".value"));
                    }
                }
                return objectMap;
            case STRING:
                return fieldValue;
            case BOOLEAN:
                return Boolean.parseBoolean(fieldValue);
            case TINYINT:
                return Byte.parseByte(fieldValue);
            case SMALLINT:
                return Short.parseShort(fieldValue);
            case INT:
                return Integer.parseInt(fieldValue);
            case BIGINT:
                return Long.parseLong(fieldValue);
            case FLOAT:
                return Float.parseFloat(fieldValue);
            case DOUBLE:
                return Double.parseDouble(fieldValue);
            case DECIMAL:
                return new BigDecimal(fieldValue);
            case NULL:
                return null;
            case BYTES:
                return fieldValue.getBytes(StandardCharsets.UTF_8);
            case DATE:
                return parseDate(fieldValue, fieldName, dateFormatterConfig, fieldFormatterCache);
            case TIME:
                return parseTime(fieldValue, fieldName, timeFormatterConfig, fieldFormatterCache);
            case TIMESTAMP:
                return parseTimestamp(
                        fieldValue, fieldName, dateTimeFormatterConfig, fieldFormatterCache);
            case ROW:
                Map<Integer, String> splitsMap =
                        splitLineBySeaTunnelRowType(
                                fieldValue, (SeaTunnelRowType) fieldType, level + 1);
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
