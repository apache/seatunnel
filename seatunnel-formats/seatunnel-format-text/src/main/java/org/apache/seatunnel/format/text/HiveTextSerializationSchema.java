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

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.format.text.constant.TextFormatConstant;

import lombok.NonNull;

import java.util.Map;
import java.util.Set;

public class HiveTextSerializationSchema extends TextSerializationSchema {
    private final String collectionDelimiter;
    private final String mapKeysDelimiter;
    public static final String REGEX = "^\"|\"$|^\'|\'$";

    public HiveTextSerializationSchema(
            @NonNull SeaTunnelRowType seaTunnelRowType,
            String[] separators,
            DateUtils.Formatter dateFormatter,
            DateTimeUtils.Formatter dateTimeFormatter,
            TimeUtils.Formatter timeFormatter,
            String collectionDelimiter,
            String mapKeysDelimiter) {
        super(seaTunnelRowType, separators, dateFormatter, dateTimeFormatter, timeFormatter);
        this.collectionDelimiter = collectionDelimiter;
        this.mapKeysDelimiter = mapKeysDelimiter;
    }

    public static HiveTextSerializationSchema.Builder builder1() {
        return new HiveTextSerializationSchema.Builder();
    }

    public static class Builder {
        private SeaTunnelRowType seaTunnelRowType;
        private String[] separators = TextFormatConstant.SEPARATOR.clone();
        private DateUtils.Formatter dateFormatter = DateUtils.Formatter.YYYY_MM_DD;
        private DateTimeUtils.Formatter dateTimeFormatter =
                DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS;
        private TimeUtils.Formatter timeFormatter = TimeUtils.Formatter.HH_MM_SS;
        private String collectionDelimiter;
        private String mapKeysDelimiter;

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

        public Builder collectionDelimiter(String collectionDelimiter) {
            this.collectionDelimiter = collectionDelimiter;
            return this;
        }

        public Builder mapKeysDelimiter(String mapKeysDelimiter) {
            this.mapKeysDelimiter = mapKeysDelimiter;
            return this;
        }

        public HiveTextSerializationSchema build() {
            return new HiveTextSerializationSchema(
                    seaTunnelRowType,
                    separators,
                    dateFormatter,
                    dateTimeFormatter,
                    timeFormatter,
                    collectionDelimiter,
                    mapKeysDelimiter);
        }
    }

    @Override
    public String convert(Object field, SeaTunnelDataType<?> fieldType, int level) {
        switch (fieldType.getSqlType()) {
            case ARRAY:
                Object[] array = (Object[]) field;
                StringBuilder elementBuilder = new StringBuilder();
                for (Object element : array) {
                    String trimmed = element.toString().trim().replaceAll(REGEX, "");
                    elementBuilder.append(trimmed).append(collectionDelimiter);
                }
                elementBuilder.deleteCharAt(elementBuilder.length() - 1);
                return elementBuilder.toString();
            case MAP:
                Map map = (Map) field;
                Set<Map.Entry> set = map.entrySet();
                StringBuilder stringBuilder = new StringBuilder();
                for (Map.Entry entry : set) {
                    String key =
                            entry.getKey() == null
                                    ? null
                                    : entry.getKey().toString().trim().replaceAll(REGEX, "");
                    String value =
                            entry.getValue() == null
                                    ? null
                                    : entry.getValue().toString().trim().replaceAll(REGEX, "");
                    stringBuilder
                            .append(key)
                            .append(mapKeysDelimiter)
                            .append(value)
                            .append(collectionDelimiter);
                }
                stringBuilder.deleteCharAt(stringBuilder.length() - 1);
                return stringBuilder.toString();
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
                return String.join(collectionDelimiter, strings);
            default:
                return super.convert(field, fieldType, level);
        }
    }
}
