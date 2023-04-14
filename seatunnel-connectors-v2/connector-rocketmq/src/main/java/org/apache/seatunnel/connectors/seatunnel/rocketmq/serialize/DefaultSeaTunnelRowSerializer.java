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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.serialize;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.SchemaFormat;
import org.apache.seatunnel.format.json.JsonSerializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;
import org.apache.seatunnel.format.text.TextSerializationSchema;

import org.apache.rocketmq.common.message.Message;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.function.Function;

@Slf4j
public class DefaultSeaTunnelRowSerializer implements SeaTunnelRowSerializer<byte[], byte[]> {
    private final String topic;
    private final SerializationSchema keySerialization;
    private final SerializationSchema valueSerialization;

    public DefaultSeaTunnelRowSerializer(
            String topic,
            SeaTunnelRowType seaTunnelRowType,
            SchemaFormat format,
            String delimiter) {
        this(
                topic,
                element -> null,
                createSerializationSchema(seaTunnelRowType, format, delimiter));
    }

    public DefaultSeaTunnelRowSerializer(
            String topic,
            List<String> keyFieldNames,
            SeaTunnelRowType seaTunnelRowType,
            SchemaFormat format,
            String delimiter) {
        this(
                topic,
                createKeySerializationSchema(keyFieldNames, seaTunnelRowType),
                createSerializationSchema(seaTunnelRowType, format, delimiter));
    }

    public DefaultSeaTunnelRowSerializer(
            String topic,
            SerializationSchema keySerialization,
            SerializationSchema valueSerialization) {
        this.topic = topic;
        this.keySerialization = keySerialization;
        this.valueSerialization = valueSerialization;
    }

    private static SerializationSchema createSerializationSchema(
            SeaTunnelRowType rowType, SchemaFormat format, String delimiter) {
        switch (format) {
            case TEXT:
                return TextSerializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter(delimiter)
                        .build();
            case JSON:
                return new JsonSerializationSchema(rowType);
            default:
                throw new SeaTunnelJsonFormatException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE, "Unsupported format: " + format);
        }
    }

    private static SerializationSchema createKeySerializationSchema(
            List<String> keyFieldNames, SeaTunnelRowType seaTunnelRowType) {
        if (keyFieldNames == null || keyFieldNames.isEmpty()) {
            return element -> null;
        }
        int[] keyFieldIndexArr = new int[keyFieldNames.size()];
        SeaTunnelDataType[] keyFieldDataTypeArr = new SeaTunnelDataType[keyFieldNames.size()];
        for (int i = 0; i < keyFieldNames.size(); i++) {
            String keyFieldName = keyFieldNames.get(i);
            int rowFieldIndex = seaTunnelRowType.indexOf(keyFieldName);
            keyFieldIndexArr[i] = rowFieldIndex;
            keyFieldDataTypeArr[i] = seaTunnelRowType.getFieldType(rowFieldIndex);
        }
        SeaTunnelRowType keyType =
                new SeaTunnelRowType(keyFieldNames.toArray(new String[0]), keyFieldDataTypeArr);
        SerializationSchema keySerializationSchema = new JsonSerializationSchema(keyType);
        Function<SeaTunnelRow, SeaTunnelRow> keyDataExtractor =
                row -> {
                    Object[] keyFields = new Object[keyFieldIndexArr.length];
                    for (int i = 0; i < keyFieldIndexArr.length; i++) {
                        keyFields[i] = row.getField(keyFieldIndexArr[i]);
                    }
                    return new SeaTunnelRow(keyFields);
                };
        return row -> keySerializationSchema.serialize(keyDataExtractor.apply(row));
    }

    @Override
    public Message serializeRow(SeaTunnelRow row) {
        byte[] value = valueSerialization.serialize(row);
        if (value == null) {
            return null;
        }
        byte[] key = keySerialization.serialize(row);
        return new Message(topic, null, key == null ? null : new String(key), value);
    }
}
