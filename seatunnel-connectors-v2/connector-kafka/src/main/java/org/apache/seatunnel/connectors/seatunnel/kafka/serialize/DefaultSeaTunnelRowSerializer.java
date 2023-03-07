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

package org.apache.seatunnel.connectors.seatunnel.kafka.serialize;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.format.json.JsonSerializationSchema;
import org.apache.seatunnel.format.json.canal.CanalJsonSerializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;
import org.apache.seatunnel.format.text.TextSerializationSchema;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.function.Function;

import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.CANNAL_FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.DEFAULT_FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.TEXT_FORMAT;

public class DefaultSeaTunnelRowSerializer implements SeaTunnelRowSerializer<byte[], byte[]> {

    private Integer partition;
    private final SerializationSchema keySerialization;
    private final SerializationSchema valueSerialization;

    public DefaultSeaTunnelRowSerializer(
            SeaTunnelRowType seaTunnelRowType, String format, String delimiter) {
        this(element -> null, createSerializationSchema(seaTunnelRowType, format, delimiter));
    }

    public DefaultSeaTunnelRowSerializer(
            Integer partition, SeaTunnelRowType seaTunnelRowType, String format, String delimiter) {
        this(seaTunnelRowType, format, delimiter);
        this.partition = partition;
    }

    public DefaultSeaTunnelRowSerializer(
            List<String> keyFieldNames,
            SeaTunnelRowType seaTunnelRowType,
            String format,
            String delimiter) {
        this(
                createKeySerializationSchema(keyFieldNames, seaTunnelRowType),
                createSerializationSchema(seaTunnelRowType, format, delimiter));
    }

    public DefaultSeaTunnelRowSerializer(
            SerializationSchema keySerialization, SerializationSchema valueSerialization) {
        this.keySerialization = keySerialization;
        this.valueSerialization = valueSerialization;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serializeRow(String topic, SeaTunnelRow row) {
        return new ProducerRecord<>(
                topic,
                partition,
                keySerialization.serialize(row),
                valueSerialization.serialize(row));
    }

    private static SerializationSchema createSerializationSchema(
            SeaTunnelRowType rowType, String format, String delimiter) {
        switch (format) {
            case DEFAULT_FORMAT:
                return new JsonSerializationSchema(rowType);
            case TEXT_FORMAT:
                return TextSerializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter(delimiter)
                        .build();
            case CANNAL_FORMAT:
                return new CanalJsonSerializationSchema(rowType);
            default:
                throw new SeaTunnelJsonFormatException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE, "Unsupported format: " + format);
        }
    }

    private static SerializationSchema createKeySerializationSchema(
            List<String> keyFieldNames, SeaTunnelRowType seaTunnelRowType) {
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
}
