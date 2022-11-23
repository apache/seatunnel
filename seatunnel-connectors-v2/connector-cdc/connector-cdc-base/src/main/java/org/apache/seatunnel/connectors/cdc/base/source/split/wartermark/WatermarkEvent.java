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

package org.apache.seatunnel.connectors.cdc.base.source.split.wartermark;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;

import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;
import java.util.Optional;

/** Utility class to deal Watermark event. */
public class WatermarkEvent {

    public static final String WATERMARK_SIGNAL = "_split_watermark_signal_";
    public static final String SPLIT_ID_KEY = "split_id";
    public static final String WATERMARK_KIND = "watermark_kind";
    public static final String SIGNAL_EVENT_KEY_SCHEMA_NAME =
            "io.debezium.connector.seatunnel.cdc.embedded.watermark.key";
    public static final String SIGNAL_EVENT_VALUE_SCHEMA_NAME =
            "io.debezium.connector.seatunnel.cdc.embedded.watermark.value";

    private static final SchemaNameAdjuster SCHEMA_NAME_ADJUSTER = SchemaNameAdjuster.create();

    private static final Schema SIGNAL_EVENT_KEY_SCHEMA =
            SchemaBuilder.struct()
                    .name(SCHEMA_NAME_ADJUSTER.adjust(SIGNAL_EVENT_KEY_SCHEMA_NAME))
                    .field(SPLIT_ID_KEY, Schema.STRING_SCHEMA)
                    .field(WATERMARK_SIGNAL, Schema.BOOLEAN_SCHEMA)
                    .build();

    private static final Schema SIGNAL_EVENT_VALUE_SCHEMA =
            SchemaBuilder.struct()
                    .name(SCHEMA_NAME_ADJUSTER.adjust(SIGNAL_EVENT_VALUE_SCHEMA_NAME))
                    .field(SPLIT_ID_KEY, Schema.STRING_SCHEMA)
                    .field(WATERMARK_KIND, Schema.STRING_SCHEMA)
                    .build();

    public static SourceRecord create(
            Map<String, ?> sourcePartition,
            String topic,
            String splitId,
            WatermarkKind watermarkKind,
            Offset watermark) {
        return new SourceRecord(
                sourcePartition,
                watermark.getOffset(),
                topic,
                SIGNAL_EVENT_KEY_SCHEMA,
                signalRecordKey(splitId),
                SIGNAL_EVENT_VALUE_SCHEMA,
                signalRecordValue(splitId, watermarkKind));
    }

    public static boolean isWatermarkEvent(SourceRecord record) {
        Optional<WatermarkKind> watermarkKind = getWatermarkKind(record);
        return watermarkKind.isPresent();
    }

    public static boolean isLowWatermarkEvent(SourceRecord record) {
        Optional<WatermarkKind> watermarkKind = getWatermarkKind(record);
        return watermarkKind.isPresent() && watermarkKind.get() == WatermarkKind.LOW;
    }

    public static boolean isHighWatermarkEvent(SourceRecord record) {
        Optional<WatermarkKind> watermarkKind = getWatermarkKind(record);
        return watermarkKind.isPresent() && watermarkKind.get() == WatermarkKind.HIGH;
    }

    public static boolean isEndWatermarkEvent(SourceRecord record) {
        Optional<WatermarkKind> watermarkKind = getWatermarkKind(record);
        return watermarkKind.isPresent() && watermarkKind.get() == WatermarkKind.END;
    }

    private static Optional<WatermarkKind> getWatermarkKind(SourceRecord record) {
        if (record.valueSchema() != null
                && SIGNAL_EVENT_VALUE_SCHEMA_NAME.equals(record.valueSchema().name())) {
            Struct value = (Struct) record.value();
            return Optional.of(WatermarkKind.valueOf(value.getString(WATERMARK_KIND)));
        }
        return Optional.empty();
    }

    private static Struct signalRecordKey(String splitId) {
        Struct result = new Struct(SIGNAL_EVENT_KEY_SCHEMA);
        result.put(SPLIT_ID_KEY, splitId);
        result.put(WATERMARK_SIGNAL, true);
        return result;
    }

    private static Struct signalRecordValue(String splitId, WatermarkKind watermarkKind) {
        Struct result = new Struct(SIGNAL_EVENT_VALUE_SCHEMA);
        result.put(SPLIT_ID_KEY, splitId);
        result.put(WATERMARK_KIND, watermarkKind.toString());
        return result;
    }
}
