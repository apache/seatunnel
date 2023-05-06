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

package io.debezium.connector.postgresql;

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;
import io.debezium.time.Conversions;
import io.debezium.util.Collect;

import java.time.Instant;
import java.util.Map;

public class PostgresEventMetadataProvider implements EventMetadataProvider {

    @Override
    public Instant getEventTimestamp(
            DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }
        if (sourceInfo.schema().field(SourceInfo.TIMESTAMP_USEC_KEY) != null) {
            final Long timestamp = sourceInfo.getInt64(SourceInfo.TIMESTAMP_USEC_KEY);
            return timestamp == null ? null : Conversions.toInstantFromMicros(timestamp);
        }
        final Long timestamp = sourceInfo.getInt64(SourceInfo.TIMESTAMP_KEY);
        return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
    }

    @Override
    public Map<String, String> getEventSourcePosition(
            DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }
        Long xmin = sourceInfo.getInt64(SourceInfo.XMIN_KEY);

        Map<String, String> r =
                Collect.hashMapOf(
                        SourceInfo.LSN_KEY, Long.toString(sourceInfo.getInt64(SourceInfo.LSN_KEY)));
        if (xmin != null) {
            r.put(SourceInfo.XMIN_KEY, Long.toString(xmin));
        }
        return r;
    }

    @Override
    public String getTransactionId(
            DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }
        return Long.toString(sourceInfo.getInt64(SourceInfo.TXID_KEY));
    }
}
