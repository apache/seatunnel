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

package org.apache.seatunnel.connectors.seatunnel.iotdb.serialize;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;

class IoTDBTimestampConversionTest {

    private static final ZoneId SHANGHAI = ZoneId.of("Asia/Shanghai");
    private static final LocalDateTime LOCAL_TIMESTAMP = LocalDateTime.of(2024, 1, 1, 0, 0);
    private static final long UTC_TIMESTAMP = 1704067200000L;
    private static final long SHANGHAI_TIMESTAMP = 1704038400000L;

    @Test
    void sinkUsesConfiguredZoneForLocalTimestamps() {
        Assertions.assertEquals(
                SHANGHAI_TIMESTAMP,
                serializer(LocalTimeType.LOCAL_DATE_TIME_TYPE, SHANGHAI)
                        .serialize(row(LOCAL_TIMESTAMP))
                        .getTimestamp()
                        .longValue());
    }

    @Test
    void sinkRetainsUtcForLegacyConstructorAndNullZone() {
        SeaTunnelRowType rowType = sinkRowType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
        DefaultSeaTunnelRowSerializer legacy =
                new DefaultSeaTunnelRowSerializer(
                        rowType, "root.test", "ts", "device", Collections.singletonList("value"));
        Assertions.assertEquals(
                UTC_TIMESTAMP, legacy.serialize(row(LOCAL_TIMESTAMP)).getTimestamp().longValue());
        Assertions.assertEquals(
                UTC_TIMESTAMP,
                serializer(LocalTimeType.LOCAL_DATE_TIME_TYPE, null)
                        .serialize(row(LOCAL_TIMESTAMP))
                        .getTimestamp()
                        .longValue());
    }

    @Test
    void sinkDoesNotShiftRawEpochValues() {
        Assertions.assertEquals(
                UTC_TIMESTAMP,
                serializer(BasicType.LONG_TYPE, SHANGHAI)
                        .serialize(row(UTC_TIMESTAMP))
                        .getTimestamp()
                        .longValue());
        Assertions.assertEquals(
                UTC_TIMESTAMP,
                serializer(BasicType.STRING_TYPE, SHANGHAI)
                        .serialize(row(Long.toString(UTC_TIMESTAMP)))
                        .getTimestamp()
                        .longValue());
    }

    @Test
    void sinkRetainsProcessingTimeAndNullMeasurements() {
        DefaultSeaTunnelRowSerializer serializer =
                serializer(LocalTimeType.LOCAL_DATE_TIME_TYPE, SHANGHAI);
        long before = System.currentTimeMillis();
        IoTDBRecord record = serializer.serialize(new SeaTunnelRow(new Object[] {null, "a", null}));
        long after = System.currentTimeMillis();
        Assertions.assertTrue(record.getTimestamp() >= before && record.getTimestamp() <= after);
        Assertions.assertEquals(Collections.singletonList(null), record.getValues());

        DefaultSeaTunnelRowSerializer withoutTimestampKey =
                new DefaultSeaTunnelRowSerializer(
                        sinkRowType(LocalTimeType.LOCAL_DATE_TIME_TYPE),
                        "root.test",
                        null,
                        "device",
                        Collections.singletonList("value"),
                        SHANGHAI);
        before = System.currentTimeMillis();
        record = withoutTimestampKey.serialize(row(LOCAL_TIMESTAMP));
        after = System.currentTimeMillis();
        Assertions.assertTrue(record.getTimestamp() >= before && record.getTimestamp() <= after);
    }

    @Test
    void sourceUsesConfiguredZoneForLocalTimestamps() {
        DefaultSeaTunnelRowDeserializer deserializer =
                new DefaultSeaTunnelRowDeserializer(
                        sourceRowType(LocalTimeType.LOCAL_DATE_TIME_TYPE), SHANGHAI);
        Assertions.assertEquals(
                LOCAL_TIMESTAMP, deserializer.deserialize(record(SHANGHAI_TIMESTAMP)).getField(0));
    }

    @Test
    void sourceRetainsUtcForLegacyConstructorAndNullZone() {
        SeaTunnelRowType rowType = sourceRowType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
        Assertions.assertEquals(
                LOCAL_TIMESTAMP,
                new DefaultSeaTunnelRowDeserializer(rowType)
                        .deserialize(record(UTC_TIMESTAMP))
                        .getField(0));
        Assertions.assertEquals(
                LOCAL_TIMESTAMP,
                new DefaultSeaTunnelRowDeserializer(rowType, null)
                        .deserialize(record(UTC_TIMESTAMP))
                        .getField(0));
    }

    @Test
    void sourceDoesNotShiftRawEpochValuesOrNullMeasurements() {
        DefaultSeaTunnelRowDeserializer deserializer =
                new DefaultSeaTunnelRowDeserializer(sourceRowType(BasicType.LONG_TYPE), SHANGHAI);
        RowRecord record = new RowRecord(UTC_TIMESTAMP);
        record.addField(null, TSDataType.INT32);
        Assertions.assertArrayEquals(
                new Object[] {UTC_TIMESTAMP, null}, deserializer.deserialize(record).getFields());
    }

    private static DefaultSeaTunnelRowSerializer serializer(
            SeaTunnelDataType<?> timestampType, ZoneId zoneId) {
        return new DefaultSeaTunnelRowSerializer(
                sinkRowType(timestampType),
                "root.test",
                "ts",
                "device",
                Collections.singletonList("value"),
                zoneId);
    }

    private static SeaTunnelRowType sinkRowType(SeaTunnelDataType<?> timestampType) {
        return new SeaTunnelRowType(
                new String[] {"ts", "device", "value"},
                new SeaTunnelDataType<?>[] {
                    timestampType, BasicType.STRING_TYPE, BasicType.INT_TYPE
                });
    }

    private static SeaTunnelRowType sourceRowType(SeaTunnelDataType<?> timestampType) {
        return new SeaTunnelRowType(
                new String[] {"ts", "value"},
                new SeaTunnelDataType<?>[] {timestampType, BasicType.INT_TYPE});
    }

    private static SeaTunnelRow row(Object timestamp) {
        return new SeaTunnelRow(new Object[] {timestamp, "a", 7});
    }

    private static RowRecord record(long timestamp) {
        RowRecord record = new RowRecord(timestamp);
        record.addField(7, TSDataType.INT32);
        return record;
    }
}
