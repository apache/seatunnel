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
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.iotdb.exception.IotdbConnectorException;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;

class IoTDBTimestampConversionTest {

    private static final ZoneId SHANGHAI = ZoneId.of("Asia/Shanghai");
    private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");
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
        long processingTime = 1730615400000L;
        DefaultSeaTunnelRowSerializer serializer =
                new DefaultSeaTunnelRowSerializer(
                        sinkRowType(LocalTimeType.LOCAL_DATE_TIME_TYPE),
                        "root.test",
                        "ts",
                        "device",
                        Collections.singletonList("value"),
                        SHANGHAI,
                        () -> processingTime);
        IoTDBRecord record = serializer.serialize(new SeaTunnelRow(new Object[] {null, "a", null}));
        Assertions.assertEquals(processingTime, record.getTimestamp().longValue());
        Assertions.assertEquals(Collections.singletonList(null), record.getValues());

        DefaultSeaTunnelRowSerializer withoutTimestampKey =
                new DefaultSeaTunnelRowSerializer(
                        sinkRowType(LocalTimeType.LOCAL_DATE_TIME_TYPE),
                        "root.test",
                        null,
                        "device",
                        Collections.singletonList("value"),
                        SHANGHAI,
                        () -> processingTime);
        record = withoutTimestampKey.serialize(row(LOCAL_TIMESTAMP));
        Assertions.assertEquals(processingTime, record.getTimestamp().longValue());
    }

    @Test
    void sinkUsesUniqueOffsetInDstZone() {
        Assertions.assertEquals(
                Instant.parse("2024-07-01T16:00:00Z").toEpochMilli(),
                serializer(LocalTimeType.LOCAL_DATE_TIME_TYPE, NEW_YORK)
                        .serialize(row(LocalDateTime.of(2024, 7, 1, 12, 0)))
                        .getTimestamp()
                        .longValue());
    }

    @Test
    void sinkRejectsAmbiguousAndMissingLocalTimes() {
        DefaultSeaTunnelRowSerializer serializer =
                serializer(LocalTimeType.LOCAL_DATE_TIME_TYPE, NEW_YORK);
        for (LocalDateTime timestamp :
                new LocalDateTime[] {
                    LocalDateTime.of(2024, 11, 3, 1, 30), LocalDateTime.of(2024, 3, 10, 2, 30)
                }) {
            IotdbConnectorException exception =
                    Assertions.assertThrows(
                            IotdbConnectorException.class,
                            () -> serializer.serialize(row(timestamp)));
            assertTimestampError(
                    exception, "field 'ts'", timestamp.toString(), NEW_YORK.toString());
        }
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

    @Test
    void sourceUsesUniqueOffsetInDstZone() {
        DefaultSeaTunnelRowDeserializer deserializer =
                new DefaultSeaTunnelRowDeserializer(
                        sourceRowType(LocalTimeType.LOCAL_DATE_TIME_TYPE), NEW_YORK);
        Assertions.assertEquals(
                LocalDateTime.of(2024, 7, 1, 12, 0),
                deserializer
                        .deserialize(record(Instant.parse("2024-07-01T16:00:00Z").toEpochMilli()))
                        .getField(0));
    }

    @Test
    void sourceRejectsBothEpochsInDstOverlap() {
        DefaultSeaTunnelRowDeserializer deserializer =
                new DefaultSeaTunnelRowDeserializer(
                        sourceRowType(LocalTimeType.LOCAL_DATE_TIME_TYPE), NEW_YORK);
        for (long timestamp : overlapEpochs()) {
            IotdbConnectorException exception =
                    Assertions.assertThrows(
                            IotdbConnectorException.class,
                            () -> deserializer.deserialize(record(timestamp)));
            assertTimestampError(
                    exception,
                    "field 'ts'",
                    Long.toString(timestamp),
                    "2024-11-03T01:30",
                    NEW_YORK.toString());
        }
    }

    @Test
    void overlapEpochsRemainLosslessAsUtcOrNumericValues() {
        DefaultSeaTunnelRowDeserializer utcDeserializer =
                new DefaultSeaTunnelRowDeserializer(
                        sourceRowType(LocalTimeType.LOCAL_DATE_TIME_TYPE), ZoneOffset.UTC);
        DefaultSeaTunnelRowDeserializer epochDeserializer =
                new DefaultSeaTunnelRowDeserializer(sourceRowType(BasicType.LONG_TYPE), NEW_YORK);
        for (long timestamp : overlapEpochs()) {
            LocalDateTime utcTimestamp =
                    (LocalDateTime) utcDeserializer.deserialize(record(timestamp)).getField(0);
            Assertions.assertEquals(
                    timestamp,
                    serializer(LocalTimeType.LOCAL_DATE_TIME_TYPE, ZoneOffset.UTC)
                            .serialize(row(utcTimestamp))
                            .getTimestamp()
                            .longValue());
            Assertions.assertEquals(
                    timestamp, epochDeserializer.deserialize(record(timestamp)).getField(0));
            Assertions.assertEquals(
                    timestamp,
                    serializer(BasicType.LONG_TYPE, NEW_YORK)
                            .serialize(row(timestamp))
                            .getTimestamp()
                            .longValue());
            Assertions.assertEquals(
                    timestamp,
                    serializer(BasicType.STRING_TYPE, NEW_YORK)
                            .serialize(row(Long.toString(timestamp)))
                            .getTimestamp()
                            .longValue());
        }
    }

    private static long[] overlapEpochs() {
        return new long[] {
            Instant.parse("2024-11-03T05:30:00Z").toEpochMilli(),
            Instant.parse("2024-11-03T06:30:00Z").toEpochMilli()
        };
    }

    private static void assertTimestampError(IotdbConnectorException exception, String... context) {
        Assertions.assertEquals(
                CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT, exception.getSeaTunnelErrorCode());
        for (String value : context) {
            Assertions.assertTrue(exception.getMessage().contains(value), exception.getMessage());
        }
        Assertions.assertTrue(exception.getMessage().contains("zone_id"), exception.getMessage());
        Assertions.assertTrue(
                exception.getMessage().contains("Use UTC or BIGINT epoch milliseconds"),
                exception.getMessage());
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
