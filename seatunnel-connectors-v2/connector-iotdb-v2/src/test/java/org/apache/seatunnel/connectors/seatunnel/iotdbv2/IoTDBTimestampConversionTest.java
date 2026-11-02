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

package org.apache.seatunnel.connectors.seatunnel.iotdbv2;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.exception.IotdbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.serialize.DefaultSeaTunnelRowDeserializer;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.serialize.DefaultSeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.serialize.relational.IoTDBv2RelationalRecord;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.serialize.relational.RelationalSeaTunnelRowSerializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import shaded.org.apache.tsfile.enums.TSDataType;
import shaded.org.apache.tsfile.read.common.Field;
import shaded.org.apache.tsfile.read.common.RowRecord;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;

class IoTDBTimestampConversionTest {

    private static final ZoneId SHANGHAI = ZoneId.of("Asia/Shanghai");
    private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");
    private static final LocalDateTime LOCAL_TIME =
            LocalDateTime.of(2024, 1, 1, 8, 0, 0, 123000000);
    private static final long EPOCH = 1704067200123L;
    private static final long UTC_EPOCH = 1704096000123L;
    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"time", "device", "event_time", "number"},
                    new SeaTunnelDataType[] {
                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                        BasicType.STRING_TYPE,
                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                        BasicType.LONG_TYPE
                    });

    @Test
    void sinkConvertsLocalTimestampsInConfiguredZone() {
        for (ZoneId zone : Arrays.asList(null, ZoneOffset.UTC, SHANGHAI)) {
            long expected = SHANGHAI.equals(zone) ? EPOCH : UTC_EPOCH;
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {LOCAL_TIME, "device", LOCAL_TIME, Long.MAX_VALUE});
            Assertions.assertEquals(
                    expected,
                    new DefaultSeaTunnelRowSerializer(
                                    ROW_TYPE,
                                    "root.test",
                                    "time",
                                    "device",
                                    Collections.singletonList("number"),
                                    zone)
                            .serialize(row)
                            .getTimestamp());
            IoTDBv2RelationalRecord record = tableSerializer(zone).serialize(row);
            Assertions.assertEquals(expected, record.getTimestamp());
            Assertions.assertEquals(Arrays.asList(expected, Long.MAX_VALUE), record.getFields());
        }
    }

    @Test
    void sourceConvertsTreeAndTableTimestampsInConfiguredZone() {
        SeaTunnelRowType treeType =
                new SeaTunnelRowType(
                        new String[] {"time"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});
        SeaTunnelRowType tableType =
                new SeaTunnelRowType(
                        new String[] {"time", "event_time", "epoch", "number", "nullable_time"},
                        new SeaTunnelDataType[] {
                            LocalTimeType.LOCAL_DATE_TIME_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.LONG_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });
        RowRecord tableRow = new RowRecord(0L);
        tableRow.addField(EPOCH, TSDataType.TIMESTAMP);
        tableRow.addField(EPOCH, TSDataType.TIMESTAMP);
        tableRow.addField(EPOCH, TSDataType.TIMESTAMP);
        tableRow.addField(Long.MAX_VALUE, TSDataType.INT64);
        tableRow.addField(new Field(null));
        for (ZoneId zone : Arrays.asList(null, ZoneOffset.UTC, SHANGHAI)) {
            LocalDateTime expected = SHANGHAI.equals(zone) ? LOCAL_TIME : LOCAL_TIME.minusHours(8);
            Assertions.assertEquals(
                    expected,
                    new DefaultSeaTunnelRowDeserializer(treeType, "tree", zone)
                            .deserialize(new RowRecord(EPOCH))
                            .getField(0));
            SeaTunnelRow row =
                    new DefaultSeaTunnelRowDeserializer(tableType, "table", zone)
                            .deserialize(tableRow);
            Assertions.assertArrayEquals(
                    new Object[] {expected, expected, EPOCH, Long.MAX_VALUE, null},
                    row.getFields());
        }
    }

    @Test
    void numericTimestampKeysRemainEpochMilliseconds() {
        for (SeaTunnelDataType<?> type :
                Arrays.asList(BasicType.LONG_TYPE, BasicType.STRING_TYPE)) {
            SeaTunnelRowType rowType =
                    new SeaTunnelRowType(
                            new String[] {"time", "device"},
                            new SeaTunnelDataType[] {type, BasicType.STRING_TYPE});
            Object value = type == BasicType.LONG_TYPE ? EPOCH : Long.toString(EPOCH);
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {value, "device"});
            Assertions.assertEquals(
                    EPOCH,
                    new DefaultSeaTunnelRowSerializer(
                                    rowType, "root.test", "time", "device", null, SHANGHAI)
                            .serialize(row)
                            .getTimestamp());
            Assertions.assertEquals(
                    EPOCH,
                    new RelationalSeaTunnelRowSerializer(
                                    rowType,
                                    "test",
                                    "device",
                                    "time",
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    SHANGHAI)
                            .serialize(row)
                            .getTimestamp());
        }
        SeaTunnelRowType bigintType =
                new SeaTunnelRowType(
                        new String[] {"time"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE});
        Assertions.assertEquals(
                EPOCH,
                new DefaultSeaTunnelRowDeserializer(bigintType, "tree", SHANGHAI)
                        .deserialize(new RowRecord(EPOCH))
                        .getField(0));
    }

    @Test
    void legacyConstructorsRetainUtcAndNullFieldsRemainNull() {
        SeaTunnelRow row =
                new SeaTunnelRow(new Object[] {LOCAL_TIME, "device", null, Long.MAX_VALUE});
        Assertions.assertEquals(
                UTC_EPOCH,
                new DefaultSeaTunnelRowSerializer(
                                ROW_TYPE,
                                "root.test",
                                "time",
                                "device",
                                Collections.singletonList("number"))
                        .serialize(row)
                        .getTimestamp());
        IoTDBv2RelationalRecord record =
                new RelationalSeaTunnelRowSerializer(
                                ROW_TYPE,
                                "test",
                                "device",
                                "time",
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Arrays.asList("event_time", "number"),
                                Arrays.asList(TSDataType.TIMESTAMP, TSDataType.INT64))
                        .serialize(row);
        Assertions.assertEquals(UTC_EPOCH, record.getTimestamp());
        Assertions.assertEquals(Arrays.asList(null, Long.MAX_VALUE), record.getFields());
        SeaTunnelRowType sourceType =
                new SeaTunnelRowType(
                        new String[] {"time"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});
        Assertions.assertEquals(
                LOCAL_TIME.minusHours(8),
                new DefaultSeaTunnelRowDeserializer(sourceType, "tree")
                        .deserialize(new RowRecord(EPOCH))
                        .getField(0));
    }

    @ParameterizedTest
    @ValueSource(strings = {"2024-11-03T01:30:00", "2024-03-10T02:30:00"})
    void sinksRejectAmbiguousAndNonexistentLocalTimestamps(String localTime) {
        LocalDateTime timestamp = LocalDateTime.parse(localTime);
        SeaTunnelRow keyRow = new SeaTunnelRow(new Object[] {timestamp, "device", LOCAL_TIME, 7L});
        assertInvalidTimestamp(
                Assertions.assertThrows(
                        IotdbConnectorException.class,
                        () -> treeSerializer(NEW_YORK).serialize(keyRow)),
                "time",
                timestamp);
        assertInvalidTimestamp(
                Assertions.assertThrows(
                        IotdbConnectorException.class,
                        () -> tableSerializer(NEW_YORK).serialize(keyRow)),
                "time",
                timestamp);

        SeaTunnelRow fieldRow =
                new SeaTunnelRow(new Object[] {LOCAL_TIME, "device", timestamp, 7L});
        assertInvalidTimestamp(
                Assertions.assertThrows(
                        IotdbConnectorException.class,
                        () -> tableSerializer(NEW_YORK).serialize(fieldRow)),
                "event_time",
                timestamp);
    }

    @ParameterizedTest
    @ValueSource(strings = {"2024-11-03T05:30:00Z", "2024-11-03T06:30:00Z"})
    void sourcesRejectOverlapBeforeDiscardingTheOffset(String instant) {
        long epoch = Instant.parse(instant).toEpochMilli();
        LocalDateTime ambiguousTime = LocalDateTime.of(2024, 11, 3, 1, 30);
        SeaTunnelRowType treeType =
                new SeaTunnelRowType(
                        new String[] {"time"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});
        IotdbConnectorException treeError =
                Assertions.assertThrows(
                        IotdbConnectorException.class,
                        () ->
                                new DefaultSeaTunnelRowDeserializer(treeType, "tree", NEW_YORK)
                                        .deserialize(new RowRecord(epoch)));
        assertInvalidTimestamp(treeError, "time", ambiguousTime);
        Assertions.assertTrue(treeError.getMessage().contains(Long.toString(epoch)));

        // Check table TIME keys and TIMESTAMP fields independently.
        for (String fieldName : Arrays.asList("time", "event_time")) {
            SeaTunnelRowType tableType =
                    new SeaTunnelRowType(
                            new String[] {fieldName},
                            new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});
            RowRecord tableRow = new RowRecord(0L);
            tableRow.addField(epoch, TSDataType.TIMESTAMP);
            IotdbConnectorException tableError =
                    Assertions.assertThrows(
                            IotdbConnectorException.class,
                            () ->
                                    new DefaultSeaTunnelRowDeserializer(
                                                    tableType, "table", NEW_YORK)
                                            .deserialize(tableRow));
            assertInvalidTimestamp(tableError, fieldName, ambiguousTime);
            Assertions.assertTrue(tableError.getMessage().contains(Long.toString(epoch)));
        }
        Assertions.assertEquals(
                LocalDateTime.ofInstant(Instant.parse(instant), ZoneOffset.UTC),
                new DefaultSeaTunnelRowDeserializer(treeType, "tree")
                        .deserialize(new RowRecord(epoch))
                        .getField(0));
    }

    @Test
    void rawEpochsRemainDistinctDuringOverlap() {
        for (long epoch :
                Arrays.asList(
                        Instant.parse("2024-11-03T05:30:00Z").toEpochMilli(),
                        Instant.parse("2024-11-03T06:30:00Z").toEpochMilli())) {
            SeaTunnelRowType sourceType =
                    new SeaTunnelRowType(
                            new String[] {"time"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE});
            Assertions.assertEquals(
                    epoch,
                    new DefaultSeaTunnelRowDeserializer(sourceType, "tree", NEW_YORK)
                            .deserialize(new RowRecord(epoch))
                            .getField(0));
            RowRecord tableRow = new RowRecord(0L);
            tableRow.addField(epoch, TSDataType.TIMESTAMP);
            Assertions.assertEquals(
                    epoch,
                    new DefaultSeaTunnelRowDeserializer(sourceType, "table", NEW_YORK)
                            .deserialize(tableRow)
                            .getField(0));
            for (SeaTunnelDataType<?> type :
                    Arrays.asList(BasicType.LONG_TYPE, BasicType.STRING_TYPE)) {
                SeaTunnelRowType sinkType =
                        new SeaTunnelRowType(
                                new String[] {"time", "device"},
                                new SeaTunnelDataType[] {type, BasicType.STRING_TYPE});
                Object value = type == BasicType.LONG_TYPE ? epoch : Long.toString(epoch);
                SeaTunnelRow row = new SeaTunnelRow(new Object[] {value, "device"});
                Assertions.assertEquals(
                        epoch,
                        new DefaultSeaTunnelRowSerializer(
                                        sinkType, "root.test", "time", "device", null, NEW_YORK)
                                .serialize(row)
                                .getTimestamp());
                Assertions.assertEquals(
                        epoch,
                        new RelationalSeaTunnelRowSerializer(
                                        sinkType,
                                        "test",
                                        "device",
                                        "time",
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        NEW_YORK)
                                .serialize(row)
                                .getTimestamp());
            }
        }
    }

    @ParameterizedTest
    @CsvSource({
        "2024-03-10T01:30:00,2024-03-10T06:30:00Z",
        "2024-03-10T03:30:00,2024-03-10T07:30:00Z",
        "2024-11-03T00:30:00,2024-11-03T04:30:00Z",
        "2024-11-03T02:30:00,2024-11-03T07:30:00Z"
    })
    void validLocalTimesAroundTransitionsRoundTrip(String localTime, String instant) {
        LocalDateTime timestamp = LocalDateTime.parse(localTime);
        long epoch = Instant.parse(instant).toEpochMilli();
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {timestamp, "device", timestamp, 7L});
        Assertions.assertEquals(epoch, treeSerializer(NEW_YORK).serialize(row).getTimestamp());
        IoTDBv2RelationalRecord record = tableSerializer(NEW_YORK).serialize(row);
        Assertions.assertEquals(epoch, record.getTimestamp());
        Assertions.assertEquals(Arrays.asList(epoch, 7L), record.getFields());
        SeaTunnelRowType sourceType =
                new SeaTunnelRowType(
                        new String[] {"time"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});
        Assertions.assertEquals(
                timestamp,
                new DefaultSeaTunnelRowDeserializer(sourceType, "tree", NEW_YORK)
                        .deserialize(new RowRecord(epoch))
                        .getField(0));
        RowRecord tableRow = new RowRecord(0L);
        tableRow.addField(epoch, TSDataType.TIMESTAMP);
        Assertions.assertEquals(
                timestamp,
                new DefaultSeaTunnelRowDeserializer(sourceType, "table", NEW_YORK)
                        .deserialize(tableRow)
                        .getField(0));
    }

    private void assertInvalidTimestamp(
            IotdbConnectorException exception, String fieldName, LocalDateTime timestamp) {
        Assertions.assertEquals(
                CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT, exception.getSeaTunnelErrorCode());
        String message = exception.getMessage();
        Assertions.assertTrue(message.contains("'" + fieldName + "'"), message);
        Assertions.assertTrue(message.contains(timestamp.toString()), message);
        Assertions.assertTrue(message.contains("zone_id 'America/New_York'"), message);
        Assertions.assertTrue(message.contains("UTC"), message);
        Assertions.assertTrue(message.contains("BIGINT"), message);
    }

    private DefaultSeaTunnelRowSerializer treeSerializer(ZoneId zone) {
        return new DefaultSeaTunnelRowSerializer(
                ROW_TYPE, "root.test", "time", "device", Collections.singletonList("number"), zone);
    }

    private RelationalSeaTunnelRowSerializer tableSerializer(ZoneId zone) {
        return new RelationalSeaTunnelRowSerializer(
                ROW_TYPE,
                "test",
                "device",
                "time",
                Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList("event_time", "number"),
                Arrays.asList(TSDataType.TIMESTAMP, TSDataType.INT64),
                zone);
    }
}
