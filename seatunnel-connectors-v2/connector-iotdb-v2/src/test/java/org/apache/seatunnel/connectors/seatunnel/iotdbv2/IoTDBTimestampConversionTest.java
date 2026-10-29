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
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.serialize.DefaultSeaTunnelRowDeserializer;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.serialize.DefaultSeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.serialize.relational.IoTDBv2RelationalRecord;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.serialize.relational.RelationalSeaTunnelRowSerializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import shaded.org.apache.tsfile.enums.TSDataType;
import shaded.org.apache.tsfile.read.common.Field;
import shaded.org.apache.tsfile.read.common.RowRecord;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;

class IoTDBTimestampConversionTest {

    private static final ZoneId SHANGHAI = ZoneId.of("Asia/Shanghai");
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
