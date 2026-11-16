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

package org.apache.seatunnel.connectors.doris.serialize;

import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.doris.sink.writer.LoadConstants;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.TimeZone;

public class SeaTunnelRowSerializerTest {

    @Test
    void testTimestampTzCsvUsesExplicitDatetimeTimezone() throws Exception {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"ts_tz"},
                        new SeaTunnelDataType<?>[] {LocalTimeType.OFFSET_DATE_TIME_TYPE});

        TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            SeaTunnelRowSerializer serializer =
                    new SeaTunnelRowSerializer(
                            LoadConstants.CSV,
                            rowType,
                            ",",
                            false,
                            true,
                            ZoneId.of("Asia/Shanghai"));

            // 2024-01-01T10:00:00Z == 2024-01-01 18:00:00 in Asia/Shanghai, regardless of JVM
            // default being UTC.
            SeaTunnelRow utcRow =
                    new SeaTunnelRow(
                            new Object[] {
                                OffsetDateTime.of(2024, 1, 1, 10, 0, 0, 0, ZoneOffset.UTC)
                            });
            Assertions.assertEquals(
                    "2024-01-01 18:00:00", new String(serializer.serialize(utcRow)));
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @Test
    void testTimestampTzJsonUsesExplicitDatetimeTimezone() throws Exception {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"ts_tz"},
                        new SeaTunnelDataType<?>[] {LocalTimeType.OFFSET_DATE_TIME_TYPE});

        TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            SeaTunnelRowSerializer serializer =
                    new SeaTunnelRowSerializer(
                            LoadConstants.JSON,
                            rowType,
                            ",",
                            false,
                            true,
                            ZoneId.of("Asia/Shanghai"));

            // 2024-01-01T10:00:00Z == 2024-01-01T18:00:00 in Asia/Shanghai, regardless of JVM
            // default being UTC.
            SeaTunnelRow utcRow =
                    new SeaTunnelRow(
                            new Object[] {
                                OffsetDateTime.of(2024, 1, 1, 10, 0, 0, 0, ZoneOffset.UTC)
                            });
            String json = new String(serializer.serialize(utcRow));
            Assertions.assertTrue(json.contains("2024-01-01T18:00:00"), json);
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @Test
    void testTimestampTzCsvFallsBackToJvmDefaultWhenDatetimeTimezoneUnset() throws Exception {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"ts_tz"},
                        new SeaTunnelDataType<?>[] {LocalTimeType.OFFSET_DATE_TIME_TYPE});

        TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
            // No explicit datetimeTimezone: legacy behavior — JVM default is used.
            SeaTunnelRowSerializer serializer =
                    new SeaTunnelRowSerializer(LoadConstants.CSV, rowType, ",", false, true, null);

            SeaTunnelRow utcRow =
                    new SeaTunnelRow(
                            new Object[] {
                                OffsetDateTime.of(2024, 1, 1, 10, 0, 0, 0, ZoneOffset.UTC)
                            });
            Assertions.assertEquals(
                    "2024-01-01 18:00:00", new String(serializer.serialize(utcRow)));
        } finally {
            TimeZone.setDefault(original);
        }
    }
}
