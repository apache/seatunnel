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

package org.apache.seatunnel.connectors.seatunnel.influxdb.serialize;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.influxdb.dto.Point;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class DefaultSerializerTest {

    private static final String MEASUREMENT = "cpu";

    /** 2024-01-01T00:00:00 in UTC, as epoch milliseconds. */
    private static final long EPOCH_MILLIS_2024_01_01 = 1704067200000L;

    private static final long EPOCH_MILLIS_2020_01_01 = 1577836800000L;

    private DefaultSerializer serializer(SeaTunnelDataType<?> timeType) {
        return serializer(timeType, TimeUnit.NANOSECONDS);
    }

    private DefaultSerializer serializer(SeaTunnelDataType<?> timeType, TimeUnit precision) {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"time", "value"},
                        new SeaTunnelDataType[] {timeType, BasicType.DOUBLE_TYPE});
        return new DefaultSerializer(
                rowType, precision, Collections.emptyList(), "time", MEASUREMENT);
    }

    /**
     * Reads the point timestamp from the line protocol. {@link Point#lineProtocol()} always formats
     * the timestamp as nanoseconds, so the last token is the timestamp in nanoseconds.
     */
    private static long emittedTimestampNanos(Point point) {
        String lineProtocol = point.lineProtocol();
        Assertions.assertNotNull(lineProtocol);
        String[] tokens = lineProtocol.split("\\s+");
        Assertions.assertTrue(
                tokens.length >= 2, "line protocol does not contain a timestamp: " + lineProtocol);
        return Long.parseLong(tokens[tokens.length - 1]);
    }

    @Test
    public void shouldFallBackToCurrentTimeWhenStringTimeColumnIsNull() {
        DefaultSerializer serializer = serializer(BasicType.STRING_TYPE);
        Point point = serializer.serialize(new SeaTunnelRow(new Object[] {null, 1.0d}));
        Assertions.assertNotNull(point);
        Assertions.assertNotNull(point.lineProtocol());
    }

    @Test
    public void shouldFallBackToCurrentTimeWhenBigintTimeColumnIsNull() {
        DefaultSerializer serializer = serializer(BasicType.LONG_TYPE);
        Point point = serializer.serialize(new SeaTunnelRow(new Object[] {null, 1.0d}));
        Assertions.assertNotNull(point);
        Assertions.assertNotNull(point.lineProtocol());
    }

    @Test
    public void shouldFallBackToCurrentTimeWhenTimestampTimeColumnIsNull() {
        DefaultSerializer serializer = serializer(LocalTimeType.LOCAL_DATE_TIME_TYPE);
        Point point = serializer.serialize(new SeaTunnelRow(new Object[] {null, 1.0d}));
        Assertions.assertNotNull(point);
        Assertions.assertNotNull(point.lineProtocol());
    }

    @Test
    public void shouldUseConfiguredTimeColumnWhenPresent() {
        DefaultSerializer serializer = serializer(BasicType.STRING_TYPE);
        Point point = serializer.serialize(new SeaTunnelRow(new Object[] {"123456", 1.0d}));
        Assertions.assertNotNull(point);
        Assertions.assertTrue(point.lineProtocol().contains("123456"));
    }

    @Test
    public void shouldSerializeTags() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"host", "time", "value"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE, BasicType.LONG_TYPE, BasicType.DOUBLE_TYPE
                        });
        DefaultSerializer serializer =
                new DefaultSerializer(
                        rowType,
                        TimeUnit.NANOSECONDS,
                        Collections.singletonList("host"),
                        "time",
                        MEASUREMENT);
        Point point =
                serializer.serialize(new SeaTunnelRow(new Object[] {"server-1", 123456L, 1.0d}));
        Assertions.assertNotNull(point);
        Assertions.assertTrue(point.lineProtocol().contains("server-1"));
        Assertions.assertTrue(point.lineProtocol().contains("123456"));
    }

    @Test
    public void shouldUseProcessingTimeWhenTimeKeyNotConfigured() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        // default epoch (nanoseconds) of the influxdb sink
        DefaultSerializer serializer =
                new DefaultSerializer(
                        rowType, TimeUnit.NANOSECONDS, Collections.emptyList(), null, MEASUREMENT);

        long before = System.currentTimeMillis();
        Point point = serializer.serialize(new SeaTunnelRow(new Object[] {1.0d}));
        long after = System.currentTimeMillis();

        Assertions.assertNotNull(point);
        long timestampMillis = TimeUnit.NANOSECONDS.toMillis(emittedTimestampNanos(point));
        Assertions.assertTrue(
                timestampMillis >= before && timestampMillis <= after,
                "processing time timestamp should be the current time in milliseconds but was "
                        + timestampMillis
                        + " ("
                        + Instant.ofEpochMilli(timestampMillis)
                        + "), expected it to be within ["
                        + before
                        + ", "
                        + after
                        + "]");
        // the millisecond value must not be written as if it were nanoseconds (1970-01-01)
        Assertions.assertTrue(
                timestampMillis > EPOCH_MILLIS_2020_01_01,
                "processing time timestamp must not be a 1970 timestamp but was "
                        + timestampMillis);
    }

    @Test
    public void shouldSerializeLocalDateTimeTimeColumn() {
        DefaultSerializer serializer = serializer(LocalTimeType.LOCAL_DATE_TIME_TYPE);
        LocalDateTime time = LocalDateTime.of(2024, 1, 1, 0, 0, 0);
        Point point = serializer.serialize(new SeaTunnelRow(new Object[] {time, 1.0d}));
        Assertions.assertNotNull(point);

        // the TIMESTAMP column is converted with UTC to epoch milliseconds; the line protocol
        // writes nanoseconds, so the emitted timestamp is the epoch millisecond value * 1_000_000
        Assertions.assertEquals(
                TimeUnit.MILLISECONDS.toNanos(EPOCH_MILLIS_2024_01_01),
                emittedTimestampNanos(point),
                "TIMESTAMP key_time must be written as epoch milliseconds");
    }

    @Test
    public void shouldSerializeLocalDateTimeTimeColumnWithMillisecondEpoch() {
        // a TIMESTAMP key_time column must be correct for any epoch value, not only for MS
        DefaultSerializer serializer =
                serializer(LocalTimeType.LOCAL_DATE_TIME_TYPE, TimeUnit.MILLISECONDS);
        LocalDateTime time = LocalDateTime.of(2024, 1, 1, 0, 0, 0);
        Point point = serializer.serialize(new SeaTunnelRow(new Object[] {time, 1.0d}));
        Assertions.assertNotNull(point);
        Assertions.assertEquals(
                TimeUnit.MILLISECONDS.toNanos(EPOCH_MILLIS_2024_01_01),
                emittedTimestampNanos(point),
                "TIMESTAMP key_time must be written as epoch milliseconds");
    }
}
