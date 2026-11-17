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
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.TimePrecision;

import org.influxdb.dto.Point;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultSerializerTimestampPrecisionTest {

    private static final LocalDateTime TIMESTAMP =
            LocalDateTime.of(2025, 1, 2, 3, 4, 5, 123_000_000);

    @ParameterizedTest
    @EnumSource(TimePrecision.class)
    void processingTimeKeepsItsMillisecondUnit(TimePrecision precision) {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        AtomicLong clock = new AtomicLong(TIMESTAMP.toInstant(ZoneOffset.UTC).toEpochMilli());
        DefaultSerializer serializer =
                new DefaultSerializer(
                        rowType,
                        precision.getTimeUnit(),
                        Collections.emptyList(),
                        null,
                        "metrics",
                        clock::get);

        assertProcessingTime(serializer, new SeaTunnelRow(new Object[] {1}), clock, precision);
    }

    @ParameterizedTest
    @EnumSource(TimePrecision.class)
    void nullTimestampUsesProcessingTime(TimePrecision precision) {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"time", "value"},
                        new SeaTunnelDataType[] {
                            LocalTimeType.LOCAL_DATE_TIME_TYPE, BasicType.INT_TYPE
                        });
        AtomicLong clock = new AtomicLong(TIMESTAMP.toInstant(ZoneOffset.UTC).toEpochMilli());
        DefaultSerializer serializer =
                new DefaultSerializer(
                        rowType,
                        precision.getTimeUnit(),
                        Collections.emptyList(),
                        "time",
                        "metrics",
                        clock::get);

        assertProcessingTime(
                serializer, new SeaTunnelRow(new Object[] {null, 1}), clock, precision);
    }

    @ParameterizedTest
    @EnumSource(TimePrecision.class)
    void localDateTimeKeepsItsUtcInstant(TimePrecision precision) {
        DefaultSerializer serializer = serializer(LocalTimeType.LOCAL_DATE_TIME_TYPE, precision);
        Point point = serializer.serialize(new SeaTunnelRow(new Object[] {TIMESTAMP, 1}));

        long epochMillis = TIMESTAMP.toInstant(ZoneOffset.UTC).toEpochMilli();
        assertEquals(epochMillis, timestampIn(point, TimeUnit.MILLISECONDS));
        assertEquals(
                precision.getTimeUnit().convert(epochMillis, TimeUnit.MILLISECONDS),
                timestampIn(point, precision.getTimeUnit()));
    }

    @ParameterizedTest
    @EnumSource(TimePrecision.class)
    void numericTimestampUsesConfiguredPrecision(TimePrecision precision) {
        long timestamp =
                precision
                        .getTimeUnit()
                        .convert(
                                TIMESTAMP.toInstant(ZoneOffset.UTC).toEpochMilli(),
                                TimeUnit.MILLISECONDS);
        Point point =
                serializer(BasicType.LONG_TYPE, precision)
                        .serialize(new SeaTunnelRow(new Object[] {timestamp, 1}));

        assertEquals(timestamp, timestampIn(point, precision.getTimeUnit()));
    }

    @ParameterizedTest
    @EnumSource(TimePrecision.class)
    void numericStringTimestampUsesConfiguredPrecision(TimePrecision precision) {
        long timestamp =
                precision
                        .getTimeUnit()
                        .convert(
                                TIMESTAMP.toInstant(ZoneOffset.UTC).toEpochMilli(),
                                TimeUnit.MILLISECONDS);
        Point point =
                serializer(BasicType.STRING_TYPE, precision)
                        .serialize(new SeaTunnelRow(new Object[] {Long.toString(timestamp), 1}));

        assertEquals(timestamp, timestampIn(point, precision.getTimeUnit()));
    }

    private DefaultSerializer serializer(
            SeaTunnelDataType<?> timestampType, TimePrecision precision) {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"time", "value"},
                        new SeaTunnelDataType[] {timestampType, BasicType.INT_TYPE});
        return new DefaultSerializer(
                rowType, precision.getTimeUnit(), Collections.emptyList(), "time", "metrics");
    }

    /**
     * Checks exact unit conversion before and after a backwards step in the supplied wall clock.
     */
    private void assertProcessingTime(
            DefaultSerializer serializer,
            SeaTunnelRow row,
            AtomicLong clock,
            TimePrecision precision) {
        for (int i = 0; i < 2; i++) {
            Point point = serializer.serialize(row);
            assertEquals(clock.get(), timestampIn(point, TimeUnit.MILLISECONDS));
            assertEquals(
                    precision.getTimeUnit().convert(clock.get(), TimeUnit.MILLISECONDS),
                    timestampIn(point, precision.getTimeUnit()));
            clock.addAndGet(-TimeUnit.HOURS.toMillis(2));
        }
    }

    private long timestampIn(Point point, TimeUnit precision) {
        String line = point.lineProtocol(precision);
        return Long.parseLong(line.substring(line.lastIndexOf(' ') + 1));
    }
}
