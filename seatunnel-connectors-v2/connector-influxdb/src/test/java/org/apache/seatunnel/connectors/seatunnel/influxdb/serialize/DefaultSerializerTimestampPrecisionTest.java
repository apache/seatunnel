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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultSerializerTimestampPrecisionTest {

    private static final LocalDateTime TIMESTAMP =
            LocalDateTime.of(2025, 1, 2, 3, 4, 5, 123_000_000);

    @ParameterizedTest
    @EnumSource(TimePrecision.class)
    void processingTimeKeepsItsMillisecondUnit(TimePrecision precision) {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        DefaultSerializer serializer =
                new DefaultSerializer(
                        rowType, precision.getTimeUnit(), Collections.emptyList(), null, "metrics");

        long before = System.currentTimeMillis();
        Point point = serializer.serialize(new SeaTunnelRow(new Object[] {1}));
        long after = System.currentTimeMillis();

        long timestamp = timestampIn(point, TimeUnit.MILLISECONDS);
        assertTrue(timestamp >= before && timestamp <= after, "timestamp: " + timestamp);
        assertEquals(
                precision.getTimeUnit().convert(timestamp, TimeUnit.MILLISECONDS),
                timestampIn(point, precision.getTimeUnit()));
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

    private long timestampIn(Point point, TimeUnit precision) {
        String line = point.lineProtocol(precision);
        return Long.parseLong(line.substring(line.lastIndexOf(' ') + 1));
    }
}
