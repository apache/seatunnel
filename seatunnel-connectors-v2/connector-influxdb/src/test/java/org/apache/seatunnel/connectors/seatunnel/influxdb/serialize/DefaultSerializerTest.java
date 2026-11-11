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

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class DefaultSerializerTest {

    private static final String MEASUREMENT = "cpu";

    private DefaultSerializer serializer(SeaTunnelDataType<?> timeType) {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"time", "value"},
                        new SeaTunnelDataType[] {timeType, BasicType.DOUBLE_TYPE});
        return new DefaultSerializer(
                rowType, TimeUnit.NANOSECONDS, Collections.emptyList(), "time", MEASUREMENT);
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
        DefaultSerializer serializer =
                new DefaultSerializer(
                        rowType, TimeUnit.NANOSECONDS, Collections.emptyList(), null, MEASUREMENT);
        Point point = serializer.serialize(new SeaTunnelRow(new Object[] {1.0d}));
        Assertions.assertNotNull(point);
        Assertions.assertNotNull(point.lineProtocol());
    }

    @Test
    public void shouldSerializeLocalDateTimeTimeColumn() {
        DefaultSerializer serializer = serializer(LocalTimeType.LOCAL_DATE_TIME_TYPE);
        LocalDateTime time = LocalDateTime.of(2024, 1, 1, 0, 0, 0);
        Point point = serializer.serialize(new SeaTunnelRow(new Object[] {time, 1.0d}));
        Assertions.assertNotNull(point);
        Assertions.assertNotNull(point.lineProtocol());
    }
}
