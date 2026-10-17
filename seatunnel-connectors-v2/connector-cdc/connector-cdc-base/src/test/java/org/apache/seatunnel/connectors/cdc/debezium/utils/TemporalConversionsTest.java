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

package org.apache.seatunnel.connectors.cdc.debezium.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;

public class TemporalConversionsTest {

    @Test
    public void parsesUtcZonedTimestampOnJava8() {
        LocalDateTime value =
                TemporalConversions.toLocalDateTime("2022-10-30T01:34:56.007890Z", ZoneOffset.UTC);

        Assertions.assertEquals(LocalDateTime.parse("2022-10-30T01:34:56.007890"), value);
        Assertions.assertEquals(
                Instant.parse("2022-10-30T01:34:56.123456789Z"),
                TemporalConversions.parseDebeziumTimestamp("2022-10-30T01:34:56.123456789Z"));
    }

    @Test
    public void parsesNumericOffsetIntoTheServerZone() {
        Assertions.assertEquals(
                LocalDateTime.parse("2022-10-30T06:34:56.007890"),
                TemporalConversions.toLocalDateTime(
                        "2022-10-30T01:34:56.007890-05:00", ZoneOffset.UTC));
        Assertions.assertEquals(
                LocalDateTime.parse("2022-10-30T01:34:56.007890"),
                TemporalConversions.toLocalDateTime(
                        "2022-10-30T01:34:56.007890-05:00", ZoneOffset.ofHours(-5)));
        Assertions.assertEquals(
                LocalDateTime.parse("2022-10-29T18:34:56"),
                TemporalConversions.toLocalDateTime("2022-10-30T02:34:56+08:00", ZoneOffset.UTC));
    }

    @Test
    public void rejectsTimestampWithoutOffset() {
        DateTimeParseException error =
                Assertions.assertThrows(
                        DateTimeParseException.class,
                        () ->
                                TemporalConversions.toLocalDateTime(
                                        "2022-10-30T01:34:56", ZoneOffset.UTC));

        Assertions.assertTrue(error.getMessage().contains("2022-10-30T01:34:56"));
    }
}
