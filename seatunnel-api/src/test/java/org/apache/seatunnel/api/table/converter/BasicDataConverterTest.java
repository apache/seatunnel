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

package org.apache.seatunnel.api.table.converter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class BasicDataConverterTest {

    private static final BasicDataConverter<Object> CONVERTER =
            new BasicDataConverter<Object>() {
                @Override
                public String identifier() {
                    return "test";
                }
            };

    @Test
    void testConvertEpochSecondsToLocalDateTime() {
        long epochSeconds = 1_700_000_000L;

        Assertions.assertEquals(
                toLocalDateTime(Instant.ofEpochSecond(epochSeconds)),
                CONVERTER.convertLocalDateTime(epochSeconds));
    }

    @Test
    void testConvertEpochMillisToLocalDateTime() {
        long epochMillis = 1_700_000_000_000L;

        Assertions.assertEquals(
                toLocalDateTime(Instant.ofEpochMilli(epochMillis)),
                CONVERTER.convertLocalDateTime(epochMillis));
    }

    @Test
    void testConvertEpochMicrosToLocalDateTime() {
        long epochMicros = 1_700_000_000_000_000L;

        Assertions.assertEquals(
                toLocalDateTime(Instant.ofEpochSecond(1_700_000_000L)),
                CONVERTER.convertLocalDateTime(epochMicros));
    }

    @Test
    void testConvertEpochNanosToLocalDateTime() {
        long epochNanos = 1_700_000_000_000_000_000L;

        Assertions.assertEquals(
                toLocalDateTime(Instant.ofEpochSecond(1_700_000_000L)),
                CONVERTER.convertLocalDateTime(epochNanos));
    }

    @Test
    void testConvertEpochSecondsToLocalDate() {
        long epochSeconds = 1_700_000_000L;

        Assertions.assertEquals(
                toLocalDate(Instant.ofEpochSecond(epochSeconds)),
                CONVERTER.convertLocalDate(epochSeconds));
    }

    @Test
    void testConvertEpochMillisToLocalDate() {
        long epochMillis = 1_700_000_000_000L;

        Assertions.assertEquals(
                toLocalDate(Instant.ofEpochMilli(epochMillis)),
                CONVERTER.convertLocalDate(epochMillis));
    }

    private static LocalDateTime toLocalDateTime(Instant instant) {
        return instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    private static LocalDate toLocalDate(Instant instant) {
        return instant.atZone(ZoneId.systemDefault()).toLocalDate();
    }
}
