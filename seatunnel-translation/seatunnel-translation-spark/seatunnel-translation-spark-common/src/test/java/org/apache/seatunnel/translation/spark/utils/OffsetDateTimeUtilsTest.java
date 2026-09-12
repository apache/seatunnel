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

package org.apache.seatunnel.translation.spark.utils;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OffsetDateTimeUtilsTest {

    private static void assertRoundTrip(OffsetDateTime original) {
        BigDecimal encoded = OffsetDateTimeUtils.toBigDecimal(original);
        OffsetDateTime decoded = OffsetDateTimeUtils.toOffsetDateTime(encoded);
        assertEquals(original.toInstant(), decoded.toInstant(), "Epoch mismatch for " + original);
        assertEquals(original.getOffset(), decoded.getOffset(), "Offset mismatch for " + original);
    }

    @Test
    void positiveOffset() {
        assertRoundTrip(OffsetDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneOffset.ofHours(9)));
    }

    @Test
    void negativeOffset() {
        // This was broken before the fix: -08:00 produced "epochMilli.-28800" (invalid BigDecimal)
        assertRoundTrip(OffsetDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneOffset.ofHours(-8)));
    }

    @Test
    void utcOffset() {
        assertRoundTrip(OffsetDateTime.of(2024, 6, 15, 0, 0, 0, 0, ZoneOffset.UTC));
    }

    @Test
    void maxPositiveOffset() {
        assertRoundTrip(OffsetDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneOffset.ofHours(14)));
    }

    @Test
    void maxNegativeOffset() {
        assertRoundTrip(OffsetDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneOffset.ofHours(-12)));
    }

    @Test
    void fractionalOffset() {
        // India Standard Time: +05:30
        assertRoundTrip(
                OffsetDateTime.of(2024, 3, 10, 8, 30, 0, 0, ZoneOffset.ofHoursMinutes(5, 30)));
    }
}
