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

import org.apache.spark.sql.types.DecimalType;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class OffsetDateTimeUtils {
    public static final String LOGICAL_TIMESTAMP_WITH_OFFSET_TYPE_FLAG =
            "logical_timestamp_with_offset_type";

    // Shift applied to totalSeconds so the fractional part is always positive.
    // Offset range: [-43200, +50400] seconds → shifted range: [56800, 150400] (6 digits, always >
    // 0)
    static final int OFFSET_SHIFT = 100_000;

    // scale=6 to hold the 6-digit shifted offset; precision=20 for 13-digit epochMilli + 6 decimal
    public static final DecimalType OFFSET_DATETIME_WITH_DECIMAL = new DecimalType(20, 6);

    public static BigDecimal toBigDecimal(OffsetDateTime time) {
        long epochMilli = time.toInstant().toEpochMilli();
        int shiftedOffset = time.getOffset().getTotalSeconds() + OFFSET_SHIFT;
        // epochMilli may be negative; shiftedOffset is always a positive 6-digit integer.
        // String.format guarantees no sign in the fractional part.
        return new BigDecimal(epochMilli + "." + String.format("%06d", shiftedOffset));
    }

    public static OffsetDateTime toOffsetDateTime(BigDecimal timeWithDecimal) {
        BigDecimal normalized = timeWithDecimal.setScale(6);
        long epochMilli = normalized.longValue(); // truncates toward zero — correct for ±epoch
        BigDecimal fractional = normalized.subtract(BigDecimal.valueOf(epochMilli)).abs();
        int shiftedOffset = fractional.movePointRight(6).intValue();
        return Instant.ofEpochMilli(epochMilli)
                .atOffset(ZoneOffset.ofTotalSeconds(shiftedOffset - OFFSET_SHIFT));
    }
}
