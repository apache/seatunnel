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

    // epochMilli length 13, timezone offset length 5
    public static final DecimalType OFFSET_DATETIME_WITH_DECIMAL = new DecimalType(18, 5);

    public static BigDecimal toBigDecimal(OffsetDateTime time) {
        long epochMilli = time.toInstant().toEpochMilli();
        int offsetSeconds = time.getOffset().getTotalSeconds();

        int sign = offsetSeconds < 0 ? 1 : 0;
        int absOffset = Math.abs(offsetSeconds);
        int decimalPart = sign * 100000 + absOffset;

        BigDecimal millis = new BigDecimal(epochMilli);
        BigDecimal decimal =
                new BigDecimal(decimalPart)
                        .divide(new BigDecimal(1000000), 6, java.math.RoundingMode.HALF_UP);
        return millis.add(decimal);
    }

    public static OffsetDateTime toOffsetDateTime(BigDecimal timeWithDecimal) {
        long epochMilli = timeWithDecimal.longValue();

        BigDecimal fractionalPart = timeWithDecimal.subtract(new BigDecimal(epochMilli));
        int decimalPart =
                fractionalPart
                        .multiply(new BigDecimal(1000000))
                        .setScale(0, java.math.RoundingMode.HALF_UP)
                        .intValue();

        int sign = decimalPart / 100000; // 0 or 1
        int absOffset = decimalPart % 100000;
        int offsetSeconds = sign == 1 ? -absOffset : absOffset;

        return Instant.ofEpochMilli(epochMilli).atOffset(ZoneOffset.ofTotalSeconds(offsetSeconds));
    }
}
