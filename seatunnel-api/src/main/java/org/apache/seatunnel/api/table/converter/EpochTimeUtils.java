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

import java.time.Instant;
import java.util.concurrent.TimeUnit;

final class EpochTimeUtils {

    private static final long LEGACY_EPOCH_MILLIS_LOWER_BOUND = 999_999_999L;
    private static final long MICROSECONDS_PER_SECOND = TimeUnit.SECONDS.toMicros(1);
    private static final long NANOSECONDS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

    private EpochTimeUtils() {}

    /**
     * Converts a numeric epoch value using timestamp precision metadata when it is available. Scale
     * 0, 3, 6 and 9 are treated as seconds, milliseconds, microseconds and nanoseconds. Other
     * scales fall back to the legacy shared heuristic.
     */
    static Instant convertToInstant(Object typeDefine, Number value) {
        if (!(typeDefine instanceof BasicTypeDefine)) {
            return convertToInstant(value);
        }
        Integer scale = ((BasicTypeDefine<?>) typeDefine).getScale();
        if (scale == null) {
            return convertToInstant(value);
        }
        switch (scale) {
            case 0:
                return Instant.ofEpochSecond(value.longValue());
            case 3:
                return Instant.ofEpochMilli(value.longValue());
            case 6:
                return ofEpochMicro(value.longValue());
            case 9:
                return ofEpochNano(value.longValue());
            default:
                return convertToInstant(value);
        }
    }

    /**
     * Preserves the existing no-metadata conversion contract: values below 999999999 are epoch
     * seconds, while larger values are epoch milliseconds. This keeps early-epoch millisecond
     * values such as 1000000000 from being reinterpreted as seconds.
     */
    static Instant convertToInstant(Number value) {
        long epochValue = value.longValue();
        if (epochValue < LEGACY_EPOCH_MILLIS_LOWER_BOUND) {
            return Instant.ofEpochSecond(epochValue);
        }
        return Instant.ofEpochMilli(epochValue);
    }

    private static Instant ofEpochMicro(long epochMicro) {
        long seconds = Math.floorDiv(epochMicro, MICROSECONDS_PER_SECOND);
        long micros = Math.floorMod(epochMicro, MICROSECONDS_PER_SECOND);
        return Instant.ofEpochSecond(seconds, TimeUnit.MICROSECONDS.toNanos(micros));
    }

    private static Instant ofEpochNano(long epochNano) {
        long seconds = Math.floorDiv(epochNano, NANOSECONDS_PER_SECOND);
        long nanos = Math.floorMod(epochNano, NANOSECONDS_PER_SECOND);
        return Instant.ofEpochSecond(seconds, nanos);
    }
}
