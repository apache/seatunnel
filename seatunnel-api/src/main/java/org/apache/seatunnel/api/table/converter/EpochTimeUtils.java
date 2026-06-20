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

    private static final long EPOCH_MILLIS_LOWER_BOUND = 10_000_000_000L;
    private static final long EPOCH_MICROS_LOWER_BOUND = 10_000_000_000_000L;
    private static final long EPOCH_NANOS_LOWER_BOUND = 10_000_000_000_000_000L;
    private static final long MICROSECONDS_PER_SECOND = TimeUnit.SECONDS.toMicros(1);
    private static final long NANOSECONDS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

    private EpochTimeUtils() {}

    static Instant convertToInstant(Number value) {
        long epochValue = value.longValue();
        long absoluteEpochValue =
                epochValue == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(epochValue);
        // Choose by decimal precision so modern epoch seconds are not treated as milliseconds.
        if (absoluteEpochValue < EPOCH_MILLIS_LOWER_BOUND) {
            return Instant.ofEpochSecond(epochValue);
        }
        if (absoluteEpochValue < EPOCH_MICROS_LOWER_BOUND) {
            return Instant.ofEpochMilli(epochValue);
        }
        if (absoluteEpochValue < EPOCH_NANOS_LOWER_BOUND) {
            long seconds = Math.floorDiv(epochValue, MICROSECONDS_PER_SECOND);
            long micros = Math.floorMod(epochValue, MICROSECONDS_PER_SECOND);
            return Instant.ofEpochSecond(seconds, TimeUnit.MICROSECONDS.toNanos(micros));
        }
        long seconds = Math.floorDiv(epochValue, NANOSECONDS_PER_SECOND);
        long nanos = Math.floorMod(epochValue, NANOSECONDS_PER_SECOND);
        return Instant.ofEpochSecond(seconds, nanos);
    }
}
