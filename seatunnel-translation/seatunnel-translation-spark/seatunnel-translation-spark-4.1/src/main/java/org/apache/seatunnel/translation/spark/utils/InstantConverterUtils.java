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

import java.time.Instant;

public class InstantConverterUtils {

    private static final long MICRO_OF_SECOND = 1000_000;
    private static final int MICRO_OF_NANOS = 1000;

    /** @see Instant#toEpochMilli() */
    public static Long toEpochMicro(Instant instant) {
        long seconds = instant.getEpochSecond();
        int nanos = instant.getNano();
        if (seconds < 0 && nanos > 0) {
            long micro = Math.multiplyExact(seconds + 1, MICRO_OF_SECOND);
            long adjustment = nanos / MICRO_OF_NANOS - MICRO_OF_SECOND;
            return Math.addExact(micro, adjustment);
        } else {
            long millis = Math.multiplyExact(seconds, MICRO_OF_SECOND);
            return Math.addExact(millis, nanos / MICRO_OF_NANOS);
        }
    }

    /** @see Instant#ofEpochMilli(long) */
    public static Instant ofEpochMicro(long epochMicro) {
        long secs = Math.floorDiv(epochMicro, MICRO_OF_SECOND);
        int mos = (int) Math.floorMod(epochMicro, MICRO_OF_SECOND);
        return Instant.ofEpochSecond(secs, Math.multiplyExact(mos, MICRO_OF_NANOS));
    }
}
